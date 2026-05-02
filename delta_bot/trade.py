from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from .data import Candle, load_ohlcv_csv, make_ohlcv_path
from .exchange import create_exchange
from .strategy import generate_signal
from .utils import ensure_dir, timeframe_seconds


def _setup_logger(log_dir: str) -> logging.Logger:
    ensure_dir(log_dir)
    logger = logging.getLogger("delta_bot")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        log_path = Path(log_dir) / "bot.log"
        file_handler = logging.FileHandler(log_path)
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
    return logger


def _state_path() -> Path:
    return ensure_dir("state") / "runtime_state.json"


def _load_state() -> Dict[str, Any]:
    path = _state_path()
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _save_state(state: Dict[str, Any]) -> None:
    with open(_state_path(), "w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2, sort_keys=True)


def _fetch_equity(exchange: Any, config: Dict[str, Any]) -> float:
    try:
        balance = exchange.fetch_balance()
        total = balance.get("total") or {}
        for key in ("USDT", "USD"):
            if key in total:
                return float(total[key])
    except Exception:
        pass
    return float(config["risk"]["capital_usd"])


def _get_position(exchange: Any, symbol: str, state: Dict[str, Any], paper: bool) -> float:
    if paper:
        return float(state.get("paper_position", 0.0))
    try:
        if exchange.has.get("fetchPositions"):
            positions = exchange.fetch_positions([symbol])
            for pos in positions:
                if pos.get("symbol") == symbol:
                    return float(pos.get("contracts") or pos.get("positionAmt") or 0.0)
    except Exception:
        pass
    return float(state.get("last_position", 0.0))


def _record_trade(log_dir: str, payload: Dict[str, Any]) -> None:
    ensure_dir(log_dir)
    path = Path(log_dir) / "trades.jsonl"
    with open(path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload) + "\n")


def _update_daily_state(state: Dict[str, Any], equity: float) -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if state.get("day") != today:
        state["day"] = today
        state["day_start_equity"] = equity


def _apply_slippage(price: float, side: str, bps: float) -> float:
    delta = price * (bps / 10000)
    return price + delta if side == "buy" else price - delta


def _paper_execute(state: Dict[str, Any], side: str, qty: float, price: float, config: Dict[str, Any]) -> None:
    fee = abs(price * qty) * (config["risk"]["fee_bps"] / 10000)
    cash = float(state.get("paper_cash", config["risk"]["capital_usd"]))
    position = float(state.get("paper_position", 0.0))

    if side == "buy":
        cash -= price * qty + fee
        position += qty
    else:
        cash += price * qty - fee
        position -= qty

    state["paper_cash"] = cash
    state["paper_position"] = position


def _position_size(config: Dict[str, Any], price: float, stop_distance: float, equity: float) -> float:
    if stop_distance <= 0:
        return 0.0
    risk_amount = equity * config["risk"]["risk_per_trade"]
    qty_by_risk = risk_amount / stop_distance
    max_notional = equity * config["risk"]["max_leverage"]
    max_qty = max_notional / price
    max_qty_by_pct = (equity * config["risk"]["max_position_pct"]) / price
    return max(0.0, min(qty_by_risk, max_qty, max_qty_by_pct))


def run_trading(config: Dict[str, Any], *, paper: bool, once: bool) -> None:
    logger = _setup_logger(config["logging"]["log_dir"])
    exchange = create_exchange(config, for_trading=not paper)

    symbol = config["market"]["symbol"]
    timeframe = config["market"]["timeframe"]
    polling = config["execution"]["polling_seconds"]
    data_path = make_ohlcv_path(config["data"]["data_dir"], symbol, timeframe)

    state = _load_state()
    if data_path.exists():
        candles = load_ohlcv_csv(data_path)
    else:
        candles = []

    state.setdefault("last_timestamp", candles[-1].timestamp if candles else 0)

    while True:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe, limit=config["strategy"]["slow_window"] + 2)
        if not ohlcv:
            logger.warning("No OHLCV returned")
            if once:
                break
            time.sleep(polling)
            continue

        latest_timestamp = int(ohlcv[-1][0])
        if latest_timestamp <= state["last_timestamp"]:
            if once:
                break
            time.sleep(polling)
            continue

        candles = [
            Candle(
                timestamp=int(row[0]),
                datetime=datetime.fromtimestamp(row[0] / 1000, tz=timezone.utc).isoformat(),
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=float(row[5]),
            )
            for row in ohlcv
        ]

        signal = generate_signal(candles, config)
        if signal is None:
            logger.info("Not enough candles for signal")
            if once:
                break
            time.sleep(polling)
            continue

        price = candles[-1].close
        equity = _fetch_equity(exchange, config) if not paper else float(state.get("paper_cash", config["risk"]["capital_usd"]))
        _update_daily_state(state, equity)

        day_start = float(state.get("day_start_equity", equity))
        if day_start > 0:
            daily_loss = (day_start - equity) / day_start
        else:
            daily_loss = 0.0

        overall_drawdown = (float(state.get("peak_equity", equity)) - equity) / float(state.get("peak_equity", equity))
        state["peak_equity"] = max(float(state.get("peak_equity", equity)), equity)

        if daily_loss >= config["risk"]["max_daily_loss_pct"] or overall_drawdown >= config["risk"]["max_drawdown_pct"]:
            logger.error("Kill switch triggered. Exiting trading loop.")
            _save_state(state)
            break

        current_position = _get_position(exchange, symbol, state, paper)
        current_side = "flat"
        if current_position > 0:
            current_side = "long"
        elif current_position < 0:
            current_side = "short"

        if signal.side != current_side:
            if current_position != 0:
                side = "sell" if current_position > 0 else "buy"
                trade_price = _apply_slippage(price, side, config["risk"]["slippage_bps"])
                if paper:
                    _paper_execute(state, side, abs(current_position), trade_price, config)
                else:
                    exchange.create_order(symbol, "market", side, abs(current_position), None, {"reduceOnly": True})
                _record_trade(
                    config["logging"]["log_dir"],
                    {
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                        "action": "close",
                        "side": side,
                        "qty": abs(current_position),
                        "price": trade_price,
                    },
                )

            if signal.side in {"long", "short"}:
                qty = _position_size(config, price, signal.stop_distance, equity)
                if qty > 0:
                    side = "buy" if signal.side == "long" else "sell"
                    trade_price = _apply_slippage(price, side, config["risk"]["slippage_bps"])
                    if paper:
                        _paper_execute(state, side, qty, trade_price, config)
                    else:
                        exchange.create_order(symbol, "market", side, qty, None)
                    _record_trade(
                        config["logging"]["log_dir"],
                        {
                            "timestamp": datetime.now(timezone.utc).isoformat(),
                            "action": "open",
                            "side": side,
                            "qty": qty,
                            "price": trade_price,
                        },
                    )

        state["last_timestamp"] = latest_timestamp
        state["last_position"] = current_position
        _save_state(state)

        logger.info("Signal=%s Price=%.2f Equity=%.2f", signal.side, price, equity)

        if once:
            break
        time.sleep(max(1, polling))
