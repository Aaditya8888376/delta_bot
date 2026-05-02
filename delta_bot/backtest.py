from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from .data import Candle, FundingRate
from .strategy import Signal, generate_signal
from .utils import ensure_dir, utc_now


@dataclass
class Trade:
    entry_time: str
    exit_time: str
    side: str
    qty: float
    entry_price: float
    exit_price: float
    pnl: float
    fees: float
    funding: float


def _apply_slippage(price: float, side: str, bps: float) -> float:
    delta = price * (bps / 10000)
    return price + delta if side == "buy" else price - delta


def _position_size(config: Dict[str, Dict[str, float]], price: float, stop_distance: float, equity: float) -> float:
    risk = config["risk"]
    if stop_distance <= 0:
        return 0.0
    risk_amount = equity * risk["risk_per_trade"]
    qty_by_risk = risk_amount / stop_distance
    max_notional = equity * risk["max_leverage"]
    max_qty = max_notional / price
    max_qty_by_pct = (equity * risk["max_position_pct"]) / price
    return max(0.0, min(qty_by_risk, max_qty, max_qty_by_pct))


def _funding_rate_at(timestamp: int, funding_rates: List[FundingRate]) -> float:
    if not funding_rates:
        return 0.0
    latest: Optional[FundingRate] = None
    for rate in funding_rates:
        if rate.timestamp <= timestamp:
            latest = rate
        else:
            break
    return latest.rate if latest else 0.0


def run_backtest(
    config: Dict[str, Dict[str, float]],
    candles: List[Candle],
    funding_rates: List[FundingRate],
) -> Dict[str, object]:
    if len(candles) < config["strategy"]["slow_window"] + 1:
        raise ValueError("Not enough candles for backtest")

    equity = config["risk"]["capital_usd"]
    balance = equity
    position = 0.0
    entry_price = 0.0
    entry_time = ""
    trade_funding = 0.0
    trades: List[Trade] = []
    equity_curve: List[Dict[str, float]] = []
    fees_paid = 0.0
    funding_paid = 0.0
    peak_equity = equity
    max_drawdown = 0.0

    for index in range(1, len(candles)):
        window = candles[: index + 1]
        candle = candles[index]
        signal: Optional[Signal] = generate_signal(window, config)
        if signal is None:
            continue

        funding_rate = _funding_rate_at(candle.timestamp, funding_rates)
        if position != 0:
            notional = abs(position) * candle.close
            funding_payment = notional * funding_rate
            if position > 0:
                funding_payment *= -1
            balance += funding_payment
            funding_paid += funding_payment
            trade_funding += funding_payment

        desired = signal.side
        current_side = "flat"
        if position > 0:
            current_side = "long"
        elif position < 0:
            current_side = "short"

        if desired != current_side:
            if position != 0:
                exit_side = "sell" if position > 0 else "buy"
                exit_price = _apply_slippage(candle.open, exit_side, config["risk"]["slippage_bps"])
                pnl = (exit_price - entry_price) * position
                fee = abs(exit_price * position) * (config["risk"]["fee_bps"] / 10000)
                balance += pnl - fee
                fees_paid += fee
                trades.append(
                    Trade(
                        entry_time=entry_time or candle.datetime,
                        exit_time=candle.datetime,
                        side=current_side,
                        qty=abs(position),
                        entry_price=entry_price,
                        exit_price=exit_price,
                        pnl=pnl - fee,
                        fees=fee,
                        funding=trade_funding,
                    )
                )
                position = 0.0
                entry_price = 0.0
                entry_time = ""
                trade_funding = 0.0

            if desired in {"long", "short"}:
                qty = _position_size(config, candle.open, signal.stop_distance, balance)
                if qty > 0:
                    entry_side = "buy" if desired == "long" else "sell"
                    entry_price = _apply_slippage(candle.open, entry_side, config["risk"]["slippage_bps"])
                    fee = abs(entry_price * qty) * (config["risk"]["fee_bps"] / 10000)
                    balance -= fee
                    fees_paid += fee
                    position = qty if desired == "long" else -qty
                    entry_time = candle.datetime

        equity = balance + position * candle.close
        peak_equity = max(peak_equity, equity)
        drawdown = (peak_equity - equity) / peak_equity if peak_equity else 0.0
        max_drawdown = max(max_drawdown, drawdown)
        equity_curve.append({"timestamp": candle.timestamp, "equity": equity})

    wins = [trade for trade in trades if trade.pnl > 0]
    total_return = (equity - config["risk"]["capital_usd"]) / config["risk"]["capital_usd"]
    metrics = {
        "total_return": total_return,
        "ending_equity": equity,
        "max_drawdown": max_drawdown,
        "trades": len(trades),
        "win_rate": len(wins) / len(trades) if trades else 0.0,
        "fees_paid": fees_paid,
        "funding_paid": funding_paid,
    }

    return {
        "metrics": metrics,
        "trades": trades,
        "equity_curve": equity_curve,
    }


def write_backtest_results(results: Dict[str, object], output_dir: Path) -> None:
    ensure_dir(output_dir)
    metrics_path = output_dir / "metrics.csv"
    trades_path = output_dir / "trades.csv"
    equity_path = output_dir / "equity.csv"

    with open(metrics_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["metric", "value"])
        for key, value in results["metrics"].items():
            writer.writerow([key, value])

    with open(trades_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([
            "entry_time",
            "exit_time",
            "side",
            "qty",
            "entry_price",
            "exit_price",
            "pnl",
            "fees",
            "funding",
        ])
        for trade in results["trades"]:
            writer.writerow([
                trade.entry_time,
                trade.exit_time,
                trade.side,
                trade.qty,
                trade.entry_price,
                trade.exit_price,
                trade.pnl,
                trade.fees,
                trade.funding,
            ])

    with open(equity_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "equity"])
        for row in results["equity_curve"]:
            writer.writerow([row["timestamp"], row["equity"]])

    summary_path = output_dir / "summary.txt"
    with open(summary_path, "w", encoding="utf-8") as handle:
        handle.write(f"Backtest generated at {utc_now().isoformat()}\n")
        for key, value in results["metrics"].items():
            handle.write(f"{key}: {value}\n")
