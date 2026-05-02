from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path

from .backtest import run_backtest, write_backtest_results
from .config import load_config, write_sample_config
from .data import (
    fetch_funding_rate_history,
    fetch_ohlcv_history,
    load_funding_csv,
    load_ohlcv_csv,
    make_funding_path,
    make_ohlcv_path,
    save_funding_csv,
    save_ohlcv_csv,
    validate_ohlcv,
)
from .exchange import create_exchange
from .trade import run_trading
from .utils import ensure_dir


def _config_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--config", default="config.json", help="Path to config.json")


def cmd_init_config(args: argparse.Namespace) -> None:
    path = Path(args.config)
    if path.exists() and not args.force:
        raise SystemExit(f"Config already exists: {path}")
    write_sample_config(path)
    print(f"Wrote sample config to {path}")


def cmd_fetch_data(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    exchange = create_exchange(config, for_trading=False)

    symbol = config["market"]["symbol"]
    timeframe = config["market"]["timeframe"]
    data_dir = config["data"]["data_dir"]

    candles = fetch_ohlcv_history(exchange, symbol, timeframe, config["data"]["since"])
    ohlcv_path = make_ohlcv_path(data_dir, symbol, timeframe)
    save_ohlcv_csv(ohlcv_path, candles)
    print(f"Saved {len(candles)} candles to {ohlcv_path}")

    issues = validate_ohlcv(candles, timeframe)
    if issues:
        print("Data validation issues:")
        for issue in issues:
            print(f"- {issue}")

    funding = fetch_funding_rate_history(exchange, symbol, config["data"]["since"])
    if funding:
        funding_path = make_funding_path(data_dir, symbol)
        save_funding_csv(funding_path, funding)
        print(f"Saved {len(funding)} funding rows to {funding_path}")
    else:
        print("No funding history available from exchange API.")


def cmd_backtest(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    symbol = config["market"]["symbol"]
    timeframe = config["market"]["timeframe"]
    data_dir = config["data"]["data_dir"]

    ohlcv_path = make_ohlcv_path(data_dir, symbol, timeframe)
    if not ohlcv_path.exists():
        raise SystemExit("Missing OHLCV data. Run fetch-data first.")

    candles = load_ohlcv_csv(ohlcv_path)
    funding_path = make_funding_path(data_dir, symbol)
    funding = load_funding_csv(funding_path) if funding_path.exists() else []

    results = run_backtest(config, candles, funding)
    output_dir = ensure_dir(Path("results") / datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S"))
    write_backtest_results(results, output_dir)
    print(f"Backtest complete. Results in {output_dir}")


def cmd_trade(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    run_trading(config, paper=not args.live, once=args.once)


def cmd_health(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    exchange = create_exchange(config, for_trading=False)
    markets = exchange.load_markets()
    symbol = config["market"]["symbol"]
    if symbol not in markets:
        raise SystemExit(f"Symbol not found on exchange: {symbol}")
    ticker = exchange.fetch_ticker(symbol)
    print(f"OK: {symbol} last={ticker.get('last')}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="delta-bot", description="Delta Exchange algo bot")
    sub = parser.add_subparsers(dest="command", required=True)

    init_cmd = sub.add_parser("init-config", help="Create a sample config.json")
    _config_arg(init_cmd)
    init_cmd.add_argument("--force", action="store_true", help="Overwrite existing config")
    init_cmd.set_defaults(func=cmd_init_config)

    fetch_cmd = sub.add_parser("fetch-data", help="Fetch historical OHLCV and funding data")
    _config_arg(fetch_cmd)
    fetch_cmd.set_defaults(func=cmd_fetch_data)

    backtest_cmd = sub.add_parser("backtest", help="Run backtest using stored data")
    _config_arg(backtest_cmd)
    backtest_cmd.set_defaults(func=cmd_backtest)

    trade_cmd = sub.add_parser("trade", help="Run live or paper trading loop")
    _config_arg(trade_cmd)
    trade_cmd.add_argument("--live", action="store_true", help="Use real orders (default is paper)")
    trade_cmd.add_argument("--once", action="store_true", help="Run a single cycle and exit")
    trade_cmd.set_defaults(func=cmd_trade)

    health_cmd = sub.add_parser("health", help="Check exchange connectivity")
    _config_arg(health_cmd)
    health_cmd.set_defaults(func=cmd_health)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
