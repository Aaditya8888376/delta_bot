from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from .utils import ensure_dir, timeframe_seconds


@dataclass
class Candle:
    timestamp: int
    datetime: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass
class FundingRate:
    timestamp: int
    datetime: str
    rate: float


def _format_datetime(timestamp_ms: int) -> str:
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc).isoformat()


def fetch_ohlcv_history(exchange: Any, symbol: str, timeframe: str, since: str) -> List[Candle]:
    since_ms = exchange.parse8601(since) if since else None
    timeframe_ms = timeframe_seconds(timeframe) * 1000
    now_ms = exchange.milliseconds()
    all_rows: List[Candle] = []

    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since_ms, limit=1000)
        if not batch:
            break
        for row in batch:
            timestamp = int(row[0])
            if all_rows and timestamp <= all_rows[-1].timestamp:
                continue
            all_rows.append(
                Candle(
                    timestamp=timestamp,
                    datetime=_format_datetime(timestamp),
                    open=float(row[1]),
                    high=float(row[2]),
                    low=float(row[3]),
                    close=float(row[4]),
                    volume=float(row[5]),
                )
            )
        since_ms = all_rows[-1].timestamp + timeframe_ms
        if since_ms >= now_ms:
            break
    return all_rows


def fetch_funding_rate_history(exchange: Any, symbol: str, since: str) -> List[FundingRate]:
    if not exchange.has.get("fetchFundingRateHistory"):
        return []
    since_ms = exchange.parse8601(since) if since else None
    batch = exchange.fetch_funding_rate_history(symbol, since=since_ms, limit=1000)
    results: List[FundingRate] = []
    for entry in batch:
        timestamp = int(entry["timestamp"])
        results.append(
            FundingRate(
                timestamp=timestamp,
                datetime=_format_datetime(timestamp),
                rate=float(entry.get("fundingRate") or entry.get("rate") or 0.0),
            )
        )
    return results


def save_ohlcv_csv(path: Path, candles: List[Candle]) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "datetime", "open", "high", "low", "close", "volume"])
        for candle in candles:
            writer.writerow(
                [
                    candle.timestamp,
                    candle.datetime,
                    candle.open,
                    candle.high,
                    candle.low,
                    candle.close,
                    candle.volume,
                ]
            )


def load_ohlcv_csv(path: Path) -> List[Candle]:
    candles: List[Candle] = []
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            candles.append(
                Candle(
                    timestamp=int(row["timestamp"]),
                    datetime=row["datetime"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                )
            )
    return candles


def save_funding_csv(path: Path, rates: List[FundingRate]) -> None:
    ensure_dir(path.parent)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "datetime", "rate"])
        for rate in rates:
            writer.writerow([rate.timestamp, rate.datetime, rate.rate])


def load_funding_csv(path: Path) -> List[FundingRate]:
    rates: List[FundingRate] = []
    with open(path, "r", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            rates.append(
                FundingRate(
                    timestamp=int(row["timestamp"]),
                    datetime=row["datetime"],
                    rate=float(row["rate"]),
                )
            )
    return rates


def validate_ohlcv(candles: List[Candle], timeframe: str) -> List[str]:
    issues: List[str] = []
    if not candles:
        return ["No candles fetched"]
    expected_gap = timeframe_seconds(timeframe) * 1000
    seen = set()
    prev = candles[0].timestamp
    for candle in candles:
        if candle.timestamp in seen:
            issues.append(f"Duplicate timestamp: {candle.timestamp}")
        seen.add(candle.timestamp)
        gap = candle.timestamp - prev
        if gap > expected_gap:
            issues.append(f"Gap detected: {gap / 1000:.0f}s at {candle.datetime}")
        prev = candle.timestamp
    return issues


def make_ohlcv_path(data_dir: str, symbol: str, timeframe: str) -> Path:
    safe_symbol = symbol.replace("/", "-")
    return Path(data_dir) / f"ohlcv_{safe_symbol}_{timeframe}.csv"


def make_funding_path(data_dir: str, symbol: str) -> Path:
    safe_symbol = symbol.replace("/", "-")
    return Path(data_dir) / f"funding_{safe_symbol}.csv"
