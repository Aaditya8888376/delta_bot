from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

_TIMEFRAME_SECONDS = {
    "1m": 60,
    "3m": 180,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "2h": 7200,
    "4h": 14400,
    "6h": 21600,
    "12h": 43200,
    "1d": 86400,
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def timeframe_seconds(timeframe: str) -> int:
    if timeframe not in _TIMEFRAME_SECONDS:
        raise ValueError(f"Unsupported timeframe: {timeframe}")
    return _TIMEFRAME_SECONDS[timeframe]


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def load_json(path: str | Path) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def save_json(path: str | Path, payload: Dict[str, Any]) -> None:
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)


def resolve_env(value: Any) -> Any:
    if isinstance(value, str) and value.startswith("env:"):
        key = value[4:]
        resolved = os.getenv(key)
        if resolved is None:
            raise ValueError(f"Missing environment variable: {key}")
        return resolved
    if isinstance(value, dict):
        return {k: resolve_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_env(v) for v in value]
    return value


def chunked(items: List[Any], size: int) -> Iterable[List[Any]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def apply_slippage(price: float, side: str, bps: float) -> float:
    delta = price * (bps / 10000)
    return price + delta if side == "buy" else price - delta


def calculate_position_size(risk: Dict[str, float], price: float, stop_distance: float, equity: float) -> float:
    if stop_distance <= 0:
        return 0.0
    risk_amount = equity * risk["risk_per_trade"]
    qty_by_risk = risk_amount / stop_distance
    max_notional = equity * risk["max_leverage"]
    max_qty = max_notional / price
    max_qty_by_pct = (equity * risk["max_position_pct"]) / price
    return max(0.0, min(qty_by_risk, max_qty, max_qty_by_pct))
