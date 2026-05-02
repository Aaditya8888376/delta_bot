from __future__ import annotations

from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

from .utils import load_json, resolve_env, save_json

DEFAULT_CONFIG: Dict[str, Any] = {
    "exchange": {
        "name": "delta",
        "api_key": "",
        "api_secret": "",
        "sandbox": True,
        "enable_rate_limit": True,
        "timeout_ms": 30000,
    },
    "market": {"type": "perp", "symbol": "BTC/USDT", "timeframe": "1h", "leverage": 3},
    "risk": {
        "capital_usd": 1000,
        "risk_per_trade": 0.01,
        "max_leverage": 3,
        "max_position_pct": 0.2,
        "max_daily_loss_pct": 0.05,
        "max_drawdown_pct": 0.2,
        "fee_bps": 7,
        "slippage_bps": 5,
        "stop_loss_atr": 2,
        "take_profit_atr": 4,
    },
    "strategy": {
        "name": "sma_trend_atr_filter",
        "fast_window": 20,
        "slow_window": 50,
        "atr_window": 14,
        "min_atr_ratio": 0.002,
        "allow_short": True,
    },
    "data": {"since": "2023-01-01T00:00:00Z", "data_dir": "data"},
    "execution": {"order_type": "market", "polling_seconds": 60},
    "logging": {"log_dir": "logs"},
}


def merge_dict(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    merged = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = merge_dict(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: str | Path) -> Dict[str, Any]:
    payload = load_json(path)
    merged = merge_dict(DEFAULT_CONFIG, payload)
    resolved = resolve_env(merged)
    validate_config(resolved)
    return resolved


def validate_config(config: Dict[str, Any]) -> None:
    required_sections = ["exchange", "market", "risk", "strategy", "data", "execution", "logging"]
    for section in required_sections:
        if section not in config:
            raise ValueError(f"Missing config section: {section}")
    if not config["market"]["symbol"]:
        raise ValueError("Market symbol is required")
    if config["risk"]["risk_per_trade"] <= 0:
        raise ValueError("risk_per_trade must be positive")
    if config["risk"]["max_leverage"] <= 0:
        raise ValueError("max_leverage must be positive")


def write_sample_config(path: str | Path) -> None:
    save_json(path, DEFAULT_CONFIG)
