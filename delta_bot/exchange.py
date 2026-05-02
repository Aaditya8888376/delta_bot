from __future__ import annotations

from typing import Any, Dict

import ccxt


def create_exchange(config: Dict[str, Any], *, for_trading: bool) -> ccxt.Exchange:
    name = config["exchange"]["name"]
    if not hasattr(ccxt, name):
        raise ValueError(f"Unsupported exchange in ccxt: {name}")

    params: Dict[str, Any] = {
        "enableRateLimit": config["exchange"].get("enable_rate_limit", True),
        "timeout": config["exchange"].get("timeout_ms", 30000),
    }
    api_key = config["exchange"].get("api_key")
    api_secret = config["exchange"].get("api_secret")
    if api_key and api_secret:
        params.update({"apiKey": api_key, "secret": api_secret})
    elif for_trading:
        raise ValueError("api_key and api_secret are required for trading")

    exchange_class = getattr(ccxt, name)
    exchange = exchange_class(params)

    market_type = config["market"].get("type", "spot")
    if hasattr(exchange, "options"):
        exchange.options = exchange.options or {}
        if market_type in {"perp", "swap", "futures"}:
            exchange.options["defaultType"] = "swap"
        else:
            exchange.options["defaultType"] = "spot"

    if config["exchange"].get("sandbox"):
        if hasattr(exchange, "set_sandbox_mode"):
            exchange.set_sandbox_mode(True)

    return exchange
