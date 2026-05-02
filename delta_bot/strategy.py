from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional

from .data import Candle


@dataclass
class Signal:
    side: str
    reason: str
    atr: float
    fast_sma: float
    slow_sma: float
    stop_distance: float


def sma(values: List[float], window: int) -> float:
    if len(values) < window:
        raise ValueError("Not enough values for SMA")
    return sum(values[-window:]) / window


def atr(candles: List[Candle], window: int) -> float:
    if len(candles) < window + 1:
        raise ValueError("Not enough candles for ATR")
    trs: List[float] = []
    for idx in range(-window, 0):
        current = candles[idx]
        prev = candles[idx - 1]
        tr = max(
            current.high - current.low,
            abs(current.high - prev.close),
            abs(current.low - prev.close),
        )
        trs.append(tr)
    return sum(trs) / window


def generate_signal(candles: List[Candle], config: Dict[str, Dict[str, float]]) -> Optional[Signal]:
    strategy = config["strategy"]
    if len(candles) < strategy["slow_window"] + 1:
        return None

    closes = [candle.close for candle in candles]
    fast = sma(closes, strategy["fast_window"])
    slow = sma(closes, strategy["slow_window"])
    current_atr = atr(candles, strategy["atr_window"])
    current_price = closes[-1]
    atr_ratio = current_atr / current_price if current_price else 0.0

    if atr_ratio < strategy["min_atr_ratio"]:
        return Signal(
            side="flat",
            reason="atr_filter",
            atr=current_atr,
            fast_sma=fast,
            slow_sma=slow,
            stop_distance=current_atr * config["risk"]["stop_loss_atr"],
        )

    if fast > slow:
        return Signal(
            side="long",
            reason="fast_above_slow",
            atr=current_atr,
            fast_sma=fast,
            slow_sma=slow,
            stop_distance=current_atr * config["risk"]["stop_loss_atr"],
        )

    if fast < slow and strategy.get("allow_short", False):
        return Signal(
            side="short",
            reason="fast_below_slow",
            atr=current_atr,
            fast_sma=fast,
            slow_sma=slow,
            stop_distance=current_atr * config["risk"]["stop_loss_atr"],
        )

    return Signal(
        side="flat",
        reason="no_signal",
        atr=current_atr,
        fast_sma=fast,
        slow_sma=slow,
        stop_distance=current_atr * config["risk"]["stop_loss_atr"],
    )
