from __future__ import annotations

from statistics import mean
from typing import Any

from app.signals.base_signal import BaseSignal


class TrendSignalV1(BaseSignal):
    def __init__(self, fast_window: int = 10, slow_window: int = 30) -> None:
        self.fast_window = fast_window
        self.slow_window = slow_window

    def generate(self, history_df: list[dict[str, Any]]) -> dict[str, Any]:
        closes = [float(row["close"]) for row in history_df]
        if len(closes) < self.slow_window + 2:
            return {"action": "hold", "confidence": 0.0, "metadata": {"reason": "insufficient_data"}}

        fast = mean(closes[-self.fast_window:])
        slow = mean(closes[-self.slow_window:])
        momentum = (closes[-1] - closes[-2]) / closes[-2] if closes[-2] else 0.0

        if fast > slow and momentum > 0:
            action = "buy"
        elif fast < slow and momentum < 0:
            action = "sell"
        else:
            action = "hold"

        return {
            "action": action,
            "confidence": round(min(1.0, abs(fast - slow) / slow) if slow else 0.0, 4),
            "metadata": {"fast_ma": round(fast, 6), "slow_ma": round(slow, 6), "momentum": round(momentum, 6)},
        }
