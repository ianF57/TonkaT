from __future__ import annotations

from typing import Any

from app.signals.base_signal import BaseSignal


class BreakoutV1(BaseSignal):
    def __init__(self, window: int = 20) -> None:
        self.window = window

    def generate(self, history_df: list[dict[str, Any]]) -> dict[str, Any]:
        if len(history_df) < self.window + 1:
            return {"action": "hold", "confidence": 0.0, "metadata": {"reason": "insufficient_data"}}

        recent = history_df[-self.window - 1 : -1]
        high = max(float(r["high"]) for r in recent)
        low = min(float(r["low"]) for r in recent)
        close = float(history_df[-1]["close"])

        if close > high:
            action = "buy"
        elif close < low:
            action = "sell"
        else:
            action = "hold"

        rng = max(1e-9, high - low)
        confidence = min(1.0, abs(close - ((high + low) / 2)) / rng)
        return {
            "action": action,
            "confidence": round(confidence, 4),
            "metadata": {"breakout_high": round(high, 6), "breakout_low": round(low, 6)},
        }
