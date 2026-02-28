from __future__ import annotations

from statistics import mean
from typing import Any

from app.signals.base_signal import BaseSignal


class MeanReversionV1(BaseSignal):
    def __init__(self, window: int = 20, threshold: float = 0.02) -> None:
        self.window = window
        self.threshold = threshold

    def generate(self, history_df: list[dict[str, Any]]) -> dict[str, Any]:
        closes = [float(row["close"]) for row in history_df]
        if len(closes) < self.window:
            return {"action": "hold", "confidence": 0.0, "metadata": {"reason": "insufficient_data"}}

        baseline = mean(closes[-self.window:])
        current = closes[-1]
        z = (current - baseline) / baseline if baseline else 0.0

        if z <= -self.threshold:
            action = "buy"
        elif z >= self.threshold:
            action = "sell"
        else:
            action = "hold"

        return {
            "action": action,
            "confidence": round(min(1.0, abs(z) / max(self.threshold, 1e-9)), 4),
            "metadata": {"deviation": round(z, 6), "baseline": round(baseline, 6)},
        }
