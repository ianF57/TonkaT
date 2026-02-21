from __future__ import annotations

from typing import Any

from app.signals.base_signal import BaseSignal, SignalCandidate


class BreakoutV1(BaseSignal):
    strategy_label = "breakout"
    version = "v1"
    compatible_regimes = ["momentum_breakout", "trending", "high_volatility"]
    compatible_timeframes = ["5m", "1h", "1d", "1w"]

    def __init__(self, breakout_window: int = 20, volume_multiplier: float = 1.15, stop_loss_pct: float = 0.018) -> None:
        self.breakout_window = breakout_window
        self.volume_multiplier = volume_multiplier
        self.stop_loss_pct = stop_loss_pct

    def generate(self, asset: str, timeframe: str, ohlcv: list[dict[str, Any]], regime: str) -> SignalCandidate | None:
        if timeframe not in self.compatible_timeframes or regime not in self.compatible_regimes:
            return None
        if len(ohlcv) < self.breakout_window + 1:
            return None

        recent = ohlcv[-(self.breakout_window + 1):-1]
        last = ohlcv[-1]
        prior_high = max(float(row["high"]) for row in recent)
        prior_low = min(float(row["low"]) for row in recent)
        avg_volume = sum(float(row.get("volume", 0.0)) for row in recent) / len(recent)
        last_close = float(last["close"])
        last_volume = float(last.get("volume", 0.0))

        direction: str | None = None
        if last_close > prior_high and last_volume >= avg_volume * self.volume_multiplier:
            direction = "long"
            stop_loss = round(last_close * (1.0 - self.stop_loss_pct), 6)
        elif last_close < prior_low and last_volume >= avg_volume * self.volume_multiplier:
            direction = "short"
            stop_loss = round(last_close * (1.0 + self.stop_loss_pct), 6)
        else:
            return None

        score = min(100.0, 50.0 + abs((last_close - (prior_high if direction == 'long' else prior_low)) / last_close) * 500.0)

        return SignalCandidate(
            strategy_label=self.strategy_label,
            version=self.version,
            direction=direction,
            entry_rule=f"Enter on {self.breakout_window}-bar breakout with volume confirmation.",
            exit_rule="Exit on failed breakout (re-entry into range) or target at 2.5R.",
            stop_loss=stop_loss,
            risk_reward=2.5,
            compatible_regimes=self.compatible_regimes,
            compatible_timeframes=self.compatible_timeframes,
            parameters={"breakout_window": self.breakout_window, "volume_multiplier": self.volume_multiplier, "stop_loss_pct": self.stop_loss_pct},
            performance_score=round(max(0.0, score), 2),
            metadata={"prior_high": round(prior_high, 6), "prior_low": round(prior_low, 6), "avg_volume": round(avg_volume, 3), "last_volume": round(last_volume, 3), "asset": asset},
        )
