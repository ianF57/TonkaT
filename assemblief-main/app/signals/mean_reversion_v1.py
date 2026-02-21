from __future__ import annotations

from statistics import mean, pstdev
from typing import Any

from app.signals.base_signal import BaseSignal, SignalCandidate


class MeanReversionV1(BaseSignal):
    strategy_label = "mean_reversion"
    version = "v1"
    compatible_regimes = ["ranging", "low_volatility", "mean_reversion"]
    compatible_timeframes = ["1m", "5m", "1h", "1d"]

    def __init__(self, lookback: int = 20, z_threshold: float = 1.3, stop_loss_pct: float = 0.01) -> None:
        self.lookback = lookback
        self.z_threshold = z_threshold
        self.stop_loss_pct = stop_loss_pct

    def generate(self, asset: str, timeframe: str, ohlcv: list[dict[str, Any]], regime: str) -> SignalCandidate | None:
        if timeframe not in self.compatible_timeframes or regime not in self.compatible_regimes:
            return None
        closes = [float(row["close"]) for row in ohlcv]
        if len(closes) < self.lookback + 2:
            return None

        window = closes[-self.lookback:]
        mu = mean(window)
        sigma = pstdev(window) if len(window) > 1 else 0.0
        if sigma == 0:
            return None

        price = closes[-1]
        z_score = (price - mu) / sigma
        if abs(z_score) < self.z_threshold:
            return None

        direction = "short" if z_score > 0 else "long"
        stop_loss = round(price * (1.0 + self.stop_loss_pct), 6) if direction == "short" else round(price * (1.0 - self.stop_loss_pct), 6)
        risk_reward = 1.7
        score = min(100.0, 40.0 + abs(z_score) * 20.0)

        return SignalCandidate(
            strategy_label=self.strategy_label,
            version=self.version,
            direction=direction,
            entry_rule=f"Enter {direction} when z-score exceeds ±{self.z_threshold}.",
            exit_rule="Exit at mean reversion target (moving average) or at stop-loss.",
            stop_loss=stop_loss,
            risk_reward=risk_reward,
            compatible_regimes=self.compatible_regimes,
            compatible_timeframes=self.compatible_timeframes,
            parameters={"lookback": self.lookback, "z_threshold": self.z_threshold, "stop_loss_pct": self.stop_loss_pct},
            performance_score=round(max(0.0, score), 2),
            metadata={"z_score": round(z_score, 4), "mean": round(mu, 6), "std_dev": round(sigma, 6), "asset": asset},
        )
