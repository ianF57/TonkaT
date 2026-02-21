from __future__ import annotations

from statistics import mean
from typing import Any

from app.signals.base_signal import BaseSignal, SignalCandidate


class TrendSignalV1(BaseSignal):
    strategy_label = "trend_following"
    version = "v1"
    compatible_regimes = ["trending", "momentum_breakout", "high_volatility"]
    compatible_timeframes = ["5m", "1h", "1d", "1w"]

    def __init__(self, fast_window: int = 10, slow_window: int = 30, stop_loss_pct: float = 0.015) -> None:
        self.fast_window = fast_window
        self.slow_window = slow_window
        self.stop_loss_pct = stop_loss_pct

    def generate(self, asset: str, timeframe: str, ohlcv: list[dict[str, Any]], regime: str) -> SignalCandidate | None:
        if timeframe not in self.compatible_timeframes or regime not in self.compatible_regimes:
            return None
        closes = [float(row["close"]) for row in ohlcv]
        if len(closes) < self.slow_window + 2:
            return None

        fast_ma = mean(closes[-self.fast_window:])
        slow_ma = mean(closes[-self.slow_window:])
        last_price = closes[-1]
        prior_price = closes[-2]
        momentum = (last_price - prior_price) / prior_price if prior_price else 0.0

        if fast_ma <= slow_ma or momentum <= 0:
            return None

        risk_reward = 2.2
        stop_loss = round(last_price * (1.0 - self.stop_loss_pct), 6)
        score = min(100.0, 45.0 + (momentum * 1000.0) + ((fast_ma - slow_ma) / slow_ma) * 220.0)

        return SignalCandidate(
            strategy_label=self.strategy_label,
            version=self.version,
            direction="long",
            entry_rule="Fast MA above slow MA with positive short-term momentum.",
            exit_rule="Exit on fast MA cross below slow MA or take-profit at 2.2R.",
            stop_loss=stop_loss,
            risk_reward=risk_reward,
            compatible_regimes=self.compatible_regimes,
            compatible_timeframes=self.compatible_timeframes,
            parameters={"fast_window": self.fast_window, "slow_window": self.slow_window, "stop_loss_pct": self.stop_loss_pct},
            performance_score=round(max(0.0, score), 2),
            metadata={"fast_ma": round(fast_ma, 6), "slow_ma": round(slow_ma, 6), "momentum": round(momentum, 6), "asset": asset},
        )
