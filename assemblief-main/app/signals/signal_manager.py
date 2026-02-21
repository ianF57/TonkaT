from __future__ import annotations

from dataclasses import asdict
from typing import Any

from app.data.data_manager import DataManager
from app.regime.regime_classifier import RegimeClassifier
from app.signals.base_signal import SignalCandidate
from app.signals.breakout_v1 import BreakoutV1
from app.signals.mean_reversion_v1 import MeanReversionV1
from app.signals.trend_signal_v1 import TrendSignalV1


class SignalManager:
    """Generate and rank candidate strategy signals."""

    def __init__(self) -> None:
        self.data_manager = DataManager()
        self.regime_classifier = RegimeClassifier()
        self.strategies = [
            TrendSignalV1(),
            MeanReversionV1(),
            BreakoutV1(),
        ]

    async def generate_signals(self, asset: str, timeframe: str = "1h") -> dict[str, Any]:
        ohlcv_response = await self.data_manager.get_ohlcv(asset=asset, timeframe=timeframe)
        data = ohlcv_response["data"]
        regime_snapshot = self.regime_classifier.classify(data)

        candidates: list[SignalCandidate] = []
        for strategy in self.strategies:
            candidate = strategy.generate(
                asset=ohlcv_response["asset"],
                timeframe=timeframe,
                ohlcv=data,
                regime=regime_snapshot.current_regime,
            )
            if candidate is not None:
                candidates.append(candidate)

        ranked = sorted(candidates, key=lambda c: c.performance_score, reverse=True)
        return {
            "asset": ohlcv_response["asset"],
            "timeframe": timeframe,
            "regime": regime_snapshot.current_regime,
            "confidence_score": regime_snapshot.confidence_score,
            "signals": [asdict(signal) for signal in ranked],
            "signal_count": len(ranked),
        }
