from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from typing import Any

from app.regime.confidence_scoring import (
    score_breakout,
    score_mean_reversion,
    score_ranging,
    score_trending,
    score_volatility,
)
from app.regime.indicators import Candle, adx, hurst_exponent, rolling_volatility, rsi, volatility_clustering


@dataclass(frozen=True)
class RegimeSnapshot:
    current_regime: str
    confidence_score: float
    historical_distribution: dict[str, float]


class RegimeClassifier:
    """Classify market regime from OHLCV history."""

    _regimes = (
        "trending",
        "ranging",
        "high_volatility",
        "low_volatility",
        "momentum_breakout",
        "mean_reversion",
    )

    def classify(self, candles: list[dict[str, Any]]) -> RegimeSnapshot:
        if len(candles) < 25:
            return RegimeSnapshot(
                current_regime="ranging",
                confidence_score=35.0,
                historical_distribution={name: 0.0 for name in self._regimes},
            )

        normalized = [
            Candle(
                open=float(point["open"]),
                high=float(point["high"]),
                low=float(point["low"]),
                close=float(point["close"]),
                volume=float(point.get("volume", 0.0)),
            )
            for point in candles
        ]

        current_label, current_conf = self._classify_window(normalized)

        history_labels: list[str] = []
        sample_window = min(80, len(normalized))
        for idx in range(sample_window, len(normalized) + 1):
            window = normalized[max(0, idx - sample_window):idx]
            label, _ = self._classify_window(window)
            history_labels.append(label)

        distribution = self._distribution(history_labels)
        return RegimeSnapshot(
            current_regime=current_label,
            confidence_score=round(current_conf, 2),
            historical_distribution=distribution,
        )

    def _classify_window(self, candles: list[Candle]) -> tuple[str, float]:
        closes = [c.close for c in candles]
        vol = rolling_volatility(closes)
        adx_value = adx(candles)
        rsi_value = rsi(closes)
        clustering = volatility_clustering(closes)
        hurst = hurst_exponent(closes)

        scores = {
            "trending": score_trending(adx_value, hurst, rsi_value),
            "ranging": score_ranging(adx_value, hurst, rsi_value),
            "high_volatility": score_volatility(vol, clustering, high=True),
            "low_volatility": score_volatility(vol, clustering, high=False),
            "momentum_breakout": score_breakout(adx_value, rsi_value, vol),
            "mean_reversion": score_mean_reversion(rsi_value, adx_value, hurst),
        }

        label, score = max(scores.items(), key=lambda item: item[1])
        return label, score

    def _distribution(self, labels: list[str]) -> dict[str, float]:
        if not labels:
            return {name: 0.0 for name in self._regimes}
        counts = Counter(labels)
        total = len(labels)
        return {name: round((counts.get(name, 0) / total) * 100.0, 2) for name in self._regimes}
