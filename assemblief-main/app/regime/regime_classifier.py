from __future__ import annotations

from collections import Counter, OrderedDict
from dataclasses import dataclass
from datetime import datetime
from threading import Lock
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
    _max_cache_size = 50

    def __init__(self) -> None:
        self._cache: OrderedDict[tuple[str, str, str, int], RegimeSnapshot] = OrderedDict()
        self._cache_lock = Lock()

    def classify(
        self,
        candles: list[dict[str, Any]],
        asset: str = "unknown",
        timeframe: str = "unknown",
    ) -> RegimeSnapshot:
        cache_key = self._cache_key(candles=candles, asset=asset, timeframe=timeframe)
        if cache_key is not None:
            cached = self._cache_get(cache_key)
            if cached is not None:
                return cached

        if len(candles) < 25:
            snapshot = RegimeSnapshot(
                current_regime="ranging",
                confidence_score=35.0,
                historical_distribution={name: 0.0 for name in self._regimes},
            )
            if cache_key is not None:
                self._cache_set(cache_key, snapshot)
            return snapshot

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
        snapshot = RegimeSnapshot(
            current_regime=current_label,
            confidence_score=round(current_conf, 2),
            historical_distribution=distribution,
        )

        if cache_key is not None:
            self._cache_set(cache_key, snapshot)
        return snapshot

    def _cache_key(
        self,
        candles: list[dict[str, Any]],
        asset: str,
        timeframe: str,
    ) -> tuple[str, str, str, int] | None:
        if not candles:
            return None

        last_timestamp = candles[-1].get("timestamp")
        if isinstance(last_timestamp, datetime):
            ts_key = last_timestamp.isoformat()
        elif last_timestamp is None:
            ts_key = "none"
        else:
            ts_key = str(last_timestamp)

        return ts_key, asset, timeframe, len(candles)

    def _cache_get(self, key: tuple[str, str, str, int]) -> RegimeSnapshot | None:
        with self._cache_lock:
            cached = self._cache.get(key)
            if cached is None:
                return None
            self._cache.move_to_end(key)
            return cached

    def _cache_set(self, key: tuple[str, str, str, int], snapshot: RegimeSnapshot) -> None:
        with self._cache_lock:
            self._cache[key] = snapshot
            self._cache.move_to_end(key)
            while len(self._cache) > self._max_cache_size:
                self._cache.popitem(last=False)

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
