from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import TypedDict


SUPPORTED_TIMEFRAMES = {"1m", "5m", "1h", "1d", "1w"}


class OHLCVPoint(TypedDict):
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class BaseDataProvider(ABC):
    """Contract for market-data providers."""

    name: str

    @abstractmethod
    async def fetch_ohlcv(self, asset: str, timeframe: str, limit: int = 300) -> list[OHLCVPoint]:
        """Fetch OHLCV data from upstream provider."""

    def validate_timeframe(self, timeframe: str) -> None:
        if timeframe not in SUPPORTED_TIMEFRAMES:
            raise ValueError(f"Unsupported timeframe '{timeframe}'. Supported: {sorted(SUPPORTED_TIMEFRAMES)}")
