from __future__ import annotations

from datetime import UTC, datetime
import logging

import httpx

from app.data.base_provider import BaseDataProvider, OHLCVPoint

logger = logging.getLogger(__name__)


class BinanceProvider(BaseDataProvider):
    """Binance public market data provider for crypto symbols."""

    name = "binance"
    _base_url = "https://api.binance.com"

    async def fetch_ohlcv(self, asset: str, timeframe: str, limit: int = 300) -> list[OHLCVPoint]:
        self.validate_timeframe(timeframe)
        symbol = asset.upper()
        endpoint = f"{self._base_url}/api/v3/klines"
        params = {"symbol": symbol, "interval": timeframe, "limit": min(limit, 1000)}

        logger.info("Fetching %s %s candles from Binance", symbol, timeframe)
        async with httpx.AsyncClient(timeout=15.0) as client:
            response = await client.get(endpoint, params=params)
            response.raise_for_status()
            klines = response.json()

        points: list[OHLCVPoint] = []
        for row in klines:
            points.append(
                {
                    "timestamp": datetime.fromtimestamp(row[0] / 1000, tz=UTC),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                }
            )
        return points
