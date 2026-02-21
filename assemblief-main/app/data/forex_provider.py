from __future__ import annotations

import logging

from app.data.base_provider import OHLCVPoint
from app.data.yahoo_provider_base import YahooChartProvider

logger = logging.getLogger(__name__)


class ForexProvider(YahooChartProvider):
    """Forex OHLCV provider using Yahoo Finance public chart endpoints."""

    name = "forex"

    async def fetch_ohlcv(self, asset: str, timeframe: str, limit: int = 300) -> list[OHLCVPoint]:
        symbol = asset if asset.endswith("=X") else f"{asset}=X"
        logger.info("Fetching %s %s candles from Yahoo Forex", symbol, timeframe)
        points = await self._fetch_chart(symbol=symbol, timeframe=timeframe)
        return points[-limit:]
