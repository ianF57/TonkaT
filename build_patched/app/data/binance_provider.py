from __future__ import annotations

import asyncio
from datetime import UTC, datetime
import logging

import httpx

from app.data.base_provider import BaseDataProvider, OHLCVPoint

logger = logging.getLogger(__name__)

_BINANCE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


class BinanceProvider(BaseDataProvider):
    """Binance public market data provider for crypto symbols."""

    name = "binance"
    _base_url = "https://api.binance.com"

    async def fetch_ohlcv(self, asset: str, timeframe: str, limit: int = 300) -> list[OHLCVPoint]:
        self.validate_timeframe(timeframe)
        symbol   = asset.upper()
        endpoint = f"{self._base_url}/api/v3/klines"
        params   = {"symbol": symbol, "interval": timeframe, "limit": min(limit, 1000)}

        logger.info("Fetching %s %s candles from Binance", symbol, timeframe)

        last_exc: Exception = RuntimeError(f"Binance fetch failed for {symbol}")
        for attempt in range(3):
            try:
                async with httpx.AsyncClient(
                    headers=_BINANCE_HEADERS,
                    follow_redirects=True,
                    timeout=15.0,
                ) as client:
                    response = await client.get(endpoint, params=params)
                    if response.status_code == 429:
                        wait = 2.0 ** attempt
                        logger.warning("Binance 429 for %s — waiting %.1fs", symbol, wait)
                        await asyncio.sleep(wait)
                        continue
                    response.raise_for_status()
                    klines = response.json()
                break
            except httpx.HTTPStatusError as exc:
                logger.warning("Binance HTTP %s (attempt %d): %s", exc.response.status_code, attempt + 1, exc)
                last_exc = exc
                if attempt < 2:
                    await asyncio.sleep(1.5)
            except httpx.RequestError as exc:
                logger.warning("Binance request error (attempt %d): %s", attempt + 1, exc)
                last_exc = exc
                if attempt < 2:
                    await asyncio.sleep(1.0)
        else:
            raise last_exc

        points: list[OHLCVPoint] = []
        for row in klines:
            points.append(
                {
                    "timestamp": datetime.fromtimestamp(row[0] / 1000, tz=UTC),
                    "open":      float(row[1]),
                    "high":      float(row[2]),
                    "low":       float(row[3]),
                    "close":     float(row[4]),
                    "volume":    float(row[5]),
                }
            )
        return points
