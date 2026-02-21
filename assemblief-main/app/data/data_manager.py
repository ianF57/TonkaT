from __future__ import annotations

from datetime import datetime, timedelta
import logging

import httpx
from sqlalchemy import select

from app.data.base_provider import OHLCVPoint
from app.data.binance_provider import BinanceProvider
from app.data.database import get_db_session
from app.data.forex_provider import ForexProvider
from app.data.futures_provider import FuturesProvider
from app.data.models import OHLCVCache

logger = logging.getLogger(__name__)

CACHE_TTL_MINUTES: dict[str, int] = {
    "1m": 5,
    "5m": 15,
    "15m": 30,
    "30m": 45,
    "1h": 60,
    "4h": 240,
    "1d": 1440,
    "1w": 10080,
}
DEFAULT_CACHE_TTL_MINUTES = 60


class DataManager:
    """Unified market-data entrypoint with local SQLite caching."""

    def __init__(self) -> None:
        self.providers = {
            "crypto": BinanceProvider(),
            "forex": ForexProvider(),
            "futures": FuturesProvider(),
        }

    def _resolve_market(self, asset: str) -> tuple[str, str]:
        if ":" in asset:
            market, symbol = asset.split(":", 1)
            market = market.lower()
            if market in self.providers:
                return market, symbol
        symbol = asset.upper()
        if symbol.endswith("USDT"):
            return "crypto", symbol
        if symbol.endswith("=F"):
            return "futures", symbol[:-2]
        if symbol.endswith("=X"):
            return "forex", symbol[:-2]
        raise ValueError("Asset must include market prefix (crypto:, forex:, futures:) or a known symbol suffix")

    async def get_ohlcv(self, asset: str, timeframe: str, limit: int = 300) -> dict[str, object]:
        market, symbol = self._resolve_market(asset)
        provider = self.providers[market]

        cached_points = self._load_cached(provider.name, symbol, timeframe, limit)
        if cached_points:
            logger.info("Serving %s/%s %s candles from cache", market, symbol, timeframe)
            return {
                "asset": f"{market}:{symbol}",
                "provider": provider.name,
                "timeframe": timeframe,
                "source": "cache",
                "rows": len(cached_points),
                "data": cached_points,
            }

        try:
            fetched_points = await provider.fetch_ohlcv(symbol, timeframe, limit=limit)
        except httpx.HTTPError as exc:
            logger.exception("Provider HTTP error for %s/%s", market, symbol)
            raise RuntimeError("Upstream provider request failed") from exc
        except ValueError:
            raise
        except Exception as exc:
            logger.exception("Unexpected provider error for %s/%s", market, symbol)
            raise RuntimeError("Unexpected error fetching market data") from exc

        self._store_points(provider.name, symbol, timeframe, fetched_points)
        return {
            "asset": f"{market}:{symbol}",
            "provider": provider.name,
            "timeframe": timeframe,
            "source": "provider",
            "rows": len(fetched_points),
            "data": fetched_points,
        }

    def _load_cached(self, provider: str, asset: str, timeframe: str, limit: int) -> list[dict[str, object]]:
        ttl_minutes = CACHE_TTL_MINUTES.get(timeframe, DEFAULT_CACHE_TTL_MINUTES)
        fresh_cutoff = datetime.utcnow() - timedelta(minutes=ttl_minutes)

        with get_db_session() as session:
            stmt = (
                select(OHLCVCache)
                .where(OHLCVCache.provider == provider)
                .where(OHLCVCache.asset == asset)
                .where(OHLCVCache.timeframe == timeframe)
                .where(OHLCVCache.fetched_at > fresh_cutoff)
                .order_by(OHLCVCache.timestamp.asc())
            )
            rows = session.execute(stmt).scalars().all()

            if not rows:
                return []

            # Read all attributes inside the session before it closes
            points = [
                {
                    "timestamp": row.timestamp.isoformat(),
                    "open": row.open,
                    "high": row.high,
                    "low": row.low,
                    "close": row.close,
                    "volume": row.volume,
                }
                for row in rows[-limit:]
            ]

        return points

    def _store_points(self, provider: str, asset: str, timeframe: str, points: list[OHLCVPoint]) -> None:
        if not points:
            return

        normalized_points = [
            {
                "timestamp": point["timestamp"] if isinstance(point["timestamp"], datetime) else datetime.fromisoformat(str(point["timestamp"])),
                "open": point["open"],
                "high": point["high"],
                "low": point["low"],
                "close": point["close"],
                "volume": point["volume"],
            }
            for point in points
        ]

        incoming_timestamps = {point["timestamp"] for point in normalized_points}

        with get_db_session() as session:
            existing_timestamps = set(
                session.execute(
                    select(OHLCVCache.timestamp)
                    .where(OHLCVCache.provider == provider)
                    .where(OHLCVCache.asset == asset)
                    .where(OHLCVCache.timeframe == timeframe)
                    .where(OHLCVCache.timestamp.in_(incoming_timestamps))
                ).scalars()
            )

            objects = [
                OHLCVCache(
                    provider=provider,
                    asset=asset,
                    timeframe=timeframe,
                    timestamp=point["timestamp"],
                    open=point["open"],
                    high=point["high"],
                    low=point["low"],
                    close=point["close"],
                    volume=point["volume"],
                )
                for point in normalized_points
                if point["timestamp"] not in existing_timestamps
            ]

            if objects:
                session.bulk_save_objects(objects)
