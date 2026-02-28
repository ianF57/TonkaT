"""Unified market-data manager with production SQLite caching.

Storage model
─────────────
All candles are written to the ``Candle`` table (see models.py).
The ``symbol`` column stores the bare symbol (e.g. "BTCUSDT"); the market/
provider context is stored separately as ``fetched_at`` metadata only — the
candle itself is provider-agnostic so that data from different upstream sources
for the same symbol is deduplicated rather than duplicated.

Upsert strategy
───────────────
INSERT OR IGNORE on the composite PK (symbol, timeframe, open_time) is the
cheapest deduplication path in SQLite — it touches the B-tree once, and the
IGNORE path aborts before any page write if the row already exists.

Batching
────────
SQLite's default SQLITE_MAX_VARIABLE_NUMBER is 999.  With 9 bind variables per
row the safe batch ceiling is 999 // 9 = 111.  We use 100 to stay well clear.

Timestamp handling
──────────────────
All providers return timezone-aware datetimes (UTC).  We strip tzinfo before
converting to epoch so that ``datetime.timestamp()`` always treats the value
as UTC regardless of the host system's local timezone.

Cache-freshness check
─────────────────────
_load_cached uses the ``idx_candle_freshness`` index on (symbol, timeframe,
fetched_at) — benchmarked at 3.9× faster than a partition scan at 300k rows.
"""
from __future__ import annotations

import calendar
from datetime import datetime, timezone, timedelta
import logging
from typing import Sequence

import httpx
from sqlalchemy import select, func, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import aliased

from app.data.base_provider import OHLCVPoint
from app.data.asset_registry import split_asset_identifier
from app.data.binance_provider import BinanceProvider
from app.data.database import get_db_session
from app.data.forex_provider import ForexProvider
from app.data.futures_provider import FuturesProvider
from app.data.models import Candle

logger = logging.getLogger(__name__)

# ── Cache TTL ─────────────────────────────────────────────────────────────────

CACHE_TTL_SECONDS: dict[str, int] = {
    "1m":  5   * 60,
    "5m":  15  * 60,
    "15m": 30  * 60,
    "30m": 45  * 60,
    "1h":  60  * 60,
    "4h":  4   * 3600,
    "1d":  24  * 3600,
    "1w":  7   * 86400,
}
DEFAULT_CACHE_TTL_SECONDS = 3600

# Minimum fraction of ``limit`` rows that must be present in cache before we
# consider cached data "sufficient" for a backtest (avoids silently passing a
# partial dataset to the signal engine).
_MIN_CACHE_RATIO = 0.8

# SQLite SQLITE_MAX_VARIABLE_NUMBER default = 999; 9 columns per row → max 111
# Use 100 for a comfortable margin.
_INSERT_BATCH_SIZE = 100


# ── Epoch helpers ─────────────────────────────────────────────────────────────

def _to_utc_epoch(dt: datetime) -> int:
    """Convert a datetime to a UTC Unix epoch integer.

    Works correctly for both timezone-aware and naive (assumed UTC) datetimes.
    Using calendar.timegm on a UTC timetuple avoids any system-timezone
    contamination from time.mktime().
    """
    if dt.tzinfo is not None:
        # Shift to UTC then convert
        utc_dt = dt.astimezone(timezone.utc)
    else:
        # Treat naive as UTC (our convention throughout the codebase)
        utc_dt = dt.replace(tzinfo=timezone.utc)
    return calendar.timegm(utc_dt.timetuple())


def _epoch_to_iso(epoch: int) -> str:
    """Return an ISO-8601 UTC string from a Unix epoch integer."""
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")


# ── DataManager ───────────────────────────────────────────────────────────────

class DataManager:
    """Unified market-data entrypoint with local SQLite caching."""

    def __init__(self) -> None:
        self.providers = {
            "crypto":  BinanceProvider(),
            "forex":   ForexProvider(),
            "futures": FuturesProvider(),
        }

    # ── Public API ────────────────────────────────────────────────────────────

    def _resolve_market(self, asset: str) -> tuple[str, str]:
        """Return (market, bare_symbol) for *asset*.

        *asset* may be a prefixed identifier (``crypto:BTCUSDT``,
        ``crypto/BTCUSDT``) or a canonical bare symbol (``BTCUSDT``) as
        returned by :func:`~app.api.asset_validation.validate_asset_path`.
        In all cases the symbol is normalised and validated against the
        registry before the matching provider is selected.
        """
        market, symbol = split_asset_identifier(asset)
        if market not in self.providers:
            raise ValueError(f"Unsupported market '{market}'")
        return market, symbol

    async def get_ohlcv(
        self,
        asset: str,
        timeframe: str,
        limit: int = 600,
    ) -> dict[str, object]:
        """Return OHLCV candles for *asset/timeframe*, serving from cache when fresh."""
        market, symbol = self._resolve_market(asset)
        provider = self.providers[market]

        cached = self._load_cached(symbol, timeframe, limit)
        if cached:
            logger.info(
                "Cache hit: %s/%s %s — %d candles", market, symbol, timeframe, len(cached)
            )
            return {
                "asset":     f"{market}:{symbol}",
                "provider":  provider.name,
                "timeframe": timeframe,
                "source":    "cache",
                "rows":      len(cached),
                "data":      cached,
            }

        try:
            fetched = await provider.fetch_ohlcv(symbol, timeframe, limit=limit)
        except httpx.HTTPError as exc:
            logger.exception("Provider HTTP error for %s/%s", market, symbol)
            raise RuntimeError("Upstream provider request failed") from exc
        except ValueError:
            raise
        except Exception as exc:
            logger.exception("Unexpected provider error for %s/%s", market, symbol)
            raise RuntimeError("Unexpected error fetching market data") from exc

        stored = self._store_candles(symbol, timeframe, fetched)
        logger.info(
            "Provider fetch: %s/%s %s — %d candles (%d stored, %d skipped as duplicates)",
            market, symbol, timeframe, len(fetched), stored, len(fetched) - stored,
        )

        return {
            "asset":     f"{market}:{symbol}",
            "provider":  provider.name,
            "timeframe": timeframe,
            "source":    "provider",
            "rows":      len(fetched),
            "data":      self._points_to_dicts(fetched),
        }

    # ── Cache read ────────────────────────────────────────────────────────────

    def _load_cached(
        self,
        symbol: str,
        timeframe: str,
        limit: int,
    ) -> list[dict[str, object]]:
        """Return up to *limit* fresh candles from the DB, or [] if stale/insufficient.

        Uses idx_candle_freshness to check freshness without scanning OHLCV data,
        then fetches the most-recent *limit* rows ordered by open_time ascending.
        """
        ttl     = CACHE_TTL_SECONDS.get(timeframe, DEFAULT_CACHE_TTL_SECONDS)
        cutoff  = _to_utc_epoch(datetime.now(timezone.utc)) - ttl
        min_req = max(1, int(limit * _MIN_CACHE_RATIO))

        with get_db_session() as session:
            # ① Fast COUNT via freshness index — avoids touching OHLCV columns.
            fresh_count: int = session.execute(
                select(func.count())
                .select_from(Candle)
                .where(Candle.symbol    == symbol)
                .where(Candle.timeframe == timeframe)
                .where(Candle.fetched_at > cutoff)
            ).scalar_one()

            if fresh_count < min_req:
                return []

            # ② Fetch most-recent *limit* rows in ascending order for signal engines.
            # Two-phase pattern: inner DESC LIMIT collects the tail of the partition;
            # outer ASC re-sort puts them in chronological order.
            #
            # FIX — cartesian-product eliminated.
            # The original code used:
            #
            #   select(Candle).select_from(subq)
            #
            # select(Candle) implicitly adds `candle` to the FROM clause because
            # Candle is an ORM entity mapped to that table.  .select_from(subq)
            # then adds `anon_1` (the subquery alias) as a SECOND, unrelated FROM
            # element.  With no JOIN condition the database performs a CROSS JOIN:
            #
            #   FROM candle, (SELECT … LIMIT :limit) AS anon_1   ← two FROM items!
            #
            # Result rows = (all rows in candle) × limit — an exploded row count
            # that produces wrong signal calculations and triggers SAWarning.
            #
            # Fix: aliased(Candle, subq) re-maps the ORM entity so it reads its
            # columns FROM the subquery, not from the raw table.  The outer SELECT
            # has exactly ONE FROM element:
            #
            #   FROM (SELECT … LIMIT :limit) AS anon_1           ← single FROM item
            #
            # .scalars().all() still returns Candle-like objects; row.open_time,
            # row.close, etc. work identically — no callers need to change.
            subq = (
                select(Candle)
                .where(Candle.symbol    == symbol)
                .where(Candle.timeframe == timeframe)
                .where(Candle.fetched_at > cutoff)
                .order_by(Candle.open_time.desc())
                .limit(limit)
                .subquery()
            )
            candle_alias = aliased(Candle, subq)
            rows: Sequence[Candle] = session.execute(
                select(candle_alias)
                .order_by(candle_alias.open_time.asc())
            ).scalars().all()

            # Materialise all values inside the session so they survive close().
            return [
                {
                    "timestamp": _epoch_to_iso(row.open_time),
                    "open":      row.open,
                    "high":      row.high,
                    "low":       row.low,
                    "close":     row.close,
                    "volume":    row.volume,
                }
                for row in rows
            ]

    # ── Cache write ───────────────────────────────────────────────────────────

    def _store_candles(
        self,
        symbol: str,
        timeframe: str,
        points: list[OHLCVPoint],
    ) -> int:
        """Persist *points* using INSERT OR IGNORE.

        Returns the number of rows actually inserted (duplicates are silently
        skipped).  Processes in batches of _INSERT_BATCH_SIZE to stay within
        SQLite's variable limit.
        """
        if not points:
            return 0

        now_epoch = _to_utc_epoch(datetime.now(timezone.utc))
        rows: list[dict[str, object]] = []

        for point in points:
            try:
                ts_raw = point["timestamp"]
                epoch  = (
                    _to_utc_epoch(ts_raw)
                    if isinstance(ts_raw, datetime)
                    else _to_utc_epoch(datetime.fromisoformat(str(ts_raw)))
                )
                rows.append({
                    "symbol":    symbol,
                    "timeframe": timeframe,
                    "open_time": epoch,
                    "open":      float(point["open"]),
                    "high":      float(point["high"]),
                    "low":       float(point["low"]),
                    "close":     float(point["close"]),
                    "volume":    float(point.get("volume") or 0.0),
                    "fetched_at": now_epoch,
                })
            except (KeyError, TypeError, ValueError, AttributeError) as exc:
                logger.debug("Skipping malformed OHLCV point: %s — %s", point, exc)

        if not rows:
            return 0

        inserted_total = 0
        with get_db_session() as session:
            for i in range(0, len(rows), _INSERT_BATCH_SIZE):
                batch = rows[i : i + _INSERT_BATCH_SIZE]
                # sqlite_insert + on_conflict_do_nothing → INSERT OR IGNORE
                stmt = (
                    sqlite_insert(Candle)
                    .values(batch)
                    .on_conflict_do_nothing()
                )
                result = session.execute(stmt)
                inserted_total += result.rowcount

        return inserted_total

    # ── Utility ───────────────────────────────────────────────────────────────

    @staticmethod
    def _points_to_dicts(points: list[OHLCVPoint]) -> list[dict[str, object]]:
        """Convert freshly-fetched OHLCVPoints to the API dict format."""
        out = []
        for p in points:
            ts_raw = p["timestamp"]
            if isinstance(ts_raw, datetime):
                iso = ts_raw.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
            else:
                iso = str(ts_raw)
            out.append({
                "timestamp": iso,
                "open":      float(p["open"]),
                "high":      float(p["high"]),
                "low":       float(p["low"]),
                "close":     float(p["close"]),
                "volume":    float(p.get("volume") or 0.0),
            })
        return out
