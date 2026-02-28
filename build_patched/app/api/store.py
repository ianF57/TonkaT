"""Data store API — query metadata about the local historical candle database.

Endpoints
─────────
GET /api/store/stats                 — partition stats for all (symbol, timeframe) pairs
GET /api/store/stats/{symbol}/{tf}   — stats for a single partition
GET /api/store/partitions            — list all stored (symbol, timeframe) pairs
GET /api/store/candles/{symbol}/{tf} — paginated candle retrieval (DB-only)
GET /api/store/ready/{symbol}/{tf}   — quick readiness check (has_sufficient_data)
"""
from __future__ import annotations

import calendar
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Path, Query

from app.api.auth import require_api_key
from app.data.historical_store import (
    HistoricalDataStore,
    InsufficientDataError,
    MINIMUM_CANDLES,
)

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/store",
    tags=["store"],
    dependencies=[Depends(require_api_key)],
)

_store = HistoricalDataStore()


@router.get("/partitions")
def list_partitions() -> dict:
    """List every (symbol, timeframe) pair that has stored candles."""
    partitions = _store.list_partitions()
    return {
        "count":      len(partitions),
        "partitions": partitions,
    }


@router.get("/stats")
def all_stats() -> dict:
    """Return metadata for every stored partition.

    Includes candle_count, oldest/newest timestamps, and coverage_days.
    Does NOT load OHLCV data — reads only key columns.
    """
    partitions = _store.list_partitions()
    stats = []
    for p in partitions:
        try:
            s = _store.get_stats(p["symbol"], p["timeframe"])
            s["ready"] = s["candle_count"] >= MINIMUM_CANDLES
            stats.append(s)
        except Exception as exc:
            logger.warning("Stats failed for %s/%s: %s", p["symbol"], p["timeframe"], exc)
    return {"count": len(stats), "partitions": stats}


@router.get("/stats/{symbol}/{timeframe}")
def partition_stats(
    symbol:    str = Path(description="Symbol e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe e.g. 1h"),
) -> dict:
    """Metadata for a single (symbol, timeframe) partition."""
    s = _store.get_stats(symbol.upper(), timeframe)
    s["ready"] = s["candle_count"] >= MINIMUM_CANDLES
    return s


@router.get("/ready/{symbol}/{timeframe}")
def partition_ready(
    symbol:    str = Path(description="Symbol e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe e.g. 1h"),
    min_candles: int = Query(default=MINIMUM_CANDLES, ge=1),
) -> dict:
    """Return whether the partition has enough data for backtest/signals.

    This is the fast pre-flight check: O(1) early-exit COUNT.
    """
    ready = _store.has_sufficient_data(symbol.upper(), timeframe, min_candles)
    return {
        "symbol":      symbol.upper(),
        "timeframe":   timeframe,
        "ready":       ready,
        "min_candles": min_candles,
        "hint":        (
            None if ready
            else "Run POST /api/backfill/run to populate historical data."
        ),
    }


@router.get("/candles/{symbol}/{timeframe}")
def get_candles(
    symbol:    str = Path(description="Symbol e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe e.g. 1h"),
    limit:     int   = Query(default=500, ge=1,  le=50_000, description="Number of most-recent candles"),
    start:     str | None = Query(default=None, description="ISO-8601 start timestamp (inclusive)"),
    end:       str | None = Query(default=None, description="ISO-8601 end timestamp (inclusive)"),
) -> dict:
    """Retrieve stored candles from the database.

    Without start/end: returns the most-recent *limit* candles.
    With start/end: returns the date-bounded range (limit is ignored).

    This endpoint NEVER calls external APIs.
    """
    sym = symbol.upper()
    try:
        if start is not None or end is not None:
            # Range query
            def _parse(s: str) -> int:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return calendar.timegm(dt.astimezone(timezone.utc).timetuple())

            start_ep = _parse(start) if start else 0
            end_ep   = _parse(end)   if end   else calendar.timegm(datetime.now(timezone.utc).timetuple())

            candles = _store.get_candles_range(sym, timeframe, start_ep, end_ep, min_candles=1)
            return {
                "symbol":    sym,
                "timeframe": timeframe,
                "source":    "database",
                "mode":      "range",
                "rows":      len(candles),
                "data":      candles,
            }
        else:
            candles = _store.get_candles(sym, timeframe, limit=limit)
            return {
                "symbol":    sym,
                "timeframe": timeframe,
                "source":    "database",
                "mode":      "latest",
                "rows":      len(candles),
                "data":      candles,
            }
    except InsufficientDataError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Store candles error: %s/%s", sym, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
