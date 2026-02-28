"""Backfill API endpoints.

POST /api/backfill/run          — launch a backfill job (async background task)
GET  /api/backfill/status       — query progress for all symbols
GET  /api/backfill/status/{sym}/{tf} — progress for a specific symbol/timeframe
POST /api/backfill/reset/{sym}/{tf}  — reset a FAILED/PARTIAL job so it can re-run
"""
from __future__ import annotations

import asyncio
import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query
from sqlalchemy import select, delete

from app.api.auth import require_api_key
from app.data.backfill import (
    BackfillTarget,
    DEFAULT_TARGETS,
    get_backfill_status,
    run_backfill,
)
from app.data.database import get_db_session
from app.data.models import BackfillProgress, BackfillStatus

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/api/backfill",
    tags=["backfill"],
    dependencies=[Depends(require_api_key)],
)

# Guard against concurrent backfill runs
_backfill_running = False


async def _run_in_background(targets: list[BackfillTarget], skip_complete: bool) -> None:
    global _backfill_running
    _backfill_running = True
    try:
        results = await run_backfill(targets=targets, skip_complete=skip_complete)
        total   = sum(r.get("total_inserted", 0) for r in results.values() if isinstance(r, dict))
        logger.info(
            "[Backfill] Background job finished — %d symbols processed, %d total rows inserted",
            len(results), total,
        )
    except Exception:
        logger.exception("[Backfill] Background job crashed")
    finally:
        _backfill_running = False


@router.post("/run")
async def trigger_backfill(
    background_tasks: BackgroundTasks,
    force: bool = Query(
        default=False,
        description="If True, re-run even symbols already marked COMPLETE",
    ),
    symbols: list[str] = Query(
        default=[],
        description="Filter to specific symbols, e.g. BTCUSDT. Empty = all.",
    ),
    timeframes: list[str] = Query(
        default=[],
        description="Filter to specific timeframes, e.g. 1h. Empty = all.",
    ),
) -> dict:
    """Trigger a historical backfill as a background task.

    The endpoint returns immediately; use GET /api/backfill/status to track
    progress. Only one concurrent run is permitted.
    """
    global _backfill_running
    if _backfill_running:
        raise HTTPException(
            status_code=409,
            detail="A backfill is already running. Check /api/backfill/status.",
        )

    # Apply optional filters
    targets = DEFAULT_TARGETS
    if symbols:
        sym_upper = {s.upper() for s in symbols}
        targets = [t for t in targets if t.symbol in sym_upper]
    if timeframes:
        targets = [t for t in targets if t.timeframe in timeframes]

    if not targets:
        raise HTTPException(
            status_code=400,
            detail="No matching backfill targets found for the given filters.",
        )

    background_tasks.add_task(
        _run_in_background,
        targets=targets,
        skip_complete=(not force),
    )

    return {
        "message":  "Backfill started in background",
        "targets":  [f"{t.symbol}/{t.timeframe}" for t in targets],
        "force":    force,
        "status_url": "/api/backfill/status",
    }


@router.get("/status")
async def backfill_status() -> dict:
    """Return progress for all backfill jobs."""
    rows = await get_backfill_status()
    total_inserted = sum(r["total_inserted"] for r in rows)
    complete = sum(1 for r in rows if r["status"] == BackfillStatus.COMPLETE)
    return {
        "is_running":     _backfill_running,
        "total_symbols":  len(rows),
        "complete":       complete,
        "total_inserted": total_inserted,
        "jobs":           rows,
    }


@router.get("/status/{symbol}/{timeframe}")
async def backfill_status_single(
    symbol:    str = Path(description="Symbol, e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe, e.g. 1h"),
) -> dict:
    """Return progress for a single symbol/timeframe combination."""
    with get_db_session() as session:
        row = session.execute(
            select(BackfillProgress)
            .where(BackfillProgress.symbol    == symbol.upper())
            .where(BackfillProgress.timeframe == timeframe)
        ).scalar_one_or_none()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No backfill record for {symbol.upper()}/{timeframe}",
            )

        from app.data.backfill import _from_epoch
        return {
            "symbol":          row.symbol,
            "timeframe":       row.timeframe,
            "status":          row.status,
            "total_inserted":  row.total_inserted,
            "windows_fetched": row.windows_fetched,
            "error_count":     row.error_count,
            "earliest":        _from_epoch(row.earliest_open_time) if row.earliest_open_time else None,
            "latest":          _from_epoch(row.latest_open_time)   if row.latest_open_time   else None,
            "target_start":    _from_epoch(row.target_start_epoch) if row.target_start_epoch else None,
            "started_at":      _from_epoch(row.started_at)         if row.started_at         else None,
            "completed_at":    _from_epoch(row.completed_at)       if row.completed_at        else None,
            "last_error":      row.last_error,
        }


@router.post("/reset/{symbol}/{timeframe}")
async def reset_backfill(
    symbol:    str = Path(description="Symbol to reset, e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe to reset, e.g. 1h"),
    full_reset: bool = Query(
        default=False,
        description="If True, delete existing candle data and restart from scratch",
    ),
) -> dict:
    """Reset a FAILED or PARTIAL backfill so it can be re-run.

    By default only resets status → PENDING while preserving progress
    (the next run will resume from earliest_open_time).
    Use full_reset=true to wipe candle data and start fresh.
    """
    global _backfill_running
    if _backfill_running:
        raise HTTPException(
            status_code=409,
            detail="Cannot reset while a backfill is running.",
        )

    sym = symbol.upper()

    with get_db_session() as session:
        row = session.execute(
            select(BackfillProgress)
            .where(BackfillProgress.symbol    == sym)
            .where(BackfillProgress.timeframe == timeframe)
        ).scalar_one_or_none()

        if row is None:
            raise HTTPException(
                status_code=404,
                detail=f"No backfill record for {sym}/{timeframe}",
            )

        if full_reset:
            # Delete candle data for this symbol/timeframe
            from app.data.models import Candle
            deleted = session.execute(
                delete(Candle)
                .where(Candle.symbol    == sym)
                .where(Candle.timeframe == timeframe)
            ).rowcount
            logger.info(
                "[Backfill] Full reset: deleted %d candles for %s/%s",
                deleted, sym, timeframe,
            )
            row.earliest_open_time = None
            row.latest_open_time   = None
            row.total_inserted     = 0
            row.windows_fetched    = 0
            row.error_count        = 0
            row.started_at         = None
            row.completed_at       = None
            row.last_error         = None

        row.status = BackfillStatus.PENDING

    return {
        "message":    f"Reset {sym}/{timeframe} to PENDING",
        "full_reset": full_reset,
    }
