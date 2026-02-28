"""Incremental update API endpoints.

POST /api/incremental/run                       — trigger an incremental update
GET  /api/incremental/status                    — summary across all targets
GET  /api/incremental/gaps/{symbol}/{timeframe} — gap report for one pair
GET  /api/incremental/log                       — recent update history
GET  /api/incremental/log/{symbol}/{timeframe}  — history for one pair
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query

from app.api.auth import require_api_key
from app.data.backfill import DEFAULT_TARGETS, BackfillTarget
from app.data.incremental import (
    _get_latest_stored_epoch,
    _get_backfill_status,
    _from_epoch,
    detect_gaps,
    get_incremental_log,
    run_incremental_update,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/incremental",
    tags=["incremental"],
    dependencies=[Depends(require_api_key)],
)

# Concurrency guard — one incremental run at a time
_incremental_running = False


async def _run_in_background(
    targets:   list[BackfillTarget],
    fill_gaps: bool,
) -> None:
    global _incremental_running
    _incremental_running = True
    try:
        results = await run_incremental_update(targets=targets, fill_gaps=fill_gaps)
        total_new = sum(
            r.get("candles_inserted", 0)
            for r in results.values()
            if isinstance(r, dict)
        )
        logger.info(
            "[Incremental] Background run finished — %d targets, %d new candles",
            len(results), total_new,
        )
    except Exception:
        logger.exception("[Incremental] Background run crashed")
    finally:
        _incremental_running = False


@router.post("/run")
async def trigger_incremental_update(
    background_tasks: BackgroundTasks,
    symbols:    list[str] = Query(default=[], description="Filter to specific symbols, e.g. BTCUSDT"),
    timeframes: list[str] = Query(default=[], description="Filter to specific timeframes, e.g. 1h"),
    fill_gaps:  bool      = Query(default=True, description="Auto-fill detected gaps after forward update"),
) -> dict:
    """Trigger an incremental update as a background task.

    Fetches only candles newer than the current MAX(open_time) for each
    target.  Returns immediately; poll GET /api/incremental/status to track.

    Only one concurrent run is permitted.
    """
    global _incremental_running
    if _incremental_running:
        raise HTTPException(
            status_code=409,
            detail="An incremental update is already running.",
        )

    targets = list(DEFAULT_TARGETS)
    if symbols:
        sym_upper = {s.upper() for s in symbols}
        targets = [t for t in targets if t.symbol in sym_upper]
    if timeframes:
        targets = [t for t in targets if t.timeframe in timeframes]

    if not targets:
        raise HTTPException(
            status_code=400,
            detail="No matching targets found for the given filters.",
        )

    background_tasks.add_task(_run_in_background, targets=targets, fill_gaps=fill_gaps)

    return {
        "message":    "Incremental update started in background",
        "targets":    [f"{t.symbol}/{t.timeframe}" for t in targets],
        "fill_gaps":  fill_gaps,
        "status_url": "/api/incremental/status",
        "log_url":    "/api/incremental/log",
    }


@router.get("/status")
async def incremental_status() -> dict:
    """Return the current data freshness for all default targets.

    Shows the latest stored candle timestamp and whether a backfill exists,
    so callers can see at a glance which pairs need updating.
    """
    rows = []
    for target in DEFAULT_TARGETS:
        latest = _get_latest_stored_epoch(target.symbol, target.timeframe)
        bf_status = _get_backfill_status(target.symbol, target.timeframe)
        rows.append({
            "symbol":         target.symbol,
            "timeframe":      target.timeframe,
            "market":         target.market,
            "backfill_status": bf_status,
            "latest_candle":  _from_epoch(latest) if latest else None,
            "has_data":       latest is not None,
        })

    return {
        "is_running": _incremental_running,
        "targets":    rows,
    }


@router.get("/gaps/{symbol}/{timeframe}")
async def gap_report(
    symbol:    str = Path(description="Symbol, e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe, e.g. 1h"),
) -> dict:
    """Return all detected gaps for a single (symbol, timeframe) pair.

    A gap is any interval where consecutive stored open_times differ by more
    than tf_secs, meaning one or more expected candles are absent.

    Gaps may represent exchange outages, delistings, or missed fetches.
    The incremental engine auto-fills gaps ≤ 500 candles for Binance symbols.
    """
    sym = symbol.upper()
    latest = _get_latest_stored_epoch(sym, timeframe)
    if latest is None:
        raise HTTPException(
            status_code=404,
            detail=f"No candle data found for {sym}/{timeframe}. Run backfill first.",
        )

    gaps = detect_gaps(sym, timeframe)
    total_missing = sum(g["missing_candles"] for g in gaps)

    return {
        "symbol":         sym,
        "timeframe":      timeframe,
        "latest_candle":  _from_epoch(latest),
        "gap_count":      len(gaps),
        "total_missing_candles": total_missing,
        "gaps":           gaps,
    }


@router.get("/log")
async def update_log(
    limit: int = Query(default=50, ge=1, le=500, description="Max entries to return"),
) -> dict:
    """Return recent incremental update log entries across all symbols."""
    entries = await get_incremental_log(limit=limit)
    return {
        "count":   len(entries),
        "entries": entries,
    }


@router.get("/log/{symbol}/{timeframe}")
async def update_log_for_pair(
    symbol:    str = Path(description="Symbol, e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe, e.g. 1h"),
    limit:     int = Query(default=20, ge=1, le=200),
) -> dict:
    """Return incremental update history for a specific (symbol, timeframe) pair."""
    entries = await get_incremental_log(
        symbol=symbol.upper(),
        timeframe=timeframe,
        limit=limit,
    )
    return {
        "symbol":    symbol.upper(),
        "timeframe": timeframe,
        "count":     len(entries),
        "entries":   entries,
    }
