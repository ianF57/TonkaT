"""Data integrity API endpoints.

POST /api/integrity/run
    Trigger a validate-and-repair cycle as a background task.
    Optional: filter by symbol/timeframe, choose scan_mode, disable auto_repair.

GET  /api/integrity/status
    Running flag + latest health summary per (symbol, timeframe).

GET  /api/integrity/history
    Full CandleIntegrityReport history (all pairs, paginated).

GET  /api/integrity/history/{symbol}/{timeframe}
    Report history for one pair.

GET  /api/integrity/anomalies/{symbol}/{timeframe}
    Live OHLCV anomaly scan — returns the actual bad candles (not a report).

GET  /api/integrity/gaps/{symbol}/{timeframe}
    Live gap scan (full mode) — returns the actual gap records.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Path, Query
from sqlalchemy import desc, select

from app.api.auth import require_api_key
from app.data.backfill import DEFAULT_TARGETS, BackfillTarget
from app.data.database import get_db_session
from app.data.integrity import (
    IntegrityResult,
    _detect_gaps_full,
    _scan_ohlcv_anomalies,
    get_integrity_history,
    run_integrity_check,
)
from app.data.backfill import _from_epoch
from app.data.models import CandleIntegrityReport

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/integrity",
    tags=["integrity"],
    dependencies=[Depends(require_api_key)],
)

# Concurrency guard
_integrity_running: bool = False


# ── Background task wrapper ───────────────────────────────────────────────────

async def _run_in_background(
    targets:     list[BackfillTarget],
    scan_mode:   str,
    auto_repair: bool,
) -> None:
    global _integrity_running
    _integrity_running = True
    try:
        results = await run_integrity_check(
            targets     = targets,
            scan_mode   = scan_mode,
            auto_repair = auto_repair,
        )
        issues = sum(
            (r.get("gaps_found", 0) + r.get("ohlcv_anomalies_found", 0)
             + r.get("partial_candles_found", 0))
            for r in results.values() if isinstance(r, dict)
        )
        logger.info(
            "[Integrity] Background job complete — %d pairs checked, %d total issues found",
            len(results), issues,
        )
    except Exception:
        logger.exception("[Integrity] Background job crashed")
    finally:
        _integrity_running = False


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/run")
async def trigger_integrity_check(
    background_tasks: BackgroundTasks,
    symbols:     list[str] = Query(default=[], description="Filter to specific symbols. Empty = all."),
    timeframes:  list[str] = Query(default=[], description="Filter to specific timeframes. Empty = all."),
    scan_mode:   str       = Query(default="incremental", description="'incremental' or 'full'"),
    auto_repair: bool      = Query(default=True,  description="Automatically repair detected issues"),
) -> dict:
    """Trigger a data integrity check (and optional repair) as a background task.

    Returns immediately — poll GET /api/integrity/status to track progress.
    """
    global _integrity_running
    if _integrity_running:
        raise HTTPException(
            status_code=409,
            detail="An integrity check is already running. Check /api/integrity/status.",
        )

    if scan_mode not in ("incremental", "full"):
        raise HTTPException(
            status_code=400,
            detail=f"Invalid scan_mode '{scan_mode}'. Use 'incremental' or 'full'.",
        )

    targets = DEFAULT_TARGETS
    if symbols:
        sym_upper = {s.upper() for s in symbols}
        targets   = [t for t in targets if t.symbol in sym_upper]
    if timeframes:
        targets   = [t for t in targets if t.timeframe in timeframes]

    if not targets:
        raise HTTPException(
            status_code=400,
            detail="No matching targets found for the given symbol/timeframe filters.",
        )

    background_tasks.add_task(
        _run_in_background,
        targets     = targets,
        scan_mode   = scan_mode,
        auto_repair = auto_repair,
    )

    return {
        "message":    f"Integrity check ({scan_mode}) started in background",
        "targets":    [f"{t.symbol}/{t.timeframe}" for t in targets],
        "scan_mode":  scan_mode,
        "auto_repair": auto_repair,
        "status_url": "/api/integrity/status",
    }


@router.get("/status")
async def integrity_status() -> dict:
    """Return the running flag and the latest report per (symbol, timeframe)."""
    with get_db_session() as session:
        # Latest report per pair using a subquery for the MAX(run_at)
        from sqlalchemy import func
        subq = (
            select(
                CandleIntegrityReport.symbol,
                CandleIntegrityReport.timeframe,
                func.max(CandleIntegrityReport.run_at).label("latest_run"),
            )
            .group_by(CandleIntegrityReport.symbol, CandleIntegrityReport.timeframe)
            .subquery()
        )
        rows = session.execute(
            select(CandleIntegrityReport)
            .join(
                subq,
                (CandleIntegrityReport.symbol    == subq.c.symbol)
                & (CandleIntegrityReport.timeframe == subq.c.timeframe)
                & (CandleIntegrityReport.run_at    == subq.c.latest_run),
            )
            .order_by(CandleIntegrityReport.symbol, CandleIntegrityReport.timeframe)
        ).scalars().all()

        summaries = [
            {
                "symbol":                r.symbol,
                "timeframe":             r.timeframe,
                "health":                r.health,
                "scan_mode":             r.scan_mode,
                "last_checked":          _from_epoch(r.run_at),
                "gaps_found":            r.gaps_found,
                "gaps_total_missing":    r.gaps_total_missing,
                "ohlcv_anomalies_found": r.ohlcv_anomalies_found,
                "partial_candles_found": r.partial_candles_found,
                "dataset_stale":         bool(r.dataset_stale),
                "stale_by_periods":      r.stale_by_periods,
                "gaps_repaired":         r.gaps_repaired,
                "ohlcv_anomalies_repaired": r.ohlcv_anomalies_repaired,
                "partial_candles_removed":  r.partial_candles_removed,
                "duration_ms":           r.duration_ms,
            }
            for r in rows
        ]

    total_pairs  = len(summaries)
    clean_pairs  = sum(1 for s in summaries if s["health"] == "clean")
    issue_pairs  = sum(1 for s in summaries if s["health"] in ("issues_remain", "error"))
    total_gaps   = sum(s["gaps_total_missing"] for s in summaries)
    total_anomalies = sum(s["ohlcv_anomalies_found"] for s in summaries)

    return {
        "is_running":       _integrity_running,
        "total_pairs":      total_pairs,
        "clean_pairs":      clean_pairs,
        "pairs_with_issues": issue_pairs,
        "total_missing_candles": total_gaps,
        "total_ohlcv_anomalies": total_anomalies,
        "pairs": summaries,
    }


@router.get("/history")
async def integrity_history(
    symbol:    str | None = Query(default=None, description="Filter by symbol"),
    timeframe: str | None = Query(default=None, description="Filter by timeframe"),
    limit:     int        = Query(default=50, ge=1, le=500),
) -> dict:
    """Return recent integrity report history across all pairs."""
    rows = await get_integrity_history(symbol=symbol, timeframe=timeframe, limit=limit)
    return {"count": len(rows), "reports": rows}


@router.get("/history/{symbol}/{timeframe}")
async def integrity_history_pair(
    symbol:    str = Path(description="Symbol, e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe, e.g. 1h"),
    limit:     int = Query(default=20, ge=1, le=200),
) -> dict:
    """Return integrity report history for a specific symbol/timeframe pair."""
    rows = await get_integrity_history(
        symbol    = symbol.upper(),
        timeframe = timeframe,
        limit     = limit,
    )
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No integrity reports found for {symbol.upper()}/{timeframe}",
        )
    return {"symbol": symbol.upper(), "timeframe": timeframe, "count": len(rows), "reports": rows}


@router.get("/anomalies/{symbol}/{timeframe}")
async def live_anomaly_scan(
    symbol:    str = Path(description="Symbol, e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe, e.g. 1h"),
) -> dict:
    """Run a live OHLCV anomaly scan and return the anomalous candles.

    This endpoint does NOT repair — it is for inspection only.
    To repair, POST /api/integrity/run with auto_repair=true.
    """
    sym      = symbol.upper()
    anomalies = _scan_ohlcv_anomalies(sym, timeframe)
    return {
        "symbol":    sym,
        "timeframe": timeframe,
        "count":     len(anomalies),
        "anomalies": list(anomalies),
    }


@router.get("/gaps/{symbol}/{timeframe}")
async def live_gap_scan(
    symbol:    str = Path(description="Symbol, e.g. BTCUSDT"),
    timeframe: str = Path(description="Timeframe, e.g. 1h"),
) -> dict:
    """Run a full live gap scan and return the gap records.

    This endpoint does NOT repair — it is for inspection only.
    To repair, POST /api/integrity/run with auto_repair=true.

    Note: performs a full-partition LAG scan.  For 1m datasets this may take
    up to ~450 ms.
    """
    sym = symbol.upper()
    gaps, min_ep, max_ep = _detect_gaps_full(sym, timeframe)

    total_missing = sum(g["missing_candles"] for g in gaps)

    return {
        "symbol":              sym,
        "timeframe":           timeframe,
        "scanned_from":        _from_epoch(min_ep) if min_ep else None,
        "scanned_to":          _from_epoch(max_ep) if max_ep else None,
        "gaps_found":          len(gaps),
        "total_missing_candles": total_missing,
        "gaps":                list(gaps),
    }
