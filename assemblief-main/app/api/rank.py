from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from app.scoring.ranker import SignalRanker

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["ranking"])
ranker = SignalRanker()


@router.get("/rank/{asset}")
async def get_signal_rank(asset: str, timeframe: str = Query(default="1h")) -> dict[str, object]:
    """Return top 3 ranked signals and confidence analytics."""
    try:
        return await ranker.rank_asset(asset=asset, timeframe=timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected rank endpoint error for asset=%s timeframe=%s", asset, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
