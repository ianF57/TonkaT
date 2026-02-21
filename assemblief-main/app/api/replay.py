from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from app.backtesting.replay import HistoricalReplay

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["replay"])
replay_engine = HistoricalReplay()


@router.get("/replay/{asset}")
async def replay_asset(asset: str, date: str = Query(..., description="Replay date YYYY-MM-DD"), timeframe: str = Query(default="1h")) -> dict[str, object]:
    """Replay historical context at a selected date."""
    try:
        return await replay_engine.replay(asset=asset, timeframe=timeframe, replay_date=date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected replay endpoint error for asset=%s date=%s timeframe=%s", asset, date, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
