from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.asset_validation import validate_asset_path
from app.api.auth import require_api_key
from app.backtesting.replay import HistoricalReplay
from app.data.historical_store import InsufficientDataError

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["replay"], dependencies=[Depends(require_api_key)])
replay_engine = HistoricalReplay()


@router.get("/replay/{asset}")
async def replay_asset(
    asset: str,
    date: str = Query(..., description="Replay date YYYY-MM-DD"),
    timeframe: str = Query(default="1h"),
) -> dict[str, object]:
    """Replay historical context at a chosen date (DB-only).

    Returns 422 with a clear message if the DB lacks data for the date range.
    """
    try:
        normalized_asset = validate_asset_path(asset)
        return await replay_engine.replay(
            asset=normalized_asset, timeframe=timeframe, replay_date=date
        )
    except InsufficientDataError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected replay error: asset=%s date=%s tf=%s", asset, date, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
