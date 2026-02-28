from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.asset_validation import validate_asset_path
from app.api.auth import require_api_key

from app.data.data_manager import DataManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["data"], dependencies=[Depends(require_api_key)])
manager = DataManager()


@router.get("/data/{asset}")
async def get_market_data(asset: str, timeframe: str = Query(default="1h")) -> dict[str, object]:
    """Return unified OHLCV market data across supported asset classes."""
    try:
        normalized_asset = validate_asset_path(asset)
        return await manager.get_ohlcv(asset=normalized_asset, timeframe=timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected data endpoint error for asset=%s timeframe=%s", asset, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
