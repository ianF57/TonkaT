from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.asset_validation import validate_asset_path
from app.api.auth import require_api_key

from app.signals.signal_manager import SignalManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["signals"], dependencies=[Depends(require_api_key)])
signal_manager = SignalManager()


@router.get("/signals/{asset}")
async def get_signals(asset: str, timeframe: str = Query(default="1h")) -> dict[str, object]:
    """Generate and rank strategy signal candidates for an asset/timeframe."""
    try:
        normalized_asset = validate_asset_path(asset)
        return await signal_manager.generate_signals(asset=normalized_asset, timeframe=timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected signals endpoint error for asset=%s timeframe=%s", asset, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
