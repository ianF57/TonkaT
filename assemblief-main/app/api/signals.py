from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from app.signals.signal_manager import SignalManager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["signals"])
signal_manager = SignalManager()


@router.get("/signals/{asset}")
async def get_signals(asset: str, timeframe: str = Query(default="1h")) -> dict[str, object]:
    """Generate and rank strategy signal candidates for an asset/timeframe."""
    try:
        return await signal_manager.generate_signals(asset=asset, timeframe=timeframe)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected signals endpoint error for asset=%s timeframe=%s", asset, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
