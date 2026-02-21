from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException, Query
from app.api.auth import require_api_key

from app.backtesting.backtester import Backtester

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["backtest"], dependencies=[Depends(require_api_key)])
backtester = Backtester()


@router.get("/backtest/{asset}")
async def get_backtest(asset: str, signal: str = Query(default="trend_v1"), timeframe: str = Query(default="1h")) -> dict[str, object]:
    """Run backtest and robustness checks for a selected signal."""
    try:
        return await backtester.run(asset=asset, timeframe=timeframe, signal_name=signal)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected backtest endpoint error for asset=%s signal=%s timeframe=%s", asset, signal, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
