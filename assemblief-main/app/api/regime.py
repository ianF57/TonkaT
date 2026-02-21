from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from app.data.data_manager import DataManager
from app.regime.regime_classifier import RegimeClassifier

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api", tags=["regime"])
manager = DataManager()
classifier = RegimeClassifier()


@router.get("/regime/{asset}")
async def get_regime(asset: str, timeframe: str = Query(default="1h")) -> dict[str, object]:
    """Detect current market regime with confidence and historical distribution."""
    try:
        ohlcv_response = await manager.get_ohlcv(asset=asset, timeframe=timeframe)
        snapshot = classifier.classify(ohlcv_response["data"])
        return {
            "asset": ohlcv_response["asset"],
            "timeframe": timeframe,
            "current_regime": snapshot.current_regime,
            "confidence_score": snapshot.confidence_score,
            "historical_distribution": snapshot.historical_distribution,
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    except Exception as exc:
        logger.exception("Unexpected regime endpoint error for asset=%s timeframe=%s", asset, timeframe)
        raise HTTPException(status_code=500, detail="Internal server error") from exc
