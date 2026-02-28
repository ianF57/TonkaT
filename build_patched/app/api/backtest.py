from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.api.asset_validation import validate_asset_path
from app.api.auth import require_api_key
from app.api.tiering import UserContext, enforce_tier, get_user_context
from app.backtesting.service import BacktestService

router = APIRouter(prefix="/api/backtest", tags=["backtest"], dependencies=[Depends(require_api_key)])
service = BacktestService()


class BacktestSubmitRequest(BaseModel):
    symbol: str
    timeframe: str = "1d"
    start_date: datetime
    end_date: datetime
    strategy_name: str = "trend_v1"
    strategy_params: dict[str, Any] = Field(default_factory=dict)
    initial_capital: float = 10000.0
    risk_config: dict[str, Any] = Field(default_factory=dict)
    optimization: bool = False
    walk_forward: bool = False
    optimization_mode: str = "grid"
    param_grid: dict[str, list[Any]] = Field(default_factory=dict)
    rank_metric: str = "sharpe_ratio"
    train_size: int = 252
    test_size: int = 63


@router.post("")
async def submit_backtest(payload: BacktestSubmitRequest, user: UserContext = Depends(get_user_context)) -> dict[str, Any]:
    limits = enforce_tier(user, payload.start_date, payload.end_date, payload.optimization, payload.walk_forward)
    req = {
        "user_id": user.user_id,
        "config": payload.model_dump(mode="json"),
        "optimization": payload.optimization,
        "walk_forward": payload.walk_forward,
        "optimization_mode": payload.optimization_mode,
        "param_grid": payload.param_grid,
        "rank_metric": payload.rank_metric,
        "train_size": payload.train_size,
        "test_size": payload.test_size,
        "max_combinations": limits["max_combinations"],
    }
    job_id = service.submit_job(user.user_id, req)
    return {"job_id": job_id, "status": "pending"}


@router.get("/{job_id:int}")
async def get_backtest_job(job_id: int) -> dict[str, Any]:
    try:
        return service.get_job(job_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Job not found") from None


@router.get("/{asset}")
async def get_backtest_legacy(
    asset: str,
    timeframe: str = Query(default="1d"),
    strategy_name: str = Query(default="trend_v1"),
    user: UserContext = Depends(get_user_context),
) -> dict[str, Any]:
    normalized = validate_asset_path(asset)
    now = datetime.now(timezone.utc)
    req = BacktestSubmitRequest(
        symbol=normalized,
        timeframe=timeframe,
        strategy_name=strategy_name,
        start_date=now.replace(year=now.year - 1),
        end_date=now,
    )
    limits = enforce_tier(user, req.start_date, req.end_date, False, False)
    payload = {
        "user_id": user.user_id,
        "config": req.model_dump(mode="json"),
        "optimization": False,
        "walk_forward": False,
        "param_grid": {},
        "rank_metric": "sharpe_ratio",
        "max_combinations": limits["max_combinations"],
    }
    job_id = service.submit_job(user.user_id, payload)
    return service.get_job(job_id)
