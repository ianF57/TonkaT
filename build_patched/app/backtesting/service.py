from __future__ import annotations

import hashlib
import json
import time
from calendar import timegm
from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from app.backtesting.engine import Backtester
from app.backtesting.optimization import Optimizer
from app.backtesting.types import BacktestConfig
from app.backtesting.walk_forward import WalkForwardEngine
from app.data.database import get_db_session
from app.data.historical_store import HistoricalDataStore
from app.data.models import BacktestJob, BacktestResultCache


class BacktestService:
    def __init__(self) -> None:
        self.store = HistoricalDataStore()
        self.backtester = Backtester()
        self.optimizer = Optimizer(self.backtester)
        self.walk_forward = WalkForwardEngine(self.optimizer)

    def submit_job(self, user_id: str, payload: dict[str, Any]) -> int:
        now = int(time.time())
        with get_db_session() as session:
            job = BacktestJob(
                user_id=user_id,
                status="pending",
                request_json=json.dumps(payload, default=str),
                created_at=now,
                updated_at=now,
            )
            session.add(job)
            session.flush()
            job_id = int(job.id)
        self._execute_job(job_id)
        return job_id

    def get_job(self, job_id: int) -> dict[str, Any]:
        with get_db_session() as session:
            job = session.get(BacktestJob, job_id)
            if not job:
                raise ValueError("job_not_found")
            return {
                "job_id": job.id,
                "status": job.status,
                "response": json.loads(job.response_json) if job.response_json else None,
                "error": job.error,
                "created_at": job.created_at,
                "updated_at": job.updated_at,
            }

    def _execute_job(self, job_id: int) -> None:
        with get_db_session() as session:
            job = session.get(BacktestJob, job_id)
            if not job:
                return
            job.status = "running"
            job.updated_at = int(time.time())
            payload = json.loads(job.request_json)

        try:
            response = self._run_backtest(payload)
            with get_db_session() as session:
                job = session.get(BacktestJob, job_id)
                if job:
                    job.status = "completed"
                    job.response_json = json.dumps(response)
                    job.updated_at = int(time.time())
        except Exception as exc:
            with get_db_session() as session:
                job = session.get(BacktestJob, job_id)
                if job:
                    job.status = "failed"
                    job.error = str(exc)
                    job.updated_at = int(time.time())

    def _run_backtest(self, payload: dict[str, Any]) -> dict[str, Any]:
        cfg = payload["config"]
        config = BacktestConfig(
            symbol=cfg["symbol"],
            timeframe=cfg["timeframe"],
            start_date=datetime.fromisoformat(cfg["start_date"]),
            end_date=datetime.fromisoformat(cfg["end_date"]),
            strategy_name=cfg["strategy_name"],
            strategy_params=cfg.get("strategy_params", {}),
            initial_capital=float(cfg.get("initial_capital", 10000)),
            risk_config=cfg.get("risk_config", {}),
        )

        candles = self.store.get_candles_range(
            symbol=config.symbol,
            timeframe=config.timeframe,
            start_epoch=timegm(config.start_date.utctimetuple()),
            end_epoch=timegm(config.end_date.utctimetuple()),
            min_candles=60,
        )

        config_hash = self._hash_config(payload["user_id"], config)
        cached = self._get_cache(config_hash)
        if cached:
            return {"cached": True, **cached}

        started = time.perf_counter()
        result = self.backtester.run(config, candles)
        body: dict[str, Any] = {"result": asdict(result)}

        if payload.get("optimization"):
            body["optimization"] = self.optimizer.grid_search(
                base_config=config,
                candles=candles,
                param_grid=payload.get("param_grid", {}),
                rank_metric=payload.get("rank_metric", "sharpe_ratio"),
                max_combinations=int(payload.get("max_combinations", 100)),
            )
        if payload.get("walk_forward"):
            body["walk_forward"] = self.walk_forward.run(
                base_config=config,
                candles=candles,
                param_grid=payload.get("param_grid", {}),
                train_size=int(payload.get("train_size", 252)),
                test_size=int(payload.get("test_size", 63)),
                metric=payload.get("rank_metric", "sharpe_ratio"),
                max_combinations=int(payload.get("max_combinations", 100)),
            )

        duration = time.perf_counter() - started
        self._save_cache(payload["user_id"], config_hash, body, duration)
        body["cached"] = False
        body["execution_time"] = round(duration, 4)
        return body

    @staticmethod
    def _hash_config(user_id: str, config: BacktestConfig) -> str:
        payload = {
            "user_id": user_id,
            "symbol": config.symbol,
            "timeframe": config.timeframe,
            "start": config.start_date.isoformat(),
            "end": config.end_date.isoformat(),
            "strategy": config.strategy_name,
            "params": config.strategy_params,
            "capital": config.initial_capital,
            "risk": config.risk_config,
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()

    def _get_cache(self, config_hash: str) -> dict[str, Any] | None:
        with get_db_session() as session:
            row = session.query(BacktestResultCache).filter(BacktestResultCache.config_hash == config_hash).first()
            if not row:
                return None
            return {
                "result": {
                    "metrics": json.loads(row.metrics_json),
                    "trades": json.loads(row.trades_json),
                    "equity_curve": json.loads(row.equity_curve_json),
                },
                "execution_time": row.execution_time,
            }

    def _save_cache(self, user_id: str, config_hash: str, body: dict[str, Any], execution_time: float) -> None:
        result = body["result"]
        now = int(time.time())
        with get_db_session() as session:
            existing = session.query(BacktestResultCache).filter(BacktestResultCache.config_hash == config_hash).first()
            if existing:
                return
            session.add(
                BacktestResultCache(
                    user_id=user_id,
                    config_hash=config_hash,
                    metrics_json=json.dumps(result["metrics"]),
                    trades_json=json.dumps(result["trades"]),
                    equity_curve_json=json.dumps(result["equity_curve"]),
                    execution_time=execution_time,
                    created_at=now,
                )
            )
