from __future__ import annotations

import itertools
import random
from dataclasses import replace
from typing import Any

from app.backtesting.engine import Backtester
from app.backtesting.types import BacktestConfig


class Optimizer:
    def __init__(self, backtester: Backtester) -> None:
        self.backtester = backtester

    def grid_search(
        self,
        base_config: BacktestConfig,
        candles: list[dict[str, Any]],
        param_grid: dict[str, list[Any]],
        rank_metric: str = "sharpe_ratio",
        max_combinations: int = 100,
    ) -> list[dict[str, Any]]:
        keys = sorted(param_grid.keys())
        combinations = [dict(zip(keys, vals)) for vals in itertools.product(*[param_grid[k] for k in keys])]
        return self._run_candidates(base_config, candles, combinations[:max_combinations], rank_metric)

    def random_search(
        self,
        base_config: BacktestConfig,
        candles: list[dict[str, Any]],
        param_grid: dict[str, list[Any]],
        rank_metric: str = "sharpe_ratio",
        samples: int = 25,
    ) -> list[dict[str, Any]]:
        keys = sorted(param_grid.keys())
        sampled = []
        for _ in range(samples):
            sampled.append({k: random.choice(param_grid[k]) for k in keys})
        return self._run_candidates(base_config, candles, sampled, rank_metric)

    def _run_candidates(
        self,
        base_config: BacktestConfig,
        candles: list[dict[str, Any]],
        candidates: list[dict[str, Any]],
        rank_metric: str,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for params in candidates:
            config = replace(base_config, strategy_params=params)
            result = self.backtester.run(config, candles)
            rows.append({"params": params, "metrics": result.metrics, "score": result.metrics.get(rank_metric, 0.0)})
        return sorted(rows, key=lambda row: row["score"], reverse=True)
