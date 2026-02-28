from __future__ import annotations

from dataclasses import replace
from statistics import mean, pstdev
from typing import Any

from app.backtesting.optimization import Optimizer
from app.backtesting.types import BacktestConfig


class WalkForwardEngine:
    def __init__(self, optimizer: Optimizer) -> None:
        self.optimizer = optimizer

    def run(
        self,
        base_config: BacktestConfig,
        candles: list[dict[str, Any]],
        param_grid: dict[str, list[Any]],
        train_size: int,
        test_size: int,
        metric: str = "sharpe_ratio",
        max_combinations: int = 100,
    ) -> dict[str, Any]:
        windows: list[dict[str, Any]] = []
        combined_equity: list[float] = [base_config.initial_capital]
        scores: list[float] = []

        start = 0
        while start + train_size + test_size <= len(candles):
            train = candles[start : start + train_size]
            test = candles[start + train_size : start + train_size + test_size]

            ranked = self.optimizer.grid_search(
                base_config=base_config,
                candles=train,
                param_grid=param_grid,
                rank_metric=metric,
                max_combinations=max_combinations,
            )
            best = ranked[0]
            test_config = replace(base_config, strategy_params=best["params"])
            result = self.optimizer.backtester.run(test_config, test)

            scores.append(result.metrics.get(metric, 0.0))
            offset = combined_equity[-1] - result.equity_curve[0]
            combined_equity.extend([x + offset for x in result.equity_curve[1:]])
            windows.append({"start": start, "best_params": best["params"], "metrics": result.metrics})
            start += test_size

        return {
            "windows": windows,
            "combined_equity_curve": [round(x, 6) for x in combined_equity],
            "stability_metrics": {
                "mean_score": round(mean(scores), 6) if scores else 0.0,
                "score_std_dev": round(pstdev(scores), 6) if len(scores) > 1 else 0.0,
                "window_count": len(windows),
            },
        }
