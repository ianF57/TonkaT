from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(frozen=True)
class BacktestConfig:
    symbol: str
    timeframe: str
    start_date: datetime
    end_date: datetime
    strategy_name: str
    strategy_params: dict[str, Any]
    initial_capital: float
    risk_config: dict[str, Any]


@dataclass(frozen=True)
class BacktestResult:
    metrics: dict[str, float]
    trades: list[dict[str, Any]]
    equity_curve: list[float]
    drawdown_curve: list[float]
    metadata: dict[str, Any]
