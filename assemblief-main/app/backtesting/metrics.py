from __future__ import annotations

from dataclasses import dataclass
from math import sqrt
from statistics import mean


@dataclass(frozen=True)
class BacktestMetrics:
    cagr: float
    sharpe: float
    sortino: float
    calmar: float
    max_drawdown: float
    profit_factor: float
    expectancy: float
    risk_of_ruin: float
    win_rate: float


def max_drawdown(equity_curve: list[float]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    worst = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            dd = (value - peak) / peak
            worst = min(worst, dd)
    return abs(worst)


def _returns(equity_curve: list[float]) -> list[float]:
    if len(equity_curve) < 2:
        return []
    returns: list[float] = []
    for i in range(1, len(equity_curve)):
        prev = equity_curve[i - 1]
        if prev == 0:
            continue
        returns.append((equity_curve[i] - prev) / prev)
    return returns


def calculate_metrics(equity_curve: list[float], trades: list[float], periods_per_year: int = 252) -> BacktestMetrics:
    if len(equity_curve) < 2:
        return BacktestMetrics(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 100.0, 0.0)

    rets = _returns(equity_curve)
    avg_ret = mean(rets) if rets else 0.0
    downside = [r for r in rets if r < 0]

    variance = mean([(r - avg_ret) ** 2 for r in rets]) if rets else 0.0
    std_dev = variance ** 0.5
    downside_dev = (mean([r**2 for r in downside]) ** 0.5) if downside else 0.0

    sharpe = (avg_ret / std_dev * sqrt(periods_per_year)) if std_dev > 0 else 0.0
    sortino = (avg_ret / downside_dev * sqrt(periods_per_year)) if downside_dev > 0 else 0.0

    years = max(1 / periods_per_year, len(rets) / periods_per_year)
    cagr = (equity_curve[-1] / equity_curve[0]) ** (1 / years) - 1 if equity_curve[0] > 0 else 0.0

    mdd = max_drawdown(equity_curve)
    calmar = (cagr / mdd) if mdd > 0 else 0.0

    gains = sum(t for t in trades if t > 0)
    losses = abs(sum(t for t in trades if t < 0))
    profit_factor = (gains / losses) if losses > 0 else (999.0 if gains > 0 else 0.0)

    win_count = len([t for t in trades if t > 0])
    loss_count = len([t for t in trades if t <= 0])
    trade_count = max(1, len(trades))
    win_rate = win_count / trade_count

    avg_win = (gains / win_count) if win_count else 0.0
    avg_loss = (losses / max(1, loss_count)) if loss_count else 0.0
    expectancy = win_rate * avg_win - (1 - win_rate) * avg_loss

    ruin_base = max(0.0, min(1.0, 1 - win_rate))
    risk_of_ruin = min(100.0, ruin_base ** max(1, trade_count // 5) * 100)

    return BacktestMetrics(
        cagr=round(cagr * 100, 4),
        sharpe=round(sharpe, 4),
        sortino=round(sortino, 4),
        calmar=round(calmar, 4),
        max_drawdown=round(mdd * 100, 4),
        profit_factor=round(profit_factor, 4),
        expectancy=round(expectancy, 6),
        risk_of_ruin=round(risk_of_ruin, 4),
        win_rate=round(win_rate * 100, 4),
    )
