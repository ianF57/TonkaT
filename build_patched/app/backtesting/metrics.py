from __future__ import annotations

from math import sqrt
from statistics import mean


def _max_drawdown(equity_curve: list[float]) -> float:
    peak = equity_curve[0] if equity_curve else 0.0
    max_dd = 0.0
    for value in equity_curve:
        peak = max(peak, value)
        if peak > 0:
            max_dd = min(max_dd, (value - peak) / peak)
    return abs(max_dd)


def _returns(equity_curve: list[float]) -> list[float]:
    returns: list[float] = []
    for idx in range(1, len(equity_curve)):
        prev = equity_curve[idx - 1]
        if prev > 0:
            returns.append((equity_curve[idx] - prev) / prev)
    return returns


def calculate_metrics(
    equity_curve: list[float],
    trades: list[dict[str, float]],
    periods_per_year: int,
    bars_in_market: int,
) -> dict[str, float]:
    if len(equity_curve) < 2:
        return {
            "total_return": 0.0,
            "cagr": 0.0,
            "sharpe_ratio": 0.0,
            "max_drawdown": 0.0,
            "calmar_ratio": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "expectancy": 0.0,
            "exposure_pct": 0.0,
            "time_in_market": 0.0,
        }

    rets = _returns(equity_curve)
    avg_ret = mean(rets) if rets else 0.0
    std = (mean([(r - avg_ret) ** 2 for r in rets]) ** 0.5) if rets else 0.0
    sharpe = (avg_ret / std * sqrt(periods_per_year)) if std else 0.0

    total_return = (equity_curve[-1] / equity_curve[0]) - 1
    years = max(len(rets) / periods_per_year, 1 / periods_per_year)
    cagr = (equity_curve[-1] / equity_curve[0]) ** (1 / years) - 1 if equity_curve[0] else 0.0

    max_drawdown = _max_drawdown(equity_curve)
    calmar = cagr / max_drawdown if max_drawdown else 0.0

    pnls = [float(t["pnl"]) for t in trades]
    wins = [p for p in pnls if p > 0]
    losses = [abs(p) for p in pnls if p < 0]
    win_rate = (len(wins) / len(pnls)) if pnls else 0.0
    profit_factor = (sum(wins) / sum(losses)) if losses else (999.0 if wins else 0.0)
    avg_win = mean(wins) if wins else 0.0
    avg_loss = mean(losses) if losses else 0.0
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    total_bars = max(1, len(equity_curve) - 1)
    exposure_pct = bars_in_market / total_bars

    return {
        "total_return": round(total_return * 100, 4),
        "cagr": round(cagr * 100, 4),
        "sharpe_ratio": round(sharpe, 4),
        "max_drawdown": round(max_drawdown * 100, 4),
        "calmar_ratio": round(calmar, 4),
        "win_rate": round(win_rate * 100, 4),
        "profit_factor": round(profit_factor, 4),
        "avg_win": round(avg_win, 6),
        "avg_loss": round(avg_loss, 6),
        "expectancy": round(expectancy, 6),
        "exposure_pct": round(exposure_pct * 100, 4),
        "time_in_market": round(exposure_pct, 6),
    }
