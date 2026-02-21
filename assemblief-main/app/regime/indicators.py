from __future__ import annotations

from dataclasses import dataclass
from math import log, sqrt
from statistics import mean, pstdev


@dataclass(frozen=True)
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float


def _returns(closes: list[float]) -> list[float]:
    if len(closes) < 2:
        return []
    return [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1] != 0]


def rolling_volatility(closes: list[float], window: int = 20) -> float:
    """Compute annualized rolling volatility proxy from returns."""
    rets = _returns(closes)
    if len(rets) < 2:
        return 0.0
    segment = rets[-window:]
    sigma = pstdev(segment) if len(segment) > 1 else 0.0
    return sigma * sqrt(252)


def rsi(closes: list[float], period: int = 14) -> float:
    if len(closes) <= period:
        return 50.0
    gains: list[float] = []
    losses: list[float] = []
    for idx in range(1, len(closes)):
        delta = closes[idx] - closes[idx - 1]
        gains.append(max(delta, 0.0))
        losses.append(abs(min(delta, 0.0)))
    avg_gain = mean(gains[-period:]) if gains[-period:] else 0.0
    avg_loss = mean(losses[-period:]) if losses[-period:] else 0.0
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100.0 - (100.0 / (1.0 + rs))


def adx(candles: list[Candle], period: int = 14) -> float:
    if len(candles) <= period + 1:
        return 10.0

    trs: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []

    for i in range(1, len(candles)):
        current = candles[i]
        prev = candles[i - 1]
        tr = max(current.high - current.low, abs(current.high - prev.close), abs(current.low - prev.close))
        up_move = current.high - prev.high
        down_move = prev.low - current.low
        pdm = up_move if up_move > down_move and up_move > 0 else 0.0
        mdm = down_move if down_move > up_move and down_move > 0 else 0.0
        trs.append(tr)
        plus_dm.append(pdm)
        minus_dm.append(mdm)

    tr_n = sum(trs[-period:])
    if tr_n == 0:
        return 10.0

    plus_di = 100.0 * (sum(plus_dm[-period:]) / tr_n)
    minus_di = 100.0 * (sum(minus_dm[-period:]) / tr_n)
    if plus_di + minus_di == 0:
        return 10.0

    dx = 100.0 * abs(plus_di - minus_di) / (plus_di + minus_di)
    return dx


def volatility_clustering(closes: list[float], window: int = 30) -> float:
    """Absolute-return lag-1 autocorrelation proxy for volatility clustering."""
    rets = [abs(x) for x in _returns(closes)][-window:]
    if len(rets) < 3:
        return 0.0
    left = rets[:-1]
    right = rets[1:]
    left_mean = mean(left)
    right_mean = mean(right)
    numerator = sum((l - left_mean) * (r - right_mean) for l, r in zip(left, right, strict=False))
    left_var = sum((l - left_mean) ** 2 for l in left)
    right_var = sum((r - right_mean) ** 2 for r in right)
    denominator = (left_var * right_var) ** 0.5
    if denominator == 0:
        return 0.0
    return numerator / denominator


def hurst_exponent(closes: list[float], max_lag: int = 20) -> float:
    """Estimate the Hurst exponent (optional signal)."""
    if len(closes) < max_lag + 2:
        return 0.5

    lags = range(2, max_lag)
    tau: list[float] = []
    for lag in lags:
        diffs = [closes[i + lag] - closes[i] for i in range(len(closes) - lag)]
        if not diffs:
            continue
        tau.append(pstdev(diffs) if len(diffs) > 1 else 0.0)

    filtered = [t for t in tau if t > 0]
    if len(filtered) < 2:
        return 0.5

    log_lags = [log(float(lag)) for lag in lags][: len(filtered)]
    log_tau = [log(float(val)) for val in filtered]
    x_mean = mean(log_lags)
    y_mean = mean(log_tau)
    numerator = sum((x - x_mean) * (y - y_mean) for x, y in zip(log_lags, log_tau, strict=False))
    denominator = sum((x - x_mean) ** 2 for x in log_lags)
    if denominator == 0:
        return 0.5
    slope = numerator / denominator
    return max(0.0, min(1.0, slope * 2))
