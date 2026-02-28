from __future__ import annotations


def bounded_score(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def score_trending(adx_value: float, hurst: float, rsi_value: float) -> float:
    score = (adx_value * 1.5) + (max(0.0, hurst - 0.5) * 100.0) + (abs(rsi_value - 50.0) * 0.7)
    return bounded_score(score)


def score_ranging(adx_value: float, hurst: float, rsi_value: float) -> float:
    score = ((30.0 - adx_value) * 2.2) + (max(0.0, 0.55 - hurst) * 100.0) + (20.0 - abs(rsi_value - 50.0))
    return bounded_score(score)


def score_volatility(volatility: float, clustering: float, high: bool) -> float:
    if high:
        score = (volatility * 180.0) + (max(0.0, clustering) * 35.0)
    else:
        score = (30.0 - volatility * 180.0) + (max(0.0, 0.2 - clustering) * 80.0)
    return bounded_score(score)


def score_breakout(adx_value: float, rsi_value: float, volatility: float) -> float:
    rsi_boost = max(0.0, abs(rsi_value - 50.0) - 12.0) * 2.2
    score = (adx_value * 1.2) + rsi_boost + (volatility * 120.0)
    return bounded_score(score)


def score_mean_reversion(rsi_value: float, adx_value: float, hurst: float) -> float:
    extreme_rsi = max(0.0, abs(rsi_value - 50.0) - 18.0) * 2.4
    score = extreme_rsi + max(0.0, 30.0 - adx_value) * 1.6 + max(0.0, 0.55 - hurst) * 90.0
    return bounded_score(score)
