from __future__ import annotations

import random
from statistics import mean


def monte_carlo_stability(trades: list[float], simulations: int = 200) -> float:
    if len(trades) < 5:
        return 0.0
    totals: list[float] = []
    for _ in range(simulations):
        sample = [random.choice(trades) for _ in range(len(trades))]
        totals.append(sum(sample))
    baseline = sum(trades)
    wins = len([t for t in totals if t >= baseline * 0.6])
    return round((wins / simulations) * 100, 2)


def parameter_sensitivity(scores: list[float]) -> float:
    if len(scores) < 2:
        return 0.0
    avg = mean(scores)
    if avg == 0:
        return 0.0
    dispersion = mean([abs(s - avg) for s in scores])
    stability = max(0.0, 100.0 - (dispersion / abs(avg)) * 100.0)
    return round(stability, 2)


def evaluate_robustness(
    out_of_sample_cagr: float,
    out_of_sample_sharpe: float,
    monte_carlo_score: float,
    sensitivity_score: float,
) -> dict[str, float | bool]:
    passed = (
        out_of_sample_cagr > 0
        and out_of_sample_sharpe > 0.25
        and monte_carlo_score >= 55.0
        and sensitivity_score >= 45.0
    )
    robustness_score = round(
        max(0.0, min(100.0, (out_of_sample_cagr * 0.2) + (out_of_sample_sharpe * 15.0) + (monte_carlo_score * 0.35) + (sensitivity_score * 0.3))),
        2,
    )
    return {
        "passed": passed,
        "robustness_score": robustness_score,
        "monte_carlo_score": monte_carlo_score,
        "sensitivity_score": sensitivity_score,
    }
