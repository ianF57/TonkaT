from __future__ import annotations


def clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return max(low, min(high, value))


def overfitting_penalty(in_sample_cagr: float, out_sample_cagr: float) -> float:
    if in_sample_cagr <= 0:
        return 0.0
    gap = max(0.0, in_sample_cagr - out_sample_cagr)
    return clamp(gap * 0.6)


def sensitivity_penalty(sensitivity_score: float) -> float:
    return clamp((100.0 - sensitivity_score) * 0.4)


def regime_fit_penalty(regime_alignment: float) -> float:
    return clamp((100.0 - regime_alignment) * 0.35)


def confidence_score(
    out_sample_performance: float,
    cross_asset_stability: float,
    cross_time_stability: float,
    regime_alignment: float,
    parameter_robustness: float,
    drawdown_control: float,
    in_sample_cagr: float,
    out_sample_cagr: float,
) -> float:
    base = (
        out_sample_performance * 0.24
        + cross_asset_stability * 0.16
        + cross_time_stability * 0.14
        + regime_alignment * 0.16
        + parameter_robustness * 0.16
        + drawdown_control * 0.14
    )

    penalty = (
        overfitting_penalty(in_sample_cagr, out_sample_cagr)
        + sensitivity_penalty(parameter_robustness)
        + regime_fit_penalty(regime_alignment)
    )

    return round(clamp(base - penalty), 2)
