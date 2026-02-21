from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SignalCandidate:
    strategy_label: str
    version: str
    direction: str
    entry_rule: str
    exit_rule: str
    stop_loss: float
    risk_reward: float
    compatible_regimes: list[str]
    compatible_timeframes: list[str]
    parameters: dict[str, Any]
    performance_score: float
    metadata: dict[str, Any]


class BaseSignal(ABC):
    """Base contract for versioned signal strategies."""

    strategy_label: str
    version: str
    compatible_regimes: list[str]
    compatible_timeframes: list[str]

    @abstractmethod
    def generate(
        self,
        asset: str,
        timeframe: str,
        ohlcv: list[dict[str, Any]],
        regime: str,
    ) -> SignalCandidate | None:
        """Generate a signal candidate if strategy conditions are met."""
