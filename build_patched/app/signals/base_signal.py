from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any


class BaseSignal(ABC):
    """Pure signal generator contract."""

    @abstractmethod
    def generate(self, history_df: list[dict[str, Any]]) -> dict[str, Any]:
        """Return {action, confidence, metadata}."""
