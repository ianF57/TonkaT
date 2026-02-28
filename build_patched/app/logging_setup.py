from __future__ import annotations

import logging


def configure_logging(log_level: str) -> None:
    """Configure root logger with a consistent format."""
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
