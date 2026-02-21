from __future__ import annotations

from app.data.asset_registry import split_asset_identifier


def validate_asset_path(asset: str) -> str:
    """Validate and normalize a path asset identifier before downstream usage."""
    market, symbol = split_asset_identifier(asset)
    return f"{market}:{symbol}"
