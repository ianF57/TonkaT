from __future__ import annotations

from app.data.asset_registry import canonical_symbol


def validate_asset_path(asset: str) -> str:
    """Validate an asset identifier and return the canonical bare symbol.

    Strips any provider prefix (``crypto:``, ``crypto/``, ``forex:``, etc.)
    so that downstream DB queries always receive the bare symbol stored in the
    ``candle`` table (e.g. ``"BTCUSDT"`` not ``"crypto:BTCUSDT"``).

    Raises ValueError for unknown or unsupported assets.

    Examples
    --------
    >>> validate_asset_path("crypto:BTCUSDT")  -> "BTCUSDT"
    >>> validate_asset_path("crypto/BTCUSDT")  -> "BTCUSDT"
    >>> validate_asset_path("BTCUSDT")         -> "BTCUSDT"
    >>> validate_asset_path("forex:EURUSD")    -> "EURUSD"
    """
    return canonical_symbol(asset)
