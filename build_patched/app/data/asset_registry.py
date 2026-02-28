from __future__ import annotations

VALID_MARKET_PREFIXES: frozenset[str] = frozenset({"crypto", "forex", "futures"})

VALID_ASSETS_BY_MARKET: dict[str, frozenset[str]] = {
    "crypto": frozenset({
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT",
        "DOGEUSDT", "AVAXUSDT", "MATICUSDT", "LINKUSDT",
    }),
    "forex": frozenset({"EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF"}),
    "futures": frozenset({"GC", "SI", "CL", "NG", "ES", "NQ", "YM"}),
}


def canonical_symbol(asset: str) -> str:
    """Return the bare canonical symbol for *asset*, stripping any provider prefix.

    Examples
    --------
    >>> canonical_symbol("crypto:BTCUSDT")  -> "BTCUSDT"
    >>> canonical_symbol("crypto/BTCUSDT")  -> "BTCUSDT"
    >>> canonical_symbol("BTCUSDT")         -> "BTCUSDT"
    >>> canonical_symbol("forex:EURUSD")    -> "EURUSD"
    """
    _market, sym = split_asset_identifier(asset)
    return sym


def split_asset_identifier(asset: str) -> tuple[str, str]:
    """Normalize asset identifier into (market, symbol) and validate against allowlist.

    Accepts the following formats:
    - ``market:SYMBOL``   e.g. ``crypto:BTCUSDT``
    - ``market/SYMBOL``   e.g. ``crypto/BTCUSDT``  (slash separator)
    - ``SYMBOL``          bare symbol; market is inferred from suffix or registry lookup
    """
    raw_asset = asset.strip()
    if not raw_asset:
        raise ValueError("Asset cannot be empty")

    # Normalise slash separator to colon so the rest of the logic is uniform.
    if "/" in raw_asset and ":" not in raw_asset:
        raw_asset = raw_asset.replace("/", ":", 1)

    if ":" in raw_asset:
        market, symbol = raw_asset.split(":", 1)
        market = market.lower()
        symbol = symbol.upper()
    else:
        symbol = raw_asset.upper()
        if symbol.endswith("USDT"):
            market = "crypto"
        elif symbol.endswith("=F"):
            market = "futures"
            symbol = symbol[:-2]
        elif symbol.endswith("=X"):
            market = "forex"
            symbol = symbol[:-2]
        else:
            # Last resort: reverse-lookup the symbol in the registry so bare
            # forex/futures symbols (e.g. "EURUSD", "GC") can be resolved
            # without requiring the caller to supply a market prefix.
            market = _market_for_bare_symbol(symbol)
            if market is None:
                raise ValueError(
                    "Asset must include market prefix (crypto:, forex:, futures:) or a known symbol suffix"
                )

    if market not in VALID_MARKET_PREFIXES:
        raise ValueError(
            f"Unsupported market prefix '{market}'. Valid prefixes: {', '.join(sorted(VALID_MARKET_PREFIXES))}"
        )

    allowed_symbols = VALID_ASSETS_BY_MARKET.get(market, frozenset())
    if symbol not in allowed_symbols:
        raise ValueError(
            f"Unsupported symbol '{symbol}' for market '{market}'."
            f" Allowed symbols: {', '.join(sorted(allowed_symbols))}"
        )

    return market, symbol


def _market_for_bare_symbol(symbol: str) -> str | None:
    """Return the market name for a bare symbol via registry lookup, or None."""
    for market, symbols in VALID_ASSETS_BY_MARKET.items():
        if symbol in symbols:
            return market
    return None
