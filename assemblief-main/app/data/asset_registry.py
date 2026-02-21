from __future__ import annotations

VALID_MARKET_PREFIXES: frozenset[str] = frozenset({"crypto", "forex", "futures"})

VALID_ASSETS_BY_MARKET: dict[str, frozenset[str]] = {
    "crypto": frozenset({"BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "MATICUSDT", "LINKUSDT"}),
    "forex": frozenset({"EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD", "USDCHF"}),
    "futures": frozenset({"GC", "SI", "CL", "NG", "ES", "NQ", "YM"}),
}


def split_asset_identifier(asset: str) -> tuple[str, str]:
    """Normalize asset identifier into (market, symbol) and validate against allowlist."""
    raw_asset = asset.strip()
    if not raw_asset:
        raise ValueError("Asset cannot be empty")

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
