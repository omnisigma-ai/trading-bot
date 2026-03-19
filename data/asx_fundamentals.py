"""
ASX Fundamentals Fetcher
------------------------
Fetches fundamental data for ASX-listed stocks via yfinance.
Caches results in SQLite to avoid redundant API calls.
Used by the EV scorer for value stock selection.
"""
import time

import yfinance as yf

from logs.trade_logger import TradeLogger


# Metrics we extract from yfinance .info
_METRIC_MAP = {
    "trailing_pe": "trailingPE",
    "forward_pe": "forwardPE",
    "price_to_book": "priceToBook",
    "ev_to_ebitda": "enterpriseToEbitda",
    "ev_to_revenue": "enterpriseToRevenue",
    "roe": "returnOnEquity",
    "operating_margin": "operatingMargins",
    "gross_margin": "grossMargins",
    "debt_to_equity": "debtToEquity",
    "dividend_yield": "dividendYield",
    "market_cap": "marketCap",
}

# Raw fields needed for derived metrics
_RAW_FIELDS = ["freeCashflow", "marketCap", "ebitda", "totalDebt", "totalCash"]


def fetch_single_ticker(symbol: str) -> dict | None:
    """
    Fetch yfinance .info for an ASX ticker.
    Returns normalized metrics dict, or None on failure.
    """
    yf_symbol = f"{symbol}.AX"
    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info
        if not info or info.get("regularMarketPrice") is None:
            print(f"[Fundamentals] {yf_symbol}: no data available")
            return None

        # Extract standard metrics
        metrics = {}
        for our_key, yf_key in _METRIC_MAP.items():
            val = info.get(yf_key)
            if val is not None and val == val:  # not NaN
                metrics[our_key] = float(val)

        # Compute derived metrics
        derived = _compute_derived_metrics(info)
        metrics.update(derived)

        return metrics

    except Exception as e:
        print(f"[Fundamentals] {yf_symbol}: fetch failed — {e}")
        return None


def _compute_derived_metrics(info: dict) -> dict:
    """Compute FCF yield, ROIC, and interest coverage from raw fields."""
    derived = {}

    market_cap = info.get("marketCap")
    fcf = info.get("freeCashflow")
    ebitda = info.get("ebitda")
    total_debt = info.get("totalDebt", 0) or 0
    total_cash = info.get("totalCash", 0) or 0

    # FCF Yield = Free Cash Flow / Market Cap
    if fcf and market_cap and market_cap > 0:
        derived["fcf_yield"] = float(fcf) / float(market_cap)

    # ROIC = EBITDA × (1 - tax rate) / Invested Capital
    # Invested Capital = Total Debt + Market Cap - Cash
    if ebitda and market_cap:
        invested_capital = float(total_debt) + float(market_cap) - float(total_cash)
        if invested_capital > 0:
            nopat = float(ebitda) * 0.70  # ~30% AU corporate tax rate
            derived["roic"] = nopat / invested_capital

    # Interest Coverage = EBITDA / estimated interest expense
    # Assume ~5% average interest rate on total debt
    if ebitda and total_debt and total_debt > 0:
        est_interest = float(total_debt) * 0.05
        if est_interest > 0:
            derived["interest_coverage"] = float(ebitda) / est_interest

    return derived


def fetch_universe_fundamentals(
    symbols: list[str],
    logger: TradeLogger,
    max_age_hours: int = 24,
) -> list[dict]:
    """
    Fetch fundamentals for all symbols. Uses SQLite cache if fresh.
    Falls back to stale cache on network errors.

    Args:
        symbols: ASX ticker symbols (without .AX suffix)
        logger: TradeLogger for cache access
        max_age_hours: Cache validity in hours

    Returns:
        List of dicts, each with {symbol, trailing_pe, roe, ...}
    """
    # Check cache first
    cached = logger.get_cached_fundamentals(symbols, max_age_hours)
    stale_cache = {}
    if len(cached) < len(symbols):
        # Also fetch stale cache as fallback
        stale_cache = logger.get_cached_fundamentals(symbols, max_age_hours=8760)  # 1 year

    results = []
    need_fetch = [s for s in symbols if s not in cached]

    # Use cached data for symbols that are fresh
    for sym, metrics in cached.items():
        metrics["symbol"] = sym
        results.append(metrics)

    # Fetch missing symbols from yfinance
    fetch_failures = 0
    for sym in need_fetch:
        metrics = fetch_single_ticker(sym)

        if metrics is None:
            fetch_failures += 1
            # Fall back to stale cache
            if sym in stale_cache:
                print(f"[Fundamentals] {sym}: using stale cache")
                stale = stale_cache[sym]
                stale["symbol"] = sym
                results.append(stale)
            continue

        # Cache the fresh data
        try:
            logger.cache_fundamentals(sym, metrics)
        except Exception as e:
            print(f"[Fundamentals] {sym}: cache write failed — {e}")

        metrics["symbol"] = sym
        results.append(metrics)

        # Polite delay to avoid yfinance rate limits
        time.sleep(0.5)

    if fetch_failures > 0:
        print(f"[Fundamentals] {fetch_failures}/{len(need_fetch)} fetches failed")

    return results
