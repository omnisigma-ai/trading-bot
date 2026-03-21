"""
ASX Extended Fundamentals Fetcher
----------------------------------
Fetches enriched yfinance data for snowflake scoring: .financials,
.balance_sheet, .cashflow, .dividends, plus .info extras.

Builds on the existing asx_fundamentals.py pattern — SQLite cache,
stale fallback, polite rate-limiting.
"""
import json
import time
from datetime import datetime

import yfinance as yf

from data.asx_fundamentals import fetch_single_ticker, _compute_derived_metrics
from logs.trade_logger import TradeLogger


def fetch_extended_single_ticker(symbol: str) -> dict | None:
    """
    Fetch standard + extended yfinance data for an ASX ticker.

    Returns a dict compatible with the existing ev_scorer (all original
    keys present) plus extra fields needed for snowflake checks:
      - EPS history, ROCE, ROA, balance sheet items, dividend history,
        growth estimates, peg_ratio, payout_ratio, beta, etc.

    Returns None on total failure.
    """
    yf_symbol = f"{symbol}.AX"
    try:
        ticker = yf.Ticker(yf_symbol)
        info = ticker.info or {}
        if not info or info.get("regularMarketPrice") is None:
            print(f"[ExtFundamentals] {yf_symbol}: no data available")
            return None

        # ── Start with standard metrics (same as asx_fundamentals.py) ────
        from data.asx_fundamentals import _METRIC_MAP
        metrics = {}
        for our_key, yf_key in _METRIC_MAP.items():
            val = info.get(yf_key)
            if val is not None and val == val:
                metrics[our_key] = float(val)

        derived = _compute_derived_metrics(info)
        metrics.update(derived)

        # ── Extra .info fields ───────────────────────────────────────────
        for our_key, yf_key in [
            ("peg_ratio", "pegRatio"),
            ("current_ratio", "currentRatio"),
            ("roa", "returnOnAssets"),
            ("payout_ratio", "payoutRatio"),
            ("beta", "beta"),
            ("total_assets", "totalAssets"),
            ("current_price", "regularMarketPrice"),
            ("shares_outstanding", "sharesOutstanding"),
            ("earnings_growth_ttm", "earningsGrowth"),
            ("revenue_growth_ttm", "revenueGrowth"),
        ]:
            val = info.get(yf_key)
            if val is not None and val == val:
                metrics[our_key] = float(val)

        stockholders_equity = info.get("bookValue")
        shares = info.get("sharesOutstanding")
        if stockholders_equity is not None and shares is not None:
            metrics["stockholders_equity"] = float(stockholders_equity) * float(shares)

        total_debt = info.get("totalDebt")
        if total_debt is not None:
            metrics["total_debt"] = float(total_debt)

        total_cash = info.get("totalCash")
        if total_cash is not None:
            metrics["total_cash"] = float(total_cash)

        operating_cf = info.get("operatingCashflow")
        if operating_cf is not None:
            metrics["operating_cashflow"] = float(operating_cf)

        fcf = info.get("freeCashflow")
        if fcf is not None:
            metrics["free_cashflow"] = float(fcf)

        # ── Historical financials ────────────────────────────────────────
        _extract_financials(ticker, metrics)
        _extract_balance_sheet(ticker, metrics)
        _extract_cashflow(ticker, metrics)
        _extract_dividends(ticker, metrics)
        _extract_growth_estimates(ticker, metrics)

        return metrics

    except Exception as e:
        print(f"[ExtFundamentals] {yf_symbol}: fetch failed — {e}")
        return None


def _safe_float(val) -> float | None:
    """Convert a value to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        f = float(val)
        return f if f == f else None  # NaN check
    except (ValueError, TypeError):
        return None


def _extract_financials(ticker, metrics: dict) -> None:
    """Extract EPS, EBIT, revenue, interest expense from income statement."""
    try:
        fin = ticker.financials
        if fin is None or fin.empty:
            return

        dates = sorted(fin.columns, reverse=True)  # most recent first

        # EPS from Basic EPS row
        eps_row = None
        for label in ["Basic EPS", "Diluted EPS"]:
            if label in fin.index:
                eps_row = fin.loc[label]
                break

        if eps_row is not None:
            eps_vals = [_safe_float(eps_row.get(d)) for d in dates]
            eps_clean = [(i, v) for i, v in enumerate(eps_vals) if v is not None]

            if eps_clean:
                metrics["eps_current"] = eps_clean[0][1]
                if len(eps_clean) >= 2:
                    metrics["eps_1y_ago"] = eps_clean[1][1]
                    # 1-year EPS growth
                    if eps_clean[1][1] and abs(eps_clean[1][1]) > 0.001:
                        metrics["eps_growth_1y"] = (eps_clean[0][1] - eps_clean[1][1]) / abs(eps_clean[1][1])

                # 5-year ago (index 4 if available, else furthest back)
                if len(eps_clean) >= 5:
                    metrics["eps_5y_ago"] = eps_clean[4][1]
                elif len(eps_clean) >= 3:
                    metrics["eps_5y_ago"] = eps_clean[-1][1]

                # 5-year average annual growth
                if len(eps_clean) >= 3:
                    first_eps = eps_clean[-1][1]
                    last_eps = eps_clean[0][1]
                    n_years = eps_clean[-1][0] - eps_clean[0][0]
                    if n_years == 0:
                        n_years = len(eps_clean) - 1
                    if first_eps and abs(first_eps) > 0.001 and n_years > 0:
                        metrics["eps_growth_5y_avg"] = (last_eps - first_eps) / (abs(first_eps) * n_years)

        # EBIT
        if "EBIT" in fin.index and len(dates) >= 1:
            metrics["ebit"] = _safe_float(fin.loc["EBIT", dates[0]])

        # Interest Expense
        for label in ["Interest Expense", "Net Interest Income"]:
            if label in fin.index and len(dates) >= 1:
                val = _safe_float(fin.loc[label, dates[0]])
                if val is not None:
                    metrics["interest_expense"] = abs(val)
                    break

        # ROCE: EBIT / (Total Assets - Current Liabilities) — need balance sheet
        # We'll compute this after balance_sheet extraction

    except Exception as e:
        print(f"[ExtFundamentals] financials extraction error: {e}")


def _extract_balance_sheet(ticker, metrics: dict) -> None:
    """Extract balance sheet items + compute ROCE and historical D/E."""
    try:
        bs = ticker.balance_sheet
        if bs is None or bs.empty:
            return

        dates = sorted(bs.columns, reverse=True)

        # Current Assets
        for label in ["Current Assets", "Total Current Assets"]:
            if label in bs.index:
                metrics["current_assets"] = _safe_float(bs.loc[label, dates[0]])
                break

        # Current Liabilities
        for label in ["Current Liabilities", "Total Current Liabilities"]:
            if label in bs.index:
                metrics["current_liabilities"] = _safe_float(bs.loc[label, dates[0]])
                break

        # Long-term liabilities = Total Liabilities - Current Liabilities
        total_liab = None
        for label in ["Total Liabilities Net Minority Interest", "Total Liab"]:
            if label in bs.index:
                total_liab = _safe_float(bs.loc[label, dates[0]])
                break
        cur_liab = metrics.get("current_liabilities")
        if total_liab is not None and cur_liab is not None:
            metrics["long_term_liabilities"] = total_liab - cur_liab
        elif total_liab is not None:
            metrics["long_term_liabilities"] = total_liab

        # Total Assets (supplement .info if missing)
        if "Total Assets" in bs.index:
            ta = _safe_float(bs.loc["Total Assets", dates[0]])
            if ta is not None:
                metrics.setdefault("total_assets", ta)

        # Stockholders Equity from balance sheet (more reliable than .info)
        for label in ["Stockholders Equity", "Total Stockholders Equity",
                       "Stockholders' Equity", "Common Stock Equity"]:
            if label in bs.index:
                eq = _safe_float(bs.loc[label, dates[0]])
                if eq is not None:
                    metrics["stockholders_equity"] = eq
                    break

        # ROCE = EBIT / (Total Assets - Current Liabilities)
        ebit = metrics.get("ebit")
        ta = metrics.get("total_assets")
        cl = metrics.get("current_liabilities")
        if ebit is not None and ta is not None and cl is not None:
            capital_employed = ta - cl
            if capital_employed > 0:
                metrics["roce_current"] = ebit / capital_employed

        # ROCE 3 years ago
        if "EBIT" in ticker.financials.index and len(dates) >= 3:
            fin_dates = sorted(ticker.financials.columns, reverse=True)
            if len(fin_dates) >= 3:
                ebit_3y = _safe_float(ticker.financials.loc["EBIT", fin_dates[2]])
                ta_3y = _safe_float(bs.loc["Total Assets", dates[2]]) if "Total Assets" in bs.index and len(dates) >= 3 else None
                cl_3y = None
                for label in ["Current Liabilities", "Total Current Liabilities"]:
                    if label in bs.index and len(dates) >= 3:
                        cl_3y = _safe_float(bs.loc[label, dates[2]])
                        break
                if ebit_3y is not None and ta_3y is not None and cl_3y is not None:
                    ce_3y = ta_3y - cl_3y
                    if ce_3y > 0:
                        metrics["roce_3y_ago"] = ebit_3y / ce_3y

        # D/E 5 years ago (or furthest back available)
        # yfinance balance_sheet typically has 4 annual periods
        de_history = []
        for label_debt in ["Total Debt", "Long Term Debt"]:
            if label_debt in bs.index:
                for label_eq in ["Stockholders Equity", "Total Stockholders Equity",
                                  "Common Stock Equity"]:
                    if label_eq in bs.index:
                        for d in dates:
                            debt_val = _safe_float(bs.loc[label_debt, d])
                            eq_val = _safe_float(bs.loc[label_eq, d])
                            if debt_val is not None and eq_val is not None and eq_val > 0:
                                de_history.append(debt_val / eq_val * 100)  # as percentage
                        break
                break

        if len(de_history) >= 2:
            metrics["de_5y_ago"] = de_history[-1]  # oldest available

    except Exception as e:
        print(f"[ExtFundamentals] balance_sheet extraction error: {e}")


def _extract_cashflow(ticker, metrics: dict) -> None:
    """Extract operating/free cash flow history."""
    try:
        cf = ticker.cashflow
        if cf is None or cf.empty:
            return

        dates = sorted(cf.columns, reverse=True)

        # Operating Cash Flow (supplement .info)
        for label in ["Operating Cash Flow", "Total Cash From Operating Activities"]:
            if label in cf.index:
                val = _safe_float(cf.loc[label, dates[0]])
                if val is not None:
                    metrics.setdefault("operating_cashflow", val)
                break

        # Free Cash Flow history
        for label in ["Free Cash Flow"]:
            if label in cf.index:
                fcf_list = []
                for d in dates:
                    v = _safe_float(cf.loc[label, d])
                    if v is not None:
                        fcf_list.append(v)
                if fcf_list:
                    metrics["fcf_history"] = fcf_list
                    metrics.setdefault("free_cashflow", fcf_list[0])
                break

    except Exception as e:
        print(f"[ExtFundamentals] cashflow extraction error: {e}")


def _extract_dividends(ticker, metrics: dict) -> None:
    """Extract dividend history and compute stability checks."""
    try:
        divs = ticker.dividends
        if divs is None or divs.empty:
            metrics["dividend_10y_history"] = []
            return

        # Group by year and sum
        annual = {}
        for date, amount in divs.items():
            year = date.year
            annual[year] = annual.get(year, 0.0) + float(amount)

        current_year = datetime.utcnow().year
        # Last 10 years of data
        years = sorted([y for y in annual.keys() if y <= current_year and y >= current_year - 10])
        history = [annual[y] for y in years]
        metrics["dividend_10y_history"] = history

        if len(history) >= 2:
            # Check for any >10% drops
            has_drop = False
            for i in range(1, len(history)):
                if history[i - 1] > 0 and history[i] < history[i - 1] * 0.9:
                    has_drop = True
                    break
            metrics["dividend_has_10y_drop"] = has_drop

            # Is current dividend higher than 10 years ago?
            if len(history) >= 10:
                metrics["dividend_higher_than_10y_ago"] = history[-1] > history[0]
            elif len(history) >= 2:
                metrics["dividend_higher_than_10y_ago"] = history[-1] > history[0]
        else:
            metrics["dividend_has_10y_drop"] = None
            metrics["dividend_higher_than_10y_ago"] = None

    except Exception as e:
        print(f"[ExtFundamentals] dividend extraction error: {e}")


def _extract_growth_estimates(ticker, metrics: dict) -> None:
    """Extract analyst growth estimates if available."""
    try:
        # yfinance .growth_estimates returns a DataFrame with growth rates
        ge = ticker.growth_estimates
        if ge is not None and not ge.empty:
            # Look for the stock's own column (symbol) or first column
            cols = [c for c in ge.columns if c not in ("Industry", "Sector", "S&P 500")]
            if cols:
                col = cols[0]
                for idx_label in ge.index:
                    idx_str = str(idx_label).lower()
                    if "next 5" in idx_str or "next 5 y" in idx_str:
                        val = _safe_float(ge.loc[idx_label, col])
                        if val is not None:
                            metrics["earnings_growth_estimate"] = val
                    elif "next year" in idx_str or "next 1" in idx_str:
                        val = _safe_float(ge.loc[idx_label, col])
                        if val is not None:
                            metrics.setdefault("earnings_growth_estimate", val)
    except Exception:
        pass

    try:
        # Revenue growth estimate from .revenue_estimate
        re = ticker.revenue_estimate
        if re is not None and not re.empty:
            if "growth" in re.index:
                # Next year column
                cols = list(re.columns)
                if len(cols) >= 2:
                    val = _safe_float(re.loc["growth", cols[1]])
                    if val is not None:
                        metrics["revenue_growth_estimate"] = val
    except Exception:
        pass


def fetch_extended_universe(
    symbols: list[str],
    logger: TradeLogger,
    max_age_hours: int = 24,
) -> list[dict]:
    """
    Fetch extended fundamentals for all symbols. SQLite cache with stale fallback.
    Returns list of dicts, each with {symbol, trailing_pe, ..., eps_current, ...}.
    """
    cached = logger.get_cached_extended_fundamentals(symbols, max_age_hours)
    stale_cache = {}
    if len(cached) < len(symbols):
        stale_cache = logger.get_cached_extended_fundamentals(symbols, max_age_hours=8760)

    results = []
    need_fetch = [s for s in symbols if s not in cached]

    for sym, data in cached.items():
        data["symbol"] = sym
        results.append(data)

    fetch_failures = 0
    for sym in need_fetch:
        data = fetch_extended_single_ticker(sym)

        if data is None:
            fetch_failures += 1
            if sym in stale_cache:
                print(f"[ExtFundamentals] {sym}: using stale cache")
                stale = stale_cache[sym]
                stale["symbol"] = sym
                results.append(stale)
            continue

        try:
            logger.cache_extended_fundamentals(sym, data)
        except Exception as e:
            print(f"[ExtFundamentals] {sym}: cache write failed — {e}")

        data["symbol"] = sym
        results.append(data)

        # Polite delay — extended fetch hits more endpoints
        time.sleep(1.0)

    if fetch_failures > 0:
        print(f"[ExtFundamentals] {fetch_failures}/{len(need_fetch)} fetches failed")

    return results
