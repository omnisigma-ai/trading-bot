"""
Snowflake Scorer — SimplyWallSt-Style 30-Check Analysis
--------------------------------------------------------
Implements a binary pass/fail scoring model across 5 categories:
  Value (6), Future (6), Past (6), Health (6), Dividends (6)

Based on SimplyWallSt's open-source Company Analysis Model:
  https://github.com/SimplyWallSt/Company-Analysis-Model

Each check returns bool | None (None = insufficient data, not attempted).
The normalized score = (passed / attempted) * 100.

This augments — not replaces — the existing percentile-based EV scorer.
"""
from dataclasses import dataclass, field
from statistics import median


@dataclass
class SnowflakeScore:
    """Result of running all 30 checks on a single stock."""
    total_checks_passed: int = 0
    total_checks_attempted: int = 0

    value_passed: int = 0
    value_attempted: int = 0
    value_checks: dict[str, bool | None] = field(default_factory=dict)

    future_passed: int = 0
    future_attempted: int = 0
    future_checks: dict[str, bool | None] = field(default_factory=dict)

    past_passed: int = 0
    past_attempted: int = 0
    past_checks: dict[str, bool | None] = field(default_factory=dict)

    health_passed: int = 0
    health_attempted: int = 0
    health_checks: dict[str, bool | None] = field(default_factory=dict)

    dividend_passed: int = 0
    dividend_attempted: int = 0
    dividend_checks: dict[str, bool | None] = field(default_factory=dict)

    normalized_score: float = 0.0  # 0-100
    dcf_intrinsic_value: float | None = None
    dcf_margin_of_safety: float | None = None

    def summary(self) -> str:
        return (
            f"{self.total_checks_passed}/{self.total_checks_attempted} "
            f"(V:{self.value_passed} F:{self.future_passed} P:{self.past_passed} "
            f"H:{self.health_passed} D:{self.dividend_passed})"
        )


# ── DCF Model ────────────────────────────────────────────────────────────

def simple_dcf(
    fcf_history: list[float],
    growth_estimate: float | None,
    total_debt: float,
    total_cash: float,
    shares_outstanding: float,
    beta: float = 1.0,
    risk_free_rate: float = 0.042,
    equity_risk_premium: float = 0.055,
    terminal_growth_rate: float = 0.025,
    stage1_years: int = 5,
) -> float | None:
    """
    2-stage FCF DCF model.

    Stage 1: project FCF for stage1_years using growth estimate.
    Stage 2: terminal value via Gordon Growth Model.
    Discount rate: CAPM = risk_free + beta * ERP.

    Returns intrinsic value per share, or None if insufficient data.
    """
    if not fcf_history or shares_outstanding is None or shares_outstanding <= 0:
        return None

    base_fcf = fcf_history[0]
    if base_fcf is None or base_fcf <= 0:
        return None

    # Growth rate: use estimate, or compute from history, capped at 30%
    if growth_estimate is not None:
        g = min(abs(growth_estimate), 0.30)
    elif len(fcf_history) >= 2:
        oldest = fcf_history[-1]
        if oldest and oldest > 0:
            n = len(fcf_history) - 1
            g = min(max((base_fcf / oldest) ** (1.0 / n) - 1, 0), 0.30)
        else:
            g = 0.05  # conservative default
    else:
        g = 0.05

    # Clamp beta per SWS methodology
    beta = max(0.8, min(beta, 2.0))
    discount_rate = risk_free_rate + beta * equity_risk_premium
    if discount_rate <= terminal_growth_rate:
        return None

    # Stage 1: project and discount FCF
    pv_stage1 = 0.0
    projected_fcf = base_fcf
    for year in range(1, stage1_years + 1):
        projected_fcf *= (1 + g)
        pv_stage1 += projected_fcf / (1 + discount_rate) ** year

    # Stage 2: terminal value
    terminal_fcf = projected_fcf * (1 + terminal_growth_rate)
    terminal_value = terminal_fcf / (discount_rate - terminal_growth_rate)
    pv_terminal = terminal_value / (1 + discount_rate) ** stage1_years

    # Enterprise value → equity value
    enterprise_value = pv_stage1 + pv_terminal
    equity_value = enterprise_value - total_debt + total_cash

    if equity_value <= 0:
        return None

    return equity_value / shares_outstanding


# ── Industry Averages ─────────────────────────────────────────────────────

def compute_industry_averages(
    fundamentals: list[dict],
    sector_map: dict[str, str],
) -> dict[str, dict]:
    """
    Compute per-sector and market-wide medians from the universe.

    Returns {"market": {...}, "banks": {...}, "resources": {...}, ...}
    """
    # Group by sector
    sectors: dict[str, list[dict]] = {}
    all_stocks = []
    for f in fundamentals:
        sym = f.get("symbol", "")
        sector = sector_map.get(sym, "other")
        sectors.setdefault(sector, []).append(f)
        all_stocks.append(f)

    def _median_of(stocks, key, positive_only=False):
        vals = []
        for s in stocks:
            v = s.get(key)
            if v is not None and v == v:
                if positive_only and v <= 0:
                    continue
                vals.append(v)
        return median(vals) if vals else None

    result = {}

    # Market-wide averages
    result["market"] = {
        "pe_avg": _median_of(all_stocks, "trailing_pe", positive_only=True),
        "pb_avg": _median_of(all_stocks, "price_to_book", positive_only=True),
        "earnings_growth_avg": _median_of(all_stocks, "earnings_growth_ttm"),
        "revenue_growth_avg": _median_of(all_stocks, "revenue_growth_ttm"),
        "eps_growth_avg": _median_of(all_stocks, "eps_growth_1y"),
        "roa_avg": _median_of(all_stocks, "roa"),
        "roe_avg": _median_of(all_stocks, "roe"),
    }

    # Per-sector averages
    for sector, stocks in sectors.items():
        result[sector] = {
            "pe_avg": _median_of(stocks, "trailing_pe", positive_only=True),
            "pb_avg": _median_of(stocks, "price_to_book", positive_only=True),
            "earnings_growth_avg": _median_of(stocks, "earnings_growth_ttm"),
            "revenue_growth_avg": _median_of(stocks, "revenue_growth_ttm"),
            "eps_growth_avg": _median_of(stocks, "eps_growth_1y"),
            "roa_avg": _median_of(stocks, "roa"),
            "roe_avg": _median_of(stocks, "roe"),
        }

    return result


# ── Individual Checks ─────────────────────────────────────────────────────

# Helpers
_SAVINGS_RATE = 0.0435
_INFLATION_RATE = 0.025
_THRESHOLD_RATE = _SAVINGS_RATE + _INFLATION_RATE  # ~6.85%


def _get(stock, key):
    """Get a metric value, returning None for NaN."""
    v = stock.get(key)
    if v is None:
        return None
    if v != v:  # NaN
        return None
    return v


# ── VALUE CHECKS (6) ─────────────────────────────────────────────────────

def _check_dcf(stock, dcf_config, threshold_pct):
    """DCF intrinsic value >= threshold_pct above market price."""
    price = _get(stock, "current_price")
    if price is None or price <= 0:
        return None, None

    fcf_hist = stock.get("fcf_history", [])
    growth_est = _get(stock, "earnings_growth_estimate") or _get(stock, "revenue_growth_ttm")
    total_debt = _get(stock, "total_debt") or 0
    total_cash = _get(stock, "total_cash") or 0
    shares = _get(stock, "shares_outstanding")

    intrinsic = simple_dcf(
        fcf_history=fcf_hist,
        growth_estimate=growth_est,
        total_debt=total_debt,
        total_cash=total_cash,
        shares_outstanding=shares,
        beta=_get(stock, "beta") or 1.0,
        risk_free_rate=dcf_config.get("risk_free_rate", 0.042),
        equity_risk_premium=dcf_config.get("equity_risk_premium", 0.055),
        terminal_growth_rate=dcf_config.get("terminal_growth_rate", 0.025),
        stage1_years=dcf_config.get("stage1_years", 5),
    )

    if intrinsic is None:
        return None, None

    margin = (intrinsic - price) / price
    return intrinsic >= price * (1 + threshold_pct), (intrinsic, margin)


def check_dcf_20pct(stock, dcf_config):
    result, dcf_data = _check_dcf(stock, dcf_config, 0.20)
    return result, dcf_data


def check_dcf_40pct(stock, dcf_config):
    result, dcf_data = _check_dcf(stock, dcf_config, 0.40)
    return result, dcf_data


def check_pe_vs_market(stock, market_avgs):
    pe = _get(stock, "trailing_pe")
    market_pe = market_avgs.get("pe_avg")
    if pe is None or market_pe is None:
        return None
    return pe > 0 and pe < market_pe


def check_pe_vs_industry(stock, industry_avgs):
    pe = _get(stock, "trailing_pe")
    ind_pe = industry_avgs.get("pe_avg") if industry_avgs else None
    if pe is None or ind_pe is None:
        return None
    return pe > 0 and pe < ind_pe


def check_peg_ratio(stock):
    peg = _get(stock, "peg_ratio")
    if peg is None:
        return None
    return 0 < peg < 1


def check_pb_vs_industry(stock, industry_avgs):
    pb = _get(stock, "price_to_book")
    ind_pb = industry_avgs.get("pb_avg") if industry_avgs else None
    if pb is None or ind_pb is None:
        return None
    return pb > 0 and pb < ind_pb


# ── FUTURE PERFORMANCE CHECKS (6) ────────────────────────────────────────

def _get_earnings_growth(stock):
    """Get best available earnings growth estimate."""
    return _get(stock, "earnings_growth_estimate") or _get(stock, "earnings_growth_ttm")


def check_earnings_growth_vs_savings(stock):
    g = _get_earnings_growth(stock)
    if g is None:
        return None
    return g > _THRESHOLD_RATE


def check_earnings_growth_vs_market(stock, market_avgs):
    g = _get_earnings_growth(stock)
    market_g = market_avgs.get("earnings_growth_avg")
    if g is None or market_g is None:
        return None
    return g > market_g


def check_revenue_growth_vs_market(stock, market_avgs):
    g = _get(stock, "revenue_growth_ttm") or _get(stock, "revenue_growth_estimate")
    market_g = market_avgs.get("revenue_growth_avg")
    if g is None or market_g is None:
        return None
    return g > market_g


def check_earnings_growth_20pct(stock):
    g = _get_earnings_growth(stock)
    if g is None:
        return None
    return g > 0.20


def check_revenue_growth_20pct(stock):
    g = _get(stock, "revenue_growth_ttm") or _get(stock, "revenue_growth_estimate")
    if g is None:
        return None
    return g > 0.20


def check_future_roe_20pct(stock):
    """Proxy: pass if current ROE > 20% (no forward estimate available)."""
    roe = _get(stock, "roe")
    if roe is None:
        return None
    return roe > 0.20


# ── PAST PERFORMANCE CHECKS (6) ──────────────────────────────────────────

def check_eps_growth_vs_industry(stock, industry_avgs):
    g = _get(stock, "eps_growth_1y")
    ind_g = industry_avgs.get("eps_growth_avg") if industry_avgs else None
    if g is None or ind_g is None:
        return None
    return g > ind_g


def check_eps_higher_than_5y_ago(stock):
    current = _get(stock, "eps_current")
    ago = _get(stock, "eps_5y_ago")
    if current is None or ago is None:
        return None
    return current > ago


def check_eps_growth_acceleration(stock):
    g_1y = _get(stock, "eps_growth_1y")
    g_5y = _get(stock, "eps_growth_5y_avg")
    if g_1y is None or g_5y is None:
        return None
    return g_1y > g_5y


def check_roe_above_20pct(stock):
    roe = _get(stock, "roe")
    if roe is None:
        return None
    return roe > 0.20


def check_roce_improving(stock):
    current = _get(stock, "roce_current")
    ago = _get(stock, "roce_3y_ago")
    if current is None or ago is None:
        return None
    return current > ago


def check_roa_vs_industry(stock, industry_avgs):
    roa = _get(stock, "roa")
    ind_roa = industry_avgs.get("roa_avg") if industry_avgs else None
    if roa is None or ind_roa is None:
        return None
    return roa > ind_roa


# ── HEALTH CHECKS (6) — Standard companies ───────────────────────────────

def check_current_assets_vs_current_liabilities(stock):
    ca = _get(stock, "current_assets")
    cl = _get(stock, "current_liabilities")
    if ca is not None and cl is not None:
        return ca > cl
    cr = _get(stock, "current_ratio")
    if cr is not None:
        return cr > 1.0
    return None


def check_current_assets_vs_long_term_liabilities(stock):
    ca = _get(stock, "current_assets")
    ltl = _get(stock, "long_term_liabilities")
    if ca is None or ltl is None:
        return None
    return ca > ltl


def check_de_not_increased(stock):
    de_current = _get(stock, "debt_to_equity")
    de_old = _get(stock, "de_5y_ago")
    if de_current is None or de_old is None:
        return None
    return de_current <= de_old


def check_de_below_40pct(stock):
    de = _get(stock, "debt_to_equity")
    if de is None:
        return None
    return de < 40


def check_operating_cf_vs_debt(stock):
    ocf = _get(stock, "operating_cashflow")
    debt = _get(stock, "total_debt")
    if ocf is None or debt is None:
        return None
    if debt <= 0:
        return True  # no debt = pass
    return ocf > debt * 0.20


def check_interest_coverage_5x(stock):
    ebit = _get(stock, "ebit")
    ie = _get(stock, "interest_expense")
    if ebit is not None and ie is not None and ie > 0:
        return ebit / ie > 5.0
    # Fallback to estimated interest coverage
    ic = _get(stock, "interest_coverage")
    if ic is not None:
        return ic > 5.0
    return None


# ── HEALTH CHECKS — Banks/Insurance ──────────────────────────────────────

def check_bank_leverage(stock):
    ta = _get(stock, "total_assets")
    eq = _get(stock, "stockholders_equity")
    if ta is None or eq is None or eq <= 0:
        return None
    return ta / eq <= 20


# ── DIVIDEND CHECKS (6) ──────────────────────────────────────────────────

def check_yield_above_25th_pct(stock, universe_yields):
    dy = _get(stock, "dividend_yield")
    if dy is None or not universe_yields:
        return None
    sorted_y = sorted(universe_yields)
    idx = max(0, int(len(sorted_y) * 0.25) - 1)
    return dy > sorted_y[idx]


def check_yield_above_75th_pct(stock, universe_yields):
    dy = _get(stock, "dividend_yield")
    if dy is None or not universe_yields:
        return None
    sorted_y = sorted(universe_yields)
    idx = min(len(sorted_y) - 1, int(len(sorted_y) * 0.75))
    return dy > sorted_y[idx]


def check_no_dividend_drops(stock):
    val = stock.get("dividend_has_10y_drop")
    if val is None:
        return None
    return val is False  # True means there WAS a drop, so we want False


def check_dividend_higher_than_10y_ago(stock):
    val = stock.get("dividend_higher_than_10y_ago")
    if val is None:
        return None
    return val is True


def check_payout_ratio(stock, sector=""):
    pr = _get(stock, "payout_ratio")
    if pr is None:
        return None
    max_payout = 1.0 if sector in ("reits",) else 0.9
    return 0 < pr < max_payout


def check_future_payout_ratio(stock, sector=""):
    """Proxy: use current payout ratio (no forward estimate available)."""
    return check_payout_ratio(stock, sector)


# ── Main Scoring ──────────────────────────────────────────────────────────

def run_snowflake_checks(
    stock: dict,
    industry_avgs: dict,
    market_avgs: dict,
    universe_yields: list[float],
    sector: str = "",
    dcf_config: dict | None = None,
) -> SnowflakeScore:
    """
    Run all 30 checks on a single stock.
    Returns a SnowflakeScore with per-category breakdowns.
    """
    dcf_cfg = dcf_config or {}
    score = SnowflakeScore()

    # ── VALUE ─────────────────────────────────────────────────────────
    dcf_result_20, dcf_data = check_dcf_20pct(stock, dcf_cfg)
    dcf_result_40, _ = check_dcf_40pct(stock, dcf_cfg)

    if dcf_data:
        score.dcf_intrinsic_value = round(dcf_data[0], 2)
        score.dcf_margin_of_safety = round(dcf_data[1], 4)

    value_checks = {
        "dcf_20pct_undervalued": dcf_result_20,
        "dcf_40pct_undervalued": dcf_result_40,
        "pe_below_market": check_pe_vs_market(stock, market_avgs),
        "pe_below_industry": check_pe_vs_industry(stock, industry_avgs),
        "peg_0_to_1": check_peg_ratio(stock),
        "pb_below_industry": check_pb_vs_industry(stock, industry_avgs),
    }

    # ── FUTURE ────────────────────────────────────────────────────────
    future_checks = {
        "earnings_growth_vs_savings": check_earnings_growth_vs_savings(stock),
        "earnings_growth_vs_market": check_earnings_growth_vs_market(stock, market_avgs),
        "revenue_growth_vs_market": check_revenue_growth_vs_market(stock, market_avgs),
        "earnings_growth_20pct": check_earnings_growth_20pct(stock),
        "revenue_growth_20pct": check_revenue_growth_20pct(stock),
        "future_roe_20pct": check_future_roe_20pct(stock),
    }

    # ── PAST ──────────────────────────────────────────────────────────
    past_checks = {
        "eps_growth_vs_industry": check_eps_growth_vs_industry(stock, industry_avgs),
        "eps_higher_than_5y_ago": check_eps_higher_than_5y_ago(stock),
        "eps_growth_acceleration": check_eps_growth_acceleration(stock),
        "roe_above_20pct": check_roe_above_20pct(stock),
        "roce_improving": check_roce_improving(stock),
        "roa_vs_industry": check_roa_vs_industry(stock, industry_avgs),
    }

    # ── HEALTH ────────────────────────────────────────────────────────
    is_financial = sector in ("banks", "insurance")

    if is_financial:
        health_checks = {
            "bank_leverage_20x": check_bank_leverage(stock),
            "bank_npl_provisions": None,      # data unavailable
            "bank_deposits_50pct": None,       # data unavailable
            "bank_loans_110pct": None,         # data unavailable
            "bank_loans_125pct_deposits": None, # data unavailable
            "bank_charge_offs": None,          # data unavailable
        }
    else:
        health_checks = {
            "current_assets_vs_current_liab": check_current_assets_vs_current_liabilities(stock),
            "current_assets_vs_long_term_liab": check_current_assets_vs_long_term_liabilities(stock),
            "de_not_increased_5y": check_de_not_increased(stock),
            "de_below_40pct": check_de_below_40pct(stock),
            "operating_cf_20pct_debt": check_operating_cf_vs_debt(stock),
            "interest_coverage_5x": check_interest_coverage_5x(stock),
        }

    # ── DIVIDENDS ─────────────────────────────────────────────────────
    dividend_checks = {
        "yield_above_25th_pct": check_yield_above_25th_pct(stock, universe_yields),
        "yield_above_75th_pct": check_yield_above_75th_pct(stock, universe_yields),
        "no_dividend_drops_10y": check_no_dividend_drops(stock),
        "dividend_higher_than_10y": check_dividend_higher_than_10y_ago(stock),
        "payout_ratio_sustainable": check_payout_ratio(stock, sector),
        "future_payout_sustainable": check_future_payout_ratio(stock, sector),
    }

    # ── Tally ─────────────────────────────────────────────────────────
    for category_name, checks_dict, attr_prefix in [
        ("value", value_checks, "value"),
        ("future", future_checks, "future"),
        ("past", past_checks, "past"),
        ("health", health_checks, "health"),
        ("dividend", dividend_checks, "dividend"),
    ]:
        passed = sum(1 for v in checks_dict.values() if v is True)
        attempted = sum(1 for v in checks_dict.values() if v is not None)

        setattr(score, f"{attr_prefix}_passed", passed)
        setattr(score, f"{attr_prefix}_attempted", attempted)
        setattr(score, f"{attr_prefix}_checks", checks_dict)

        score.total_checks_passed += passed
        score.total_checks_attempted += attempted

    if score.total_checks_attempted > 0:
        score.normalized_score = round(
            (score.total_checks_passed / score.total_checks_attempted) * 100, 1
        )

    return score


def score_snowflake(
    fundamentals: list[dict],
    sector_map: dict[str, str],
    dcf_config: dict | None = None,
) -> dict[str, SnowflakeScore]:
    """
    Run all 30 snowflake checks for each stock in the universe.
    Returns {symbol: SnowflakeScore}.
    """
    industry_avgs = compute_industry_averages(fundamentals, sector_map)
    market_avgs = industry_avgs.get("market", {})

    # Universe dividend yields for percentile checks
    universe_yields = [
        f["dividend_yield"] for f in fundamentals
        if f.get("dividend_yield") is not None
        and f["dividend_yield"] == f["dividend_yield"]
        and f["dividend_yield"] > 0
    ]

    results = {}
    for stock in fundamentals:
        sym = stock.get("symbol", "")
        sector = sector_map.get(sym, "")
        sf = run_snowflake_checks(
            stock=stock,
            industry_avgs=industry_avgs.get(sector, {}),
            market_avgs=market_avgs,
            universe_yields=universe_yields,
            sector=sector,
            dcf_config=dcf_config,
        )
        results[sym] = sf

    return results
