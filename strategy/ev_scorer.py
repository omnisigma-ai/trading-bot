"""
Enterprise Value Scorer — ASX Value Stock Selection
----------------------------------------------------
Scores a curated universe of ASX large-caps using fundamental data
to identify the most undervalued big-moat stock for profit reallocation.

Scoring model:
  1. Percentile-rank each metric within the universe
  2. Aggregate into three pillars:
     - Valuation (40%): EV/EBITDA, P/E, P/B, EV/Revenue, FCF yield
     - Quality   (40%): ROE, operating margin, gross margin, ROIC
     - Safety    (20%): debt/equity, interest coverage
  3. Composite = weighted sum of pillar scores (0-100)
  4. Filter by moat rating (WIDE or NARROW only)
  5. Select top-scored stock respecting position limits
"""
from dataclasses import dataclass, field


@dataclass
class EVScore:
    """Score card for a value stock candidate."""
    symbol: str
    composite_score: float = 0.0     # 0-100
    valuation_score: float = 0.0     # 0-100 pillar
    quality_score: float = 0.0       # 0-100 pillar
    safety_score: float = 0.0        # 0-100 pillar
    moat_rating: str = "NONE"        # WIDE | NARROW | NONE

    # Raw metrics for display/logging
    ev_to_ebitda: float | None = None
    trailing_pe: float | None = None
    price_to_book: float | None = None
    fcf_yield: float | None = None
    roe: float | None = None
    operating_margin: float | None = None
    debt_to_equity: float | None = None
    dividend_yield: float | None = None

    # Snowflake (SWS-style 30-check scoring)
    snowflake_total: int = 0           # 0-30 checks passed
    snowflake_attempted: int = 0       # checks with data
    snowflake_value: int = 0           # 0-6
    snowflake_future: int = 0          # 0-6
    snowflake_past: int = 0            # 0-6
    snowflake_health: int = 0          # 0-6
    snowflake_dividends: int = 0       # 0-6
    snowflake_normalized: float = 0.0  # 0-100
    dcf_intrinsic_value: float | None = None
    dcf_margin_of_safety: float | None = None

    # Selection
    rank: int = 0
    selected: bool = False
    reject_reason: str = ""

    def summary(self) -> str:
        status = "SELECTED" if self.selected else (
            f"REJECT ({self.reject_reason})" if self.reject_reason else "PASS"
        )
        pe_str = f"{self.trailing_pe:.1f}" if self.trailing_pe else "n/a"
        roe_str = f"{self.roe * 100:.1f}%" if self.roe else "n/a"
        sf_str = f" | SF:{self.snowflake_total}/{self.snowflake_attempted}" if self.snowflake_attempted else ""
        dcf_str = f" | DCF:{self.dcf_margin_of_safety:+.0%}" if self.dcf_margin_of_safety is not None else ""
        return (
            f"#{self.rank} {self.symbol} | "
            f"Score {self.composite_score:.1f} | "
            f"V:{self.valuation_score:.0f} Q:{self.quality_score:.0f} S:{self.safety_score:.0f} | "
            f"Moat: {self.moat_rating} | "
            f"P/E {pe_str} | ROE {roe_str}{sf_str}{dcf_str} | "
            f"{status}"
        )


# ── Moat Classification ──────────────────────────────────────────────────

def classify_moat(
    roe: float | None,
    operating_margin: float | None,
    gross_margin: float | None,
    debt_to_equity: float | None,
    sector: str = "",
) -> str:
    """
    Classify economic moat based on raw quality metrics.

    WIDE:   Durable competitive advantage — high returns, strong margins, low debt
    NARROW: Some competitive advantage — decent returns
    NONE:   No clear moat

    Banks get relaxed margin thresholds (their margins aren't comparable to
    non-financial companies).
    """
    is_bank = sector in ("banks", "insurance")

    if is_bank:
        # Banks: skip margin checks, focus on ROE and debt
        if roe is not None and roe > 0.12:
            return "WIDE"
        elif roe is not None and roe > 0.08:
            return "NARROW"
        return "NONE"

    # Standard moat classification
    has_high_roe = roe is not None and roe > 0.15
    has_good_gross = gross_margin is not None and gross_margin > 0.20
    has_good_op = operating_margin is not None and operating_margin > 0.15
    low_debt = debt_to_equity is None or debt_to_equity < 100

    if has_high_roe and has_good_gross and has_good_op and low_debt:
        return "WIDE"

    has_ok_roe = roe is not None and roe > 0.10
    has_ok_gross = gross_margin is not None and gross_margin > 0.15
    has_ok_op = operating_margin is not None and operating_margin > 0.10
    ok_debt = debt_to_equity is None or debt_to_equity < 200

    if has_ok_roe and has_ok_gross and has_ok_op and ok_debt:
        return "NARROW"

    return "NONE"


# ── Percentile Ranking ────────────────────────────────────────────────────

def _percentile_rank(values: list[tuple[str, float]], lower_is_better: bool = False) -> dict[str, float]:
    """
    Rank values as percentiles (0-100). Higher percentile = better.

    Args:
        values: List of (symbol, metric_value) tuples
        lower_is_better: True for valuation ratios (lower P/E = higher percentile)

    Returns:
        {symbol: percentile_score}
    """
    if not values:
        return {}

    n = len(values)
    if n == 1:
        return {values[0][0]: 50.0}

    # Sort so worst is first, best is last (higher rank_pos = higher percentile)
    # lower_is_better=True:  sort descending → worst (highest) first, best (lowest) last
    # lower_is_better=False: sort ascending  → worst (lowest) first, best (highest) last
    sorted_vals = sorted(values, key=lambda x: x[1], reverse=lower_is_better)
    ranks = {}
    for rank_pos, (sym, _) in enumerate(sorted_vals):
        ranks[sym] = (rank_pos / (n - 1)) * 100

    return ranks


# ── Pillar Definitions ────────────────────────────────────────────────────

# (metric_key, lower_is_better)
VALUATION_METRICS = [
    ("ev_to_ebitda", True),
    ("trailing_pe", True),
    ("price_to_book", True),
    ("ev_to_revenue", True),
    ("fcf_yield", False),       # higher FCF yield = cheaper
]

QUALITY_METRICS = [
    ("roe", False),
    ("operating_margin", False),
    ("gross_margin", False),
    ("roic", False),
]

SAFETY_METRICS = [
    ("debt_to_equity", True),    # lower debt = safer
    ("interest_coverage", False),  # higher coverage = safer
]

DEFAULT_WEIGHTS = {"valuation": 0.40, "quality": 0.40, "safety": 0.20}


def _compute_pillar_scores(
    fundamentals: list[dict],
    metrics_spec: list[tuple[str, bool]],
) -> dict[str, float]:
    """
    Compute pillar scores by averaging percentile ranks of constituent metrics.

    For each metric, only stocks with valid data are ranked.
    A stock's pillar score is the mean of its available metric percentiles.
    """
    symbol_percentiles: dict[str, list[float]] = {
        f["symbol"]: [] for f in fundamentals
    }

    for metric_key, lower_is_better in metrics_spec:
        # Collect valid values
        valid = []
        for f in fundamentals:
            val = f.get(metric_key)
            if val is not None and val == val:  # not None, not NaN
                # Filter out nonsensical values
                if lower_is_better and val <= 0:
                    continue  # negative P/E or EV/EBITDA = unprofitable, skip
                valid.append((f["symbol"], float(val)))

        if not valid:
            continue

        ranks = _percentile_rank(valid, lower_is_better)
        for sym, pctl in ranks.items():
            symbol_percentiles[sym].append(pctl)

    # Average percentiles per symbol
    result = {}
    for sym, pctls in symbol_percentiles.items():
        if pctls:
            result[sym] = sum(pctls) / len(pctls)
        else:
            result[sym] = 25.0  # default penalty for no data

    return result


# ── Main Scoring Functions ────────────────────────────────────────────────

def score_universe(
    fundamentals: list[dict],
    sector_map: dict[str, str],
    weights: dict = None,
    snowflake_weight: float = 0.0,
    snowflake_dcf_config: dict | None = None,
) -> list[EVScore]:
    """
    Score all stocks in the universe using percentile ranking.

    Args:
        fundamentals: List of dicts from fetch_universe_fundamentals()
        sector_map: {symbol: sector} for moat classification
        weights: Pillar weights (default: valuation 40%, quality 40%, safety 20%)

    Returns:
        List of EVScore sorted by composite_score descending
    """
    if not fundamentals:
        return []

    w = weights or DEFAULT_WEIGHTS

    # Compute pillar scores
    valuation_scores = _compute_pillar_scores(fundamentals, VALUATION_METRICS)
    quality_scores = _compute_pillar_scores(fundamentals, QUALITY_METRICS)
    safety_scores = _compute_pillar_scores(fundamentals, SAFETY_METRICS)

    scores = []
    for f in fundamentals:
        sym = f["symbol"]
        sector = sector_map.get(sym, "")

        v_score = valuation_scores.get(sym, 25.0)
        q_score = quality_scores.get(sym, 25.0)
        s_score = safety_scores.get(sym, 25.0)

        composite = (
            v_score * w.get("valuation", 0.4) +
            q_score * w.get("quality", 0.4) +
            s_score * w.get("safety", 0.2)
        )

        moat = classify_moat(
            roe=f.get("roe"),
            operating_margin=f.get("operating_margin"),
            gross_margin=f.get("gross_margin"),
            debt_to_equity=f.get("debt_to_equity"),
            sector=sector,
        )

        ev = EVScore(
            symbol=sym,
            composite_score=round(composite, 1),
            valuation_score=round(v_score, 1),
            quality_score=round(q_score, 1),
            safety_score=round(s_score, 1),
            moat_rating=moat,
            ev_to_ebitda=f.get("ev_to_ebitda"),
            trailing_pe=f.get("trailing_pe"),
            price_to_book=f.get("price_to_book"),
            fcf_yield=f.get("fcf_yield"),
            roe=f.get("roe"),
            operating_margin=f.get("operating_margin"),
            debt_to_equity=f.get("debt_to_equity"),
            dividend_yield=f.get("dividend_yield"),
        )
        scores.append(ev)

    # ── Snowflake scoring (SWS 30-check model) ─────────────────────────
    from strategy.snowflake_scorer import score_snowflake
    snowflake_scores = score_snowflake(fundamentals, sector_map, snowflake_dcf_config)

    for ev in scores:
        sf = snowflake_scores.get(ev.symbol)
        if sf:
            ev.snowflake_total = sf.total_checks_passed
            ev.snowflake_attempted = sf.total_checks_attempted
            ev.snowflake_value = sf.value_passed
            ev.snowflake_future = sf.future_passed
            ev.snowflake_past = sf.past_passed
            ev.snowflake_health = sf.health_passed
            ev.snowflake_dividends = sf.dividend_passed
            ev.snowflake_normalized = sf.normalized_score
            ev.dcf_intrinsic_value = sf.dcf_intrinsic_value
            ev.dcf_margin_of_safety = sf.dcf_margin_of_safety

            # Blend snowflake into composite if weight > 0
            if snowflake_weight > 0:
                original_weight = 1.0 - snowflake_weight
                ev.composite_score = round(
                    ev.composite_score * original_weight
                    + sf.normalized_score * snowflake_weight,
                    1,
                )

    # Sort by composite score descending, assign ranks
    scores.sort(key=lambda s: s.composite_score, reverse=True)
    for i, s in enumerate(scores):
        s.rank = i + 1

    return scores


def select_best_stock(
    scores: list[EVScore],
    current_holdings: dict[str, float],
    max_position_aud: float = 500.0,
    min_score: float = 40.0,
    min_moat: str = "NARROW",
    min_snowflake: int = 0,
) -> EVScore | None:
    """
    Pick the single best stock for this reallocation cycle.

    Filters:
      1. Moat must be WIDE or NARROW (if min_moat="NARROW")
      2. Composite score >= min_score
      3. Snowflake checks passed >= min_snowflake (if > 0)
      4. Current position < max_position_aud
    Selects the highest-scoring stock that passes all filters.

    Args:
        scores: Sorted EVScore list from score_universe()
        current_holdings: {symbol: total_invested_aud} from DB
        max_position_aud: Max investment in a single stock
        min_score: Minimum composite score to consider
        min_moat: Minimum moat rating ("WIDE" or "NARROW")
        min_snowflake: Minimum snowflake checks passed (0 = disabled)

    Returns:
        Selected EVScore with .selected=True, or None
    """
    acceptable_moats = {"WIDE"} if min_moat == "WIDE" else {"WIDE", "NARROW"}

    for score in scores:
        if score.moat_rating not in acceptable_moats:
            score.reject_reason = f"moat={score.moat_rating}"
            continue

        if score.composite_score < min_score:
            score.reject_reason = f"score {score.composite_score:.1f} < {min_score}"
            continue

        if min_snowflake > 0 and score.snowflake_total < min_snowflake:
            score.reject_reason = (
                f"snowflake {score.snowflake_total}/{score.snowflake_attempted} < {min_snowflake}"
            )
            continue

        current_invested = current_holdings.get(score.symbol, 0.0)
        if current_invested >= max_position_aud:
            score.reject_reason = f"position A${current_invested:.0f} >= max A${max_position_aud:.0f}"
            continue

        score.selected = True
        return score

    return None
