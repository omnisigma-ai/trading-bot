"""
Opportunity Scorer — Asymmetric Edge Detection
-----------------------------------------------
Unified scoring framework that evaluates every trade setup across
any instrument type (forex, stocks, ETFs) using:

  1. Risk:Reward Ratio — how much you stand to gain vs lose
  2. Setup Probability — historical win rate for this pattern type
  3. Expected Value (EV) — probability-weighted payoff
  4. Asymmetry Score — composite score favouring high-EV, high-R:R setups

A trade is only taken if its asymmetry score exceeds the minimum threshold.
This is the core filter for detecting opportunities where the payoff
distribution is skewed in our favour.

Scoring formula:
  EV = (win_prob × reward) - (loss_prob × risk)
  EV_ratio = EV / risk
  asymmetry_score = EV_ratio × rr_bonus × confluence_bonus

Where:
  rr_bonus = log2(R:R) + 1    (rewards higher R:R setups logarithmically)
  confluence_bonus = 1 + 0.1 per confluence factor present
"""
import json
import math
from dataclasses import dataclass, field

from strategy.base import TradeIntent


# ── Correlation / Exposure Groups ────────────────────────────────────────

# Sector map for stock correlation detection
STOCK_SECTORS = {
    "AAPL": "technology", "MSFT": "technology", "GOOGL": "technology",
    "META": "technology", "CRM": "technology",
    "NVDA": "semiconductor", "AMD": "semiconductor",
    "AMZN": "consumer", "TSLA": "consumer", "NFLX": "consumer",
}


def get_exposure_group(intent: TradeIntent) -> set[str]:
    """
    Return the set of exposure factors for this trade.

    Forex: decomposed into currency legs
      BUY AUDUSD  → {long_AUD, short_USD}
      SELL GBPJPY → {short_GBP, long_JPY}

    Stocks: mapped to sector
      BUY NVDA → {stock_semiconductor}

    Two intents are correlated if their exposure groups overlap.
    """
    if intent.instrument_type == "forex":
        base = intent.symbol[:3].upper()
        quote = intent.symbol[3:].upper()
        if intent.direction == "BUY":
            return {f"long_{base}", f"short_{quote}"}
        else:
            return {f"short_{base}", f"long_{quote}"}
    elif intent.instrument_type == "stock":
        sector = STOCK_SECTORS.get(intent.symbol, "unknown")
        return {f"stock_{sector}"}
    return set()


@dataclass
class SetupScore:
    """Score card for a potential trade."""
    symbol: str
    strategy: str
    direction: str

    # Core metrics
    risk_reward_ratio: float        # TP distance / SL distance
    win_probability: float          # estimated probability of hitting TP (0-1)
    expected_value: float           # probability-weighted payoff per dollar risked
    asymmetry_score: float          # composite score (higher = more asymmetric)

    # Components
    rr_bonus: float = 1.0           # bonus multiplier for high R:R
    confluence_count: int = 0       # number of confirming factors
    confluence_factors: list = field(default_factory=list)

    # Decision
    accepted: bool = False
    reject_reason: str = ""

    def summary(self) -> str:
        status = "ACCEPT" if self.accepted else f"REJECT ({self.reject_reason})"
        return (
            f"{self.symbol} {self.direction} [{self.strategy}] | "
            f"R:R {self.risk_reward_ratio:.1f}:1 | "
            f"Win% {self.win_probability*100:.0f}% | "
            f"EV {self.expected_value:+.2f} | "
            f"Score {self.asymmetry_score:.2f} | "
            f"Confluence {self.confluence_count} | "
            f"{status}"
        )


# ── Confluence factor detectors ──────────────────────────────────────────

def _check_range_compression(metadata: dict) -> bool:
    """Asian range is compressed (below 25th percentile) — expect expansion."""
    tp_mult = metadata.get("tp_multiplier", 2.0)
    return tp_mult > 2.0  # adaptive TP already detected compression


def _check_volume_confirmation(metadata: dict) -> bool:
    """Volume is above average — confirms the move has participation."""
    return metadata.get("volume_ratio", 0) >= 1.5


def _check_trend_alignment(metadata: dict) -> bool:
    """Trade direction aligns with the higher timeframe trend."""
    return metadata.get("trend_aligned", False)


def _check_support_resistance(metadata: dict) -> bool:
    """Entry is near a key support/resistance level."""
    return metadata.get("near_sr_level", False)


def _check_new_high_breakout(metadata: dict) -> bool:
    """Price is breaking out to a new N-day high (momentum)."""
    return metadata.get("new_high_breakout", False)


def _check_mean_reversion(metadata: dict) -> bool:
    """Price has reverted significantly from mean (oversold/overbought)."""
    return metadata.get("mean_reversion_signal", False)


# All available confluence checks
CONFLUENCE_CHECKS = {
    "range_compression": _check_range_compression,
    "volume_confirmation": _check_volume_confirmation,
    "trend_alignment": _check_trend_alignment,
    "support_resistance": _check_support_resistance,
    "new_high_breakout": _check_new_high_breakout,
    "mean_reversion": _check_mean_reversion,
}


# ── Win probability estimation ───────────────────────────────────────────

# Base win rates by strategy + R:R tier (from backtesting / market research)
# These are starting estimates — will be calibrated with actual trade data
BASE_WIN_RATES = {
    "london_breakout": {
        # R:R → estimated win probability
        (0, 1.5): 0.55,      # tight TP — wins often but small
        (1.5, 2.5): 0.45,    # standard 2:1 — balanced
        (2.5, 4.0): 0.35,    # extended 3:1 — wins less but bigger
        (4.0, float("inf")): 0.25,  # aggressive 5:1+ — rare but huge
    },
    "momentum_stocks": {
        (0, 2.0): 0.50,
        (2.0, 3.5): 0.40,
        (3.5, 6.0): 0.30,
        (6.0, float("inf")): 0.20,
    },
    "mean_reversion": {
        (0, 1.5): 0.60,
        (1.5, 3.0): 0.45,
        (3.0, float("inf")): 0.30,
    },
}

# Default for unknown strategies
DEFAULT_WIN_RATES = {
    (0, 2.0): 0.45,
    (2.0, 4.0): 0.35,
    (4.0, float("inf")): 0.25,
}


def estimate_win_probability(
    strategy: str,
    rr_ratio: float,
    confluence_count: int = 0,
) -> float:
    """
    Estimate the probability of a trade hitting TP given its R:R and confluence.

    Higher R:R = lower base probability (harder to reach distant target).
    Each confluence factor adds +3% to the base probability (capped at +15%).
    """
    rates = BASE_WIN_RATES.get(strategy, DEFAULT_WIN_RATES)

    base_prob = 0.35  # fallback
    for (lo, hi), prob in rates.items():
        if lo <= rr_ratio < hi:
            base_prob = prob
            break

    # Confluence bonus: each factor adds 3% (max 5 factors = +15%)
    confluence_bonus = min(confluence_count * 0.03, 0.15)

    return min(base_prob + confluence_bonus, 0.85)  # cap at 85%


# ── Core scoring function ────────────────────────────────────────────────

def score_opportunity(
    intent: TradeIntent,
    min_score: float = 0.5,
    min_rr: float = 1.5,
    min_ev: float = 0.0,
) -> SetupScore:
    """
    Score a trade intent for asymmetric edge.

    Args:
        intent: The trade setup to evaluate
        min_score: Minimum asymmetry score to accept (default 0.5)
        min_rr: Minimum R:R ratio to consider (default 1.5:1)
        min_ev: Minimum expected value per dollar risked (default 0.0 = breakeven)

    Returns:
        SetupScore with acceptance decision and detailed metrics
    """
    metadata = intent.metadata or {}

    # Calculate R:R ratio
    if intent.is_forex:
        sl_distance = intent.risk_pips
        tp_pips = metadata.get("tp_pips", 0)
        rr_ratio = tp_pips / sl_distance if sl_distance > 0 else 0
    else:
        # Stocks: use price distances
        if intent.direction == "BUY":
            sl_distance = intent.entry_price - intent.stop_loss
            tp_distance = intent.take_profit - intent.entry_price if intent.take_profit > 0 else sl_distance * 2
        else:
            sl_distance = intent.stop_loss - intent.entry_price
            tp_distance = intent.entry_price - intent.take_profit if intent.take_profit > 0 else sl_distance * 2
        rr_ratio = tp_distance / sl_distance if sl_distance > 0 else 0

    # Check confluence factors
    active_factors = []
    for name, check_fn in CONFLUENCE_CHECKS.items():
        if check_fn(metadata):
            active_factors.append(name)
    confluence_count = len(active_factors)

    # Estimate win probability
    win_prob = estimate_win_probability(intent.strategy, rr_ratio, confluence_count)
    loss_prob = 1.0 - win_prob

    # Expected value per dollar risked
    # EV = (win_prob × reward) - (loss_prob × risk), normalised to per-dollar-risked
    ev = (win_prob * rr_ratio) - (loss_prob * 1.0)

    # R:R bonus (logarithmic — diminishing returns above 3:1)
    rr_bonus = math.log2(max(rr_ratio, 1.0)) + 1.0

    # Confluence bonus
    conf_bonus = 1.0 + (confluence_count * 0.1)

    # Composite asymmetry score
    asymmetry_score = max(ev, 0) * rr_bonus * conf_bonus

    # Decision
    accepted = True
    reject_reason = ""

    if rr_ratio < min_rr:
        accepted = False
        reject_reason = f"R:R {rr_ratio:.1f} < min {min_rr}"
    elif ev < min_ev:
        accepted = False
        reject_reason = f"EV {ev:.2f} < min {min_ev}"
    elif asymmetry_score < min_score:
        accepted = False
        reject_reason = f"score {asymmetry_score:.2f} < min {min_score}"

    return SetupScore(
        symbol=intent.symbol,
        strategy=intent.strategy,
        direction=intent.direction,
        risk_reward_ratio=round(rr_ratio, 2),
        win_probability=round(win_prob, 3),
        expected_value=round(ev, 3),
        asymmetry_score=round(asymmetry_score, 3),
        rr_bonus=round(rr_bonus, 3),
        confluence_count=confluence_count,
        confluence_factors=active_factors,
        accepted=accepted,
        reject_reason=reject_reason,
    )


def filter_opportunities(
    intents: list[TradeIntent],
    min_score: float = 0.5,
    min_rr: float = 1.5,
    min_ev: float = 0.0,
    logger=None,
    session_id: str = "",
) -> tuple[list[TradeIntent], list[SetupScore]]:
    """
    Score all trade intents and return those that pass the asymmetry filter,
    rejecting correlated duplicates.

    Iterates from highest score downward. A setup is accepted only if its
    exposure group does not overlap with any already-accepted setup.
    Portfolio risk limits (not a hard trade cap) are the only other constraint.

    Args:
        intents: Raw trade intents from all strategies
        min_score: Minimum asymmetry score
        min_rr: Minimum R:R ratio
        min_ev: Minimum expected value
        logger: TradeLogger instance for feature decision tracking
        session_id: Session identifier for grouping decisions

    Returns:
        (accepted_intents, all_scores) — filtered intents + full score cards
    """
    scored = []
    for intent in intents:
        score = score_opportunity(intent, min_score, min_rr, min_ev)
        scored.append((intent, score))

    # Sort by asymmetry score descending — best setups evaluated first
    scored.sort(key=lambda x: x[1].asymmetry_score, reverse=True)

    accepted_intents = []
    accepted_exposures: set[str] = set()

    for intent, score in scored:
        if not score.accepted:
            # Log scorer rejection
            if logger and session_id:
                _log_scorer_decision(logger, session_id, intent, score, "reject",
                                     f"opportunity_scorer", score.reject_reason)
            continue

        # Check correlation with already-accepted intents
        exposure = get_exposure_group(intent)
        overlap = exposure & accepted_exposures
        if overlap:
            score.accepted = False
            score.reject_reason = f"correlated: {overlap}"
            if logger and session_id:
                _log_scorer_decision(logger, session_id, intent, score, "reject",
                                     "correlation_filter", score.reject_reason)
            continue

        accepted_intents.append(intent)
        accepted_exposures |= exposure

        # Log acceptance
        if logger and session_id:
            _log_scorer_decision(logger, session_id, intent, score, "accept",
                                 "opportunity_scorer",
                                 f"score={score.asymmetry_score:.2f}")

    all_scores = [s for _, s in scored]
    return accepted_intents, all_scores


def _log_scorer_decision(
    logger, session_id: str, intent: TradeIntent, score: SetupScore,
    decision: str, feature: str, rule: str,
) -> None:
    """Log an opportunity scorer or correlation filter decision."""
    context = {
        "rr_ratio": score.risk_reward_ratio,
        "win_prob": score.win_probability,
        "ev": score.expected_value,
        "asymmetry_score": score.asymmetry_score,
        "confluence": score.confluence_factors,
        "entry_price": intent.entry_price,
        "stop_loss": intent.stop_loss,
        "take_profit": intent.take_profit,
    }
    try:
        from strategy.feature_tracker import FeatureDecision, log_decision
        log_decision(logger, FeatureDecision(
            feature=feature,
            symbol=intent.symbol,
            strategy=intent.strategy,
            decision=decision,
            rule=rule,
            context=context,
            session_id=session_id,
        ))
    except Exception as e:
        print(f"[FeatureTracker] Failed to log {feature} decision: {e}")
