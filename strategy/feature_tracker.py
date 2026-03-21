"""
Feature Attribution Tracker
----------------------------
Central module for logging, scoring, and diagnosing feature decisions.

Every decision-making feature in the bot (opportunity scorer, correlation
filter, dip detector, EV scorer, commission check, risk limits) logs its
accept/reject/deploy/hold decisions here. This data is used to:

  1. Compute rolling "value scores" per feature (% of correct decisions)
  2. Cluster failure patterns by rule/threshold
  3. Auto-suggest threshold adjustments for underperformers
  4. Surface weekly health reports via Telegram

Value score formula:
  correct_decisions / evaluated_decisions * 100

Where "correct" means:
  - accept/deploy → trade was profitable
  - reject/hold   → counterfactual was negative (would have lost)
"""
import json
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime

from logs.trade_logger import TradeLogger


@dataclass
class FeatureDecision:
    """A single decision made by a feature."""
    feature: str        # "opportunity_scorer", "correlation_filter", etc.
    symbol: str         # AUDUSD, BHP, VGS, etc.
    strategy: str       # "london_breakout", "reallocation", etc.
    decision: str       # "accept", "reject", "deploy", "hold", "modify"
    rule: str           # specific rule that fired
    context: dict       # all relevant values at decision time
    session_id: str     # groups decisions from same run


def log_decision(logger: TradeLogger, d: FeatureDecision) -> int:
    """Write a feature decision to the database. Returns the decision ID."""
    return logger.log_feature_decision(
        feature=d.feature,
        symbol=d.symbol,
        strategy=d.strategy,
        decision=d.decision,
        rule=d.rule,
        context_json=json.dumps(d.context, default=str),
        session_id=d.session_id,
    )


@dataclass
class FeatureValueScore:
    """Value score summary for a single feature."""
    feature: str
    total: int = 0
    correct: int = 0
    pending: int = 0
    score: float | None = None   # 0-100 or None if all pending

    @property
    def evaluated(self) -> int:
        return self.total - self.pending

    @property
    def healthy(self) -> bool:
        return self.score is not None and self.score >= 50.0


def compute_value_scores(
    logger: TradeLogger, lookback_days: int = 90,
) -> list[FeatureValueScore]:
    """Compute value scores for all features over the lookback period."""
    raw = logger.get_feature_value_scores(lookback_days)
    results = []
    for feature, stats in sorted(raw.items()):
        results.append(FeatureValueScore(
            feature=feature,
            total=stats["total"],
            correct=stats["correct"],
            pending=stats["pending"],
            score=stats["score"],
        ))
    return results


@dataclass
class FeatureDiagnostic:
    """Diagnostic report for an underperforming feature."""
    feature: str
    value_score: float | None
    total_decisions: int
    failure_clusters: list[dict] = field(default_factory=list)
    suggested_fixes: list[str] = field(default_factory=list)


def diagnose_feature(
    logger: TradeLogger, feature: str, lookback_days: int = 90,
) -> FeatureDiagnostic:
    """
    Analyse an underperforming feature to find failure patterns.

    Groups all incorrect decisions by rule, ranks by frequency,
    and suggests threshold adjustments where possible.
    """
    decisions = logger.get_feature_decisions(feature=feature, lookback_days=lookback_days)

    # Identify incorrect decisions
    incorrect = []
    for d in decisions:
        decision_type = d["decision"]
        outcome = d["outcome"]
        counterfactual = d["counterfactual"]

        if decision_type in ("accept", "deploy") and outcome == "loss":
            incorrect.append(d)
        elif decision_type in ("reject", "hold") and counterfactual == "would_profit":
            incorrect.append(d)

    # Cluster by rule
    rule_counter = Counter(d["rule"] for d in incorrect if d["rule"])
    total_incorrect = len(incorrect)

    clusters = []
    for rule, count in rule_counter.most_common(5):
        pct = (count / total_incorrect * 100) if total_incorrect > 0 else 0
        clusters.append({"rule": rule, "count": count, "pct": round(pct, 1)})

    # Generate suggested fixes based on rule patterns
    fixes = _suggest_fixes(incorrect, clusters)

    # Get overall value score
    scores = logger.get_feature_value_scores(lookback_days)
    stats = scores.get(feature, {})

    return FeatureDiagnostic(
        feature=feature,
        value_score=stats.get("score"),
        total_decisions=stats.get("total", 0),
        failure_clusters=clusters,
        suggested_fixes=fixes,
    )


def _suggest_fixes(incorrect: list[dict], clusters: list[dict]) -> list[str]:
    """Generate actionable fix suggestions from failure clusters."""
    fixes = []

    for cluster in clusters[:3]:
        rule = cluster["rule"]
        count = cluster["count"]

        # Parse threshold-based rules
        if " < min " in rule or " > max " in rule:
            # e.g., "R:R 1.3 < min 1.5" → suggest lowering min
            parts = rule.split()
            try:
                actual_val = float(parts[1])
                threshold = float(parts[-1])
                metric_name = parts[0]

                if "< min" in rule:
                    new_threshold = round(actual_val * 0.9, 2)  # 10% below the typical failure
                    fixes.append(
                        f"Lower {metric_name} threshold from {threshold} to ~{new_threshold} "
                        f"({count} profitable rejects at current threshold)"
                    )
                elif "> max" in rule:
                    new_threshold = round(actual_val * 1.1, 2)
                    fixes.append(
                        f"Raise {metric_name} threshold from {threshold} to ~{new_threshold} "
                        f"({count} failures at current threshold)"
                    )
            except (ValueError, IndexError):
                pass

        elif "correlated:" in rule:
            fixes.append(
                f"Review correlation filter — {count} profitable setups blocked by overlap"
            )

        elif "moat=" in rule:
            fixes.append(
                f"Consider relaxing moat requirement — {count} profitable stocks rejected"
            )

        elif "no_dip" in rule:
            fixes.append(
                f"Consider lowering min_triggers for dip detection — {count} missed buying opportunities"
            )

        elif "comm" in rule.lower() or "commission" in rule.lower():
            fixes.append(
                f"Review commission threshold — {count} viable trades rejected"
            )

    if not fixes and incorrect:
        fixes.append(
            f"{len(incorrect)} incorrect decisions — review feature logic and thresholds"
        )

    return fixes


def get_summary_report(
    logger: TradeLogger, lookback_days: int = 90,
) -> tuple[list[FeatureValueScore], list[FeatureDiagnostic]]:
    """
    Generate the full health report: scores + diagnostics for underperformers.

    Returns (scores, diagnostics) where diagnostics only includes features
    with value_score < 50%.
    """
    scores = compute_value_scores(logger, lookback_days)
    diagnostics = []

    for score in scores:
        if score.score is not None and score.score < 50.0:
            diag = diagnose_feature(logger, score.feature, lookback_days)
            diagnostics.append(diag)

    return scores, diagnostics
