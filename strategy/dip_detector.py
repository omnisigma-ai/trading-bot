"""
Dip Detector — Macro-Driven Deployment Timing
----------------------------------------------
Rule-based dip detection using macro indicators. Determines whether
accumulated reallocation funds should be deployed now or wait for
better conditions.

Designed for ML replacement: the detect_dip() interface stays the same,
just swap the internals when a trained model is ready.

Current rules (any 2+ triggers = dip):
  1. VIX > 25 (market fear)
  2. VIX 5-day change > +30% (fear spike)
  3. Gold 5-day change > +3% (flight to safety)
  4. Oil 5-day change < -10% (demand destruction)
  5. AUD/USD 5-day change < -2% (risk-off)
  6. DXY 5-day change > +2% (dollar strength = risk-off)
"""
from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class DipSignal:
    """Result of dip detection analysis."""
    is_dip: bool = False
    confidence: float = 0.0          # 0-1, proportion of triggers fired
    triggers: list[str] = field(default_factory=list)
    macro_snapshot: dict = field(default_factory=dict)
    changes: dict = field(default_factory=dict)
    timestamp: str = ""

    def summary(self) -> str:
        status = "DIP DETECTED" if self.is_dip else "NO DIP"
        triggers_str = ", ".join(self.triggers) if self.triggers else "none"
        return (
            f"{status} | Confidence: {self.confidence:.0%} | "
            f"Triggers: {triggers_str}"
        )


# Default thresholds (overridden by config)
DEFAULT_THRESHOLDS = {
    "vix_level": 25,
    "vix_5d_change_pct": 30,
    "gold_5d_change_pct": 3,
    "oil_5d_change_pct": -10,
    "aud_usd_5d_change_pct": -2,
    "dxy_5d_change_pct": 2,
    "min_triggers": 2,
}


def detect_dip(
    snapshot: dict[str, float | None],
    changes: dict[str, float | None],
    thresholds: dict = None,
) -> DipSignal:
    """
    Analyse current macro conditions for dip signals.

    Args:
        snapshot: Current macro values {vix: 28.5, gold: 2450, ...}
        changes: 5-day percentage changes {vix_5d_chg: 0.30, ...}
        thresholds: Override default thresholds from config

    Returns:
        DipSignal with detection result
    """
    t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}
    triggers = []
    total_checks = 0

    # 1. VIX absolute level
    vix = snapshot.get("vix")
    if vix is not None:
        total_checks += 1
        if vix > t["vix_level"]:
            triggers.append(f"vix_level ({vix:.1f} > {t['vix_level']})")

    # 2. VIX 5-day spike
    vix_chg = changes.get("vix_5d_chg")
    if vix_chg is not None:
        total_checks += 1
        threshold_pct = t["vix_5d_change_pct"] / 100
        if vix_chg > threshold_pct:
            triggers.append(f"vix_spike (+{vix_chg:.0%} > +{threshold_pct:.0%})")

    # 3. Gold 5-day rally (flight to safety)
    gold_chg = changes.get("gold_5d_chg")
    if gold_chg is not None:
        total_checks += 1
        threshold_pct = t["gold_5d_change_pct"] / 100
        if gold_chg > threshold_pct:
            triggers.append(f"gold_rally (+{gold_chg:.1%} > +{threshold_pct:.0%})")

    # 4. Oil 5-day crash (demand destruction)
    oil_chg = changes.get("oil_wti_5d_chg")
    if oil_chg is not None:
        total_checks += 1
        threshold_pct = t["oil_5d_change_pct"] / 100
        if oil_chg < threshold_pct:
            triggers.append(f"oil_crash ({oil_chg:.1%} < {threshold_pct:.0%})")

    # 5. AUD/USD 5-day drop (risk-off)
    aud_chg = changes.get("aud_usd_5d_chg")
    if aud_chg is not None:
        total_checks += 1
        threshold_pct = t["aud_usd_5d_change_pct"] / 100
        if aud_chg < threshold_pct:
            triggers.append(f"aud_drop ({aud_chg:.1%} < {threshold_pct:.0%})")

    # 6. DXY 5-day rally (dollar strength = risk-off)
    dxy_chg = changes.get("dxy_5d_chg")
    if dxy_chg is not None:
        total_checks += 1
        threshold_pct = t["dxy_5d_change_pct"] / 100
        if dxy_chg > threshold_pct:
            triggers.append(f"dxy_rally (+{dxy_chg:.1%} > +{threshold_pct:.0%})")

    min_triggers = t["min_triggers"]
    confidence = len(triggers) / total_checks if total_checks > 0 else 0.0

    return DipSignal(
        is_dip=len(triggers) >= min_triggers,
        confidence=round(confidence, 3),
        triggers=triggers,
        macro_snapshot=snapshot,
        changes=changes,
        timestamp=datetime.utcnow().isoformat(),
    )


def should_deploy(
    signal: DipSignal,
    days_since_last_deploy: int | None,
    max_wait_days: int = 30,
) -> bool:
    """
    Decide whether to deploy accumulated reallocation funds.

    Deploy if:
      1. Dip detected (2+ triggers), OR
      2. It's been max_wait_days since last deployment (don't hoard cash)

    Args:
        signal: DipSignal from detect_dip()
        days_since_last_deploy: Days since last capital deployment (None = never)
        max_wait_days: Force deploy after this many days without a dip
    """
    if signal.is_dip:
        return True

    if days_since_last_deploy is None:
        # Never deployed — wait for a dip unless it's been a while
        return False

    if days_since_last_deploy >= max_wait_days:
        return True

    return False
