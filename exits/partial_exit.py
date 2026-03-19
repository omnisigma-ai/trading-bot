"""
Partial Exit Manager
--------------------
Handles scaling out of positions at defined R:R milestones.
Example: close 50% at 1.5R, let remaining 50% trail.
"""
from dataclasses import dataclass


@dataclass
class PartialExitLevel:
    """Defines when and how much to close."""
    pct: float              # percentage of position to close (e.g., 50)
    at_rr: float            # R:R multiple to trigger (e.g., 1.5)
    action: str = "close"   # "close" or "reallocate" (flag for profit reallocation)
    triggered: bool = False


class PartialExitManager:
    """Evaluates whether partial exit levels have been reached."""

    def __init__(self, levels: list[dict]):
        self.levels = [
            PartialExitLevel(
                pct=lv.get("pct", 50),
                at_rr=lv.get("at_rr", 1.5),
                action=lv.get("action", "close"),
            )
            for lv in levels
        ]
        # Sort by trigger level ascending
        self.levels.sort(key=lambda x: x.at_rr)

    def check(
        self,
        current_price: float,
        entry_price: float,
        stop_loss: float,
        side: str,
    ) -> list[PartialExitLevel]:
        """
        Check if any partial exit levels have been triggered.

        Returns list of newly triggered levels (not previously triggered).
        """
        if side == "BUY":
            risk_distance = entry_price - stop_loss
            profit = current_price - entry_price
        else:
            risk_distance = stop_loss - entry_price
            profit = entry_price - current_price

        if risk_distance <= 0:
            return []

        current_rr = profit / risk_distance
        newly_triggered = []

        for level in self.levels:
            if not level.triggered and current_rr >= level.at_rr:
                level.triggered = True
                newly_triggered.append(level)

        return newly_triggered

    @property
    def total_closed_pct(self) -> float:
        """Total percentage of position that has been closed via partials."""
        return sum(lv.pct for lv in self.levels if lv.triggered)

    @property
    def remaining_pct(self) -> float:
        """Percentage of position still open."""
        return max(0, 100 - self.total_closed_pct)

    @property
    def has_pending_levels(self) -> bool:
        """True if there are still untriggered partial exit levels."""
        return any(not lv.triggered for lv in self.levels)
