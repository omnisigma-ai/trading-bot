"""
Trailing Stop Implementations
------------------------------
Multiple trailing stop strategies that adaptively move the SL
to lock in profit as price advances.
"""
from abc import ABC, abstractmethod

import pandas as pd


class TrailingStop(ABC):
    """Base class for trailing stop calculations."""

    @abstractmethod
    def calculate_stop(
        self,
        current_price: float,
        side: str,
        entry_price: float,
        current_stop: float,
        history: pd.DataFrame | None = None,
    ) -> float:
        """
        Calculate the new stop level.

        Returns the new stop price. The caller ensures the stop only
        moves forward (tighter) — never backward.

        Args:
            current_price: Current market price
            side: "BUY" (long) or "SELL" (short)
            entry_price: Original entry price
            current_stop: Current stop loss level
            history: Recent OHLCV data for ATR-based calculations
        """
        ...


class FixedPipTrail(TrailingStop):
    """Stop follows price by a fixed number of pips."""

    def __init__(self, trail_pips: float, pip_size: float = 0.0001):
        self.trail_distance = trail_pips * pip_size

    def calculate_stop(self, current_price, side, entry_price, current_stop, history=None):
        if side == "BUY":
            new_stop = current_price - self.trail_distance
        else:
            new_stop = current_price + self.trail_distance
        return new_stop


class ATRTrail(TrailingStop):
    """Stop based on ATR (Average True Range) — adapts to volatility."""

    def __init__(self, atr_period: int = 14, atr_multiplier: float = 2.0):
        self.atr_period = atr_period
        self.atr_multiplier = atr_multiplier

    def calculate_stop(self, current_price, side, entry_price, current_stop, history=None):
        if history is None or len(history) < self.atr_period:
            return current_stop  # not enough data, keep current stop

        atr = self._calculate_atr(history)
        trail_distance = atr * self.atr_multiplier

        if side == "BUY":
            new_stop = current_price - trail_distance
        else:
            new_stop = current_price + trail_distance
        return new_stop

    def _calculate_atr(self, df: pd.DataFrame) -> float:
        """Calculate Average True Range from OHLCV data."""
        high = df["High"].values[-self.atr_period:]
        low = df["Low"].values[-self.atr_period:]
        close = df["Close"].values[-self.atr_period:]

        tr_values = []
        for i in range(len(high)):
            if i == 0:
                tr = high[i] - low[i]
            else:
                tr = max(
                    high[i] - low[i],
                    abs(high[i] - close[i - 1]),
                    abs(low[i] - close[i - 1]),
                )
            tr_values.append(tr)

        return sum(tr_values) / len(tr_values)


class ChandelierTrail(TrailingStop):
    """
    Chandelier Exit — trails from the highest high (longs) or lowest low (shorts).
    Standard for stock breakouts.
    """

    def __init__(self, lookback: int = 22, atr_period: int = 14, atr_multiplier: float = 3.0):
        self.lookback = lookback
        self.atr_trail = ATRTrail(atr_period, atr_multiplier)

    def calculate_stop(self, current_price, side, entry_price, current_stop, history=None):
        if history is None or len(history) < self.lookback:
            return current_stop

        recent = history.tail(self.lookback)
        atr = self.atr_trail._calculate_atr(history)
        trail_distance = atr * self.atr_trail.atr_multiplier

        if side == "BUY":
            highest_high = float(recent["High"].max())
            new_stop = highest_high - trail_distance
        else:
            lowest_low = float(recent["Low"].min())
            new_stop = lowest_low + trail_distance

        return new_stop


class StepTrail(TrailingStop):
    """
    Step trailing stop — only advances at discrete R:R levels.
    E.g., at 1R profit move to breakeven, at 2R move to 1R, etc.
    Uses the original stop (set on first call) to calculate R-multiples,
    so that breakeven moves don't reset the risk calculation.
    """

    def __init__(self, step_size_r: float = 1.0, pip_size: float = 0.0001):
        self.step_size_r = step_size_r
        self.pip_size = pip_size
        self._original_risk: float | None = None

    def calculate_stop(self, current_price, side, entry_price, current_stop, history=None):
        # Calculate and cache initial risk distance on first call
        if self._original_risk is None:
            if side == "BUY":
                self._original_risk = entry_price - current_stop
            else:
                self._original_risk = current_stop - entry_price
            if self._original_risk <= 0:
                return current_stop

        initial_risk = self._original_risk
        if side == "BUY":
            current_profit = current_price - entry_price
        else:
            current_profit = entry_price - current_price

        # How many R-multiples of profit do we have?
        r_multiple = current_profit / initial_risk

        # Step the stop: at N*step_size R, move stop to (N-1)*step_size R
        steps_earned = int(r_multiple / self.step_size_r)
        if steps_earned < 1:
            return current_stop  # haven't earned a step yet

        # Move stop to (steps_earned - 1) × risk distance from entry
        stop_offset = (steps_earned - 1) * self.step_size_r * initial_risk

        if side == "BUY":
            new_stop = entry_price + stop_offset
        else:
            new_stop = entry_price - stop_offset

        return new_stop


def create_trailing_stop(config: dict, pip_size: float = 0.0001) -> TrailingStop:
    """Factory to create a trailing stop from config."""
    trail_type = config.get("type", "fixed_pip")

    if trail_type == "fixed_pip":
        return FixedPipTrail(
            trail_pips=config.get("trail_pips", 20),
            pip_size=pip_size,
        )
    elif trail_type == "atr":
        return ATRTrail(
            atr_period=config.get("atr_period", 14),
            atr_multiplier=config.get("atr_multiplier", 2.0),
        )
    elif trail_type == "chandelier":
        return ChandelierTrail(
            lookback=config.get("lookback", 22),
            atr_period=config.get("atr_period", 14),
            atr_multiplier=config.get("atr_multiplier", 3.0),
        )
    elif trail_type == "step":
        return StepTrail(
            step_size_r=config.get("step_size_r", 1.0),
            pip_size=pip_size,
        )
    else:
        raise ValueError(f"Unknown trailing stop type: {trail_type}")
