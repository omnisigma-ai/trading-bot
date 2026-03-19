"""
London Breakout Strategy
------------------------
Logic:
1. At 5pm Sydney time, look back `asian_range_hours` of H1 candles
2. Calculate the High and Low of that range (Asian session consolidation)
3. Signal: BUY STOP entry 'pip_buffer' pips above range High
           SELL STOP entry 'pip_buffer' pips below range Low
4. Stop Loss:  BUY  → range Low  - pip_buffer pips
               SELL → range High + pip_buffer pips
5. Take Profit: SL distance × tp_multiplier
"""
from dataclasses import dataclass
from typing import Optional

import pandas as pd
import pytz

# Pip sizes per pair (0.01 for JPY pairs, 0.0001 for USD pairs)
PIP_SIZE = {
    "GBPJPY": 0.01,
    "AUDJPY": 0.01,
    "EURUSD": 0.0001,
    "GBPUSD": 0.0001,
    "AUDUSD": 0.0001,
    "NZDUSD": 0.0001,
    "USDJPY": 0.01,
    "USDCAD": 0.0001,
    "USDCHF": 0.0001,
    "EURJPY": 0.01,
}

# Price rounding decimals (3 for JPY pairs, 5 for USD pairs)
PRICE_DECIMALS = {pair: (3 if pip >= 0.01 else 5) for pair, pip in PIP_SIZE.items()}


@dataclass
class Signal:
    pair: str
    direction: str          # 'BUY' or 'SELL'
    entry: float            # stop-entry price
    stop_loss: float
    take_profit: float
    range_high: float
    range_low: float
    sl_pips: float
    tp_pips: float


def get_asian_range(
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    range_hours: int = 6,
) -> tuple[float, float]:
    """
    Calculate the Asian session range High and Low.

    Args:
        df: H1 OHLCV DataFrame with UTC DatetimeIndex
        as_of: The timestamp representing 'now' (5pm Sydney in UTC)
        range_hours: How many hours back to look

    Returns:
        (range_high, range_low)
    """
    window_start = as_of - pd.Timedelta(hours=range_hours)
    mask = (df.index >= window_start) & (df.index < as_of)
    window = df.loc[mask]

    if len(window) < range_hours // 2:
        raise ValueError(
            f"Not enough candles in Asian range window. "
            f"Found {len(window)}, expected ~{range_hours}."
        )

    return float(window["High"].max()), float(window["Low"].min())


def generate_signal(
    pair: str,
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    asian_range_hours: int = 6,
    pip_buffer: int = 5,
    tp_multiplier: float = 2.0,
) -> Optional[Signal]:
    """
    Generate a London Breakout signal for a given pair at a given time.

    Args:
        pair: e.g. 'GBPJPY'
        df: H1 OHLCV DataFrame (UTC index)
        as_of: Timestamp to evaluate signal (5pm Sydney in UTC)
        asian_range_hours: Hours to use for Asian range
        pip_buffer: Pips above/below range for entry & SL buffer
        tp_multiplier: TP = SL distance × multiplier

    Returns:
        Signal with both BUY and SELL levels, or None if range too tight
    """
    pip = PIP_SIZE.get(pair.upper())
    if pip is None:
        raise ValueError(f"Unknown pair: {pair}")

    buffer = pip_buffer * pip

    range_high, range_low = get_asian_range(df, as_of, asian_range_hours)
    range_size = range_high - range_low

    # Minimum viable range: at least 10 pips (avoid noise days)
    min_range = 10 * pip
    if range_size < min_range:
        return None

    # Both a BUY and SELL signal are returned — execution layer picks one
    # We return BUY here; the caller generates the mirror SELL separately
    buy_entry = range_high + buffer
    buy_sl = range_low - buffer
    buy_sl_pips = (buy_entry - buy_sl) / pip
    buy_tp = buy_entry + (buy_sl_pips * tp_multiplier * pip)

    dec = PRICE_DECIMALS.get(pair.upper(), 5)
    return Signal(
        pair=pair,
        direction="BUY",
        entry=round(buy_entry, dec),
        stop_loss=round(buy_sl, dec),
        take_profit=round(buy_tp, dec),
        range_high=round(range_high, dec),
        range_low=round(range_low, dec),
        sl_pips=round(buy_sl_pips, 1),
        tp_pips=round(buy_sl_pips * tp_multiplier, 1),
    )


def generate_both_signals(
    pair: str,
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    asian_range_hours: int = 6,
    pip_buffer: int = 5,
    tp_multiplier: float = 2.0,
) -> list[Signal]:
    """
    Returns both BUY STOP and SELL STOP signals for the breakout.
    Returns empty list if range is too tight.
    """
    pip = PIP_SIZE.get(pair.upper())
    if pip is None:
        raise ValueError(f"Unknown pair: {pair}")

    buffer = pip_buffer * pip
    min_range = 10 * pip

    range_high, range_low = get_asian_range(df, as_of, asian_range_hours)
    range_size = range_high - range_low

    if range_size < min_range:
        return []

    # BUY STOP
    buy_entry = range_high + buffer
    buy_sl = range_low - buffer
    buy_sl_pips = (buy_entry - buy_sl) / pip
    buy_tp = buy_entry + (buy_sl_pips * tp_multiplier * pip)

    # SELL STOP
    sell_entry = range_low - buffer
    sell_sl = range_high + buffer
    sell_sl_pips = (sell_sl - sell_entry) / pip
    sell_tp = sell_entry - (sell_sl_pips * tp_multiplier * pip)

    dec = PRICE_DECIMALS.get(pair.upper(), 5)
    return [
        Signal(
            pair=pair, direction="BUY",
            entry=round(buy_entry, dec), stop_loss=round(buy_sl, dec), take_profit=round(buy_tp, dec),
            range_high=round(range_high, dec), range_low=round(range_low, dec),
            sl_pips=round(buy_sl_pips, 1), tp_pips=round(buy_sl_pips * tp_multiplier, 1),
        ),
        Signal(
            pair=pair, direction="SELL",
            entry=round(sell_entry, dec), stop_loss=round(sell_sl, dec), take_profit=round(sell_tp, dec),
            range_high=round(range_high, dec), range_low=round(range_low, dec),
            sl_pips=round(sell_sl_pips, 1), tp_pips=round(sell_sl_pips * tp_multiplier, 1),
        ),
    ]


def sydney_5pm_as_utc(date: pd.Timestamp) -> pd.Timestamp:
    """Convert 5pm Sydney time on a given date to UTC (timezone-aware)."""
    sydney_tz = pytz.timezone("Australia/Sydney")
    naive = date.replace(hour=17, minute=0, second=0, microsecond=0, tzinfo=None)
    local = sydney_tz.localize(naive)
    return pd.Timestamp(local.astimezone(pytz.utc))
