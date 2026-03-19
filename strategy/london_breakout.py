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
6. Adaptive TP: if Asian range is compressed vs recent history, increase TP multiplier
"""
from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd
import pytz

from strategy.base import BaseStrategy, TradeIntent

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


def adaptive_tp_multiplier(
    df: pd.DataFrame,
    as_of: pd.Timestamp,
    base_multiplier: float = 2.0,
    lookback_days: int = 20,
    range_hours: int = 6,
) -> float:
    """
    Adjust TP multiplier based on Asian range compression.

    Tight range → expect larger breakout → use higher TP multiplier.
    - Below 25th percentile of recent ranges: 3:1 R:R
    - Below 10th percentile: 5:1 R:R
    - Normal: base_multiplier (default 2:1)
    """
    # Collect recent daily ranges for comparison
    ranges = []
    for day_offset in range(1, lookback_days + 1):
        ref_time = as_of - pd.Timedelta(days=day_offset)
        window_start = ref_time - pd.Timedelta(hours=range_hours)
        mask = (df.index >= window_start) & (df.index < ref_time)
        window = df.loc[mask]
        if len(window) >= range_hours // 2:
            day_range = float(window["High"].max()) - float(window["Low"].min())
            ranges.append(day_range)

    if len(ranges) < 5:
        return base_multiplier

    current_range_start = as_of - pd.Timedelta(hours=range_hours)
    mask = (df.index >= current_range_start) & (df.index < as_of)
    current_window = df.loc[mask]
    if len(current_window) < range_hours // 2:
        return base_multiplier

    current_range = float(current_window["High"].max()) - float(current_window["Low"].min())
    percentile = np.sum(np.array(ranges) <= current_range) / len(ranges) * 100

    if percentile <= 10:
        return 5.0
    elif percentile <= 25:
        return 3.0
    else:
        return base_multiplier


class LondonBreakoutStrategy(BaseStrategy):
    """London Breakout wrapped as a BaseStrategy for multi-strategy dispatch."""

    name = "london_breakout"

    def generate(
        self,
        config: dict,
        ib=None,
        account_balance: float = 0.0,
    ) -> list[TradeIntent]:
        from data.data_fetcher import fetch_historical
        from risk.position_sizer import calculate_lot_size

        strat_cfg = config.get("strategies", {}).get("london_breakout", config.get("strategy", {}))
        pairs = strat_cfg.get("pairs", config.get("pairs", []))
        risk_pct = strat_cfg.get("risk_per_trade", config.get("risk_per_trade", 0.01))
        exit_strategy = strat_cfg.get("exit_strategy", "fixed")
        use_adaptive_tp = strat_cfg.get("adaptive_tp", False)

        now_utc = pd.Timestamp.now("UTC")
        intents = []

        for pair in pairs:
            try:
                df = fetch_historical(pair, period="7d", interval="1h", ib=ib)

                tp_mult = strat_cfg.get("tp_multiplier", 2.0)
                if use_adaptive_tp:
                    tp_mult = adaptive_tp_multiplier(
                        df, now_utc,
                        base_multiplier=tp_mult,
                        lookback_days=20,
                        range_hours=strat_cfg.get("asian_range_hours", 6),
                    )

                signals = generate_both_signals(
                    pair=pair, df=df, as_of=now_utc,
                    asian_range_hours=strat_cfg.get("asian_range_hours", 6),
                    pip_buffer=strat_cfg.get("pip_buffer", 5),
                    tp_multiplier=tp_mult,
                )

                if not signals:
                    continue

                # Position sizing
                buy_sig = next(s for s in signals if s.direction == "BUY")
                quote_rate = 1.0 if pair.upper().endswith("USD") else 150.0
                lot_size = calculate_lot_size(
                    pair=pair, account_balance=account_balance,
                    risk_pct=risk_pct, sl_pips=buy_sig.sl_pips,
                    quote_per_usd=quote_rate,
                )
                risk_usd = account_balance * risk_pct

                for sig in signals:
                    pip = PIP_SIZE.get(pair.upper(), 0.0001)
                    slippage_guard = 5 * pip
                    if sig.direction == "BUY":
                        limit_price = sig.entry + slippage_guard
                    else:
                        limit_price = sig.entry - slippage_guard

                    intent = TradeIntent(
                        strategy=self.name,
                        instrument_type="forex",
                        symbol=pair,
                        direction=sig.direction,
                        entry_type="STOP_LIMIT",
                        entry_price=sig.entry,
                        stop_loss=sig.stop_loss,
                        take_profit=sig.take_profit,
                        risk_pips=sig.sl_pips,
                        risk_dollars=risk_usd,
                        quantity=lot_size,
                        exit_strategy=exit_strategy,
                        metadata={
                            "range_high": sig.range_high,
                            "range_low": sig.range_low,
                            "sl_pips": sig.sl_pips,
                            "tp_pips": sig.tp_pips,
                            "tp_multiplier": tp_mult,
                            "limit_price": limit_price,
                        },
                    )
                    intents.append(intent)

            except Exception as e:
                print(f"[LondonBreakout] {pair}: {e}")

        return intents

    def get_schedule(self, config: dict) -> list[dict]:
        strat_cfg = config.get("strategies", {}).get("london_breakout", {})
        schedule = strat_cfg.get("schedule", config.get("schedule", {}))
        tz = schedule.get("timezone", "Australia/Sydney")
        run_times = schedule.get("run_times", [schedule.get("time", "17:00")])
        entries = []
        for t in run_times:
            hour, minute = map(int, t.split(":"))
            entries.append({"hour": hour, "minute": minute, "timezone": tz})
        return entries
