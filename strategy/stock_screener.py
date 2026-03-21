"""
Stock Screener
--------------
Scans a universe of US stocks for breakout candidates.
Identifies setups where price is making new highs on above-average volume.
"""
import time
from dataclasses import dataclass

import pandas as pd
from ib_insync import IB

from data.stock_data import (
    fetch_stock_history,
    calculate_atr,
    calculate_volume_ratio,
    is_new_high,
    find_swing_low,
    calculate_rsi,
)


@dataclass
class StockCandidate:
    """A stock that passes the breakout screening criteria."""
    symbol: str
    current_price: float
    atr: float
    volume_ratio: float         # today's volume / 20-day average
    rsi: float
    days_since_breakout: int    # 0 = breaking out today
    swing_low: float            # recent swing low (for SL placement)
    risk_pct: float             # (entry - SL) / entry as percentage
    metadata: dict              # extra data for the opportunity scorer


def screen_universe(
    ib: IB,
    symbols: list[str],
    min_volume_ratio: float = 1.5,
    breakout_lookback: int = 20,
    max_risk_pct: float = 0.05,
) -> list[StockCandidate]:
    """
    Screen a list of stocks for momentum breakout candidates.

    Criteria:
    1. Price is at or near a 20-day high
    2. Volume is above 1.5x the 20-day average
    3. Risk (entry to swing low) is reasonable (< 5% of price)

    Args:
        ib: Connected IB instance
        symbols: List of stock tickers to scan
        min_volume_ratio: Minimum volume vs 20-day average
        breakout_lookback: N-day high lookback period
        max_risk_pct: Maximum acceptable risk as % of entry price

    Returns:
        List of StockCandidate objects sorted by volume ratio descending
    """
    candidates = []

    for i, symbol in enumerate(symbols):
        if i > 0:
            time.sleep(0.35)  # pace IB historical data requests
        try:
            df = fetch_stock_history(ib, symbol, duration="3 M", bar_size="1 day")
            if len(df) < breakout_lookback + 5:
                continue

            # Calculate indicators
            atr = calculate_atr(df, period=14)
            vol_ratio = calculate_volume_ratio(df, period=20)
            new_highs = is_new_high(df, lookback=breakout_lookback)
            rsi = calculate_rsi(df, period=14)

            latest = df.iloc[-1]
            latest_atr = float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else 0
            latest_vol_ratio = float(vol_ratio.iloc[-1]) if pd.notna(vol_ratio.iloc[-1]) else 0
            latest_rsi = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50
            at_new_high = bool(new_highs.iloc[-1]) if pd.notna(new_highs.iloc[-1]) else False

            # Filter: must be at new high with volume confirmation
            if not at_new_high:
                continue
            if latest_vol_ratio < min_volume_ratio:
                continue

            # Calculate SL (swing low of last 10 bars) and risk
            swing_low = find_swing_low(df, lookback=10)
            current_price = float(latest["Close"])
            risk_pct = (current_price - swing_low) / current_price if current_price > 0 else 1.0

            if risk_pct > max_risk_pct or risk_pct <= 0:
                continue

            # Count consecutive days at new high
            days_at_high = 0
            for i in range(len(new_highs) - 1, -1, -1):
                if new_highs.iloc[i]:
                    days_at_high += 1
                else:
                    break

            candidates.append(StockCandidate(
                symbol=symbol,
                current_price=current_price,
                atr=latest_atr,
                volume_ratio=round(latest_vol_ratio, 2),
                rsi=round(latest_rsi, 1),
                days_since_breakout=max(0, days_at_high - 1),
                swing_low=swing_low,
                risk_pct=round(risk_pct, 4),
                metadata={
                    "new_high_breakout": True,
                    "volume_ratio": latest_vol_ratio,
                    "rsi": latest_rsi,
                    "atr": latest_atr,
                    "days_at_high": days_at_high,
                    "trend_aligned": latest_rsi > 50,  # simple trend proxy
                },
            ))

        except Exception as e:
            print(f"[Screener] {symbol}: {e}")
            continue

    # Sort by volume ratio (strongest confirmation first)
    candidates.sort(key=lambda c: c.volume_ratio, reverse=True)
    return candidates
