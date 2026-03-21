"""
Stock Data Fetcher
------------------
Historical data and technical indicators for US stocks.
Uses IB as primary source, yfinance as fallback.
"""
import pandas as pd
import numpy as np
from ib_insync import IB, Stock
from core.ib_rate_limiter import (
    throttled_qualify_contracts,
    throttled_req_historical_data,
    throttled_req_tickers,
)


def fetch_stock_history(
    ib: IB,
    symbol: str,
    duration: str = "3 M",
    bar_size: str = "1 day",
) -> pd.DataFrame:
    """
    Fetch historical OHLCV data for a US stock from IB.

    Args:
        ib: Connected IB instance
        symbol: Stock ticker e.g. "AAPL"
        duration: IB duration string e.g. "3 M", "1 Y"
        bar_size: IB bar size e.g. "1 day", "1 hour"

    Returns:
        DataFrame with Open, High, Low, Close, Volume columns, UTC DatetimeIndex
    """
    contract = Stock(symbol=symbol, exchange="SMART", currency="USD")
    throttled_qualify_contracts(ib, contract)

    bars = throttled_req_historical_data(
        ib, contract, duration, bar_size,
        what_to_show="TRADES", use_rth=True,
    )

    if not bars:
        raise RuntimeError(f"No IB historical data for {symbol}")

    df = pd.DataFrame(
        [{"Datetime": b.date, "Open": b.open, "High": b.high,
          "Low": b.low, "Close": b.close, "Volume": b.volume} for b in bars]
    )
    df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
    df.set_index("Datetime", inplace=True)
    df.dropna(inplace=True)
    return df


def calculate_atr(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Average True Range indicator."""
    high = df["High"]
    low = df["Low"]
    close = df["Close"].shift(1)

    tr = pd.concat([
        high - low,
        (high - close).abs(),
        (low - close).abs(),
    ], axis=1).max(axis=1)

    return tr.rolling(window=period).mean()


def calculate_volume_ratio(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Current volume relative to N-day average."""
    avg_vol = df["Volume"].rolling(window=period).mean()
    return df["Volume"] / avg_vol


def is_new_high(df: pd.DataFrame, lookback: int = 20) -> pd.Series:
    """True if close is at a new N-day high."""
    rolling_high = df["High"].rolling(window=lookback).max()
    return df["Close"] >= rolling_high


def find_swing_low(df: pd.DataFrame, lookback: int = 10) -> float:
    """Find the recent swing low (lowest low in last N bars)."""
    return float(df["Low"].tail(lookback).min())


def find_swing_high(df: pd.DataFrame, lookback: int = 10) -> float:
    """Find the recent swing high (highest high in last N bars)."""
    return float(df["High"].tail(lookback).max())


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """Relative Strength Index."""
    delta = df["Close"].diff()
    gain = delta.where(delta > 0, 0.0).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))


def get_stock_price(ib: IB, symbol: str) -> float:
    """Get current mid price for a stock."""
    contract = Stock(symbol=symbol, exchange="SMART", currency="USD")
    throttled_qualify_contracts(ib, contract)
    tickers = throttled_req_tickers(ib, contract)
    if not tickers:
        raise RuntimeError(f"No ticker data for {symbol}")
    mid = tickers[0].midpoint()
    if mid != mid:  # NaN
        # Fall back to last price
        mid = tickers[0].last
        if mid != mid:
            raise RuntimeError(f"No valid price for {symbol}")
    return float(mid)
