"""
Fetches historical OHLCV data for the trading bot.

Data source priority (for live/paper trading):
  1. IB historical data via ib_insync (if an IB connection is passed)
  2. yfinance (Yahoo Finance)
  3. Local CSV fallback

For backtesting, use fetch_historical() without an IB connection.
"""
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf
from ib_insync import IB, Forex
from core.ib_rate_limiter import throttled_qualify_contracts, throttled_req_historical_data

# Yahoo Finance ticker suffixes for forex pairs
YAHOO_TICKERS = {
    "AUDUSD": "AUDUSD=X",
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "USDJPY=X",
    "USDCAD": "USDCAD=X",
    "USDCHF": "USDCHF=X",
    "NZDUSD": "NZDUSD=X",
    "GBPJPY": "GBPJPY=X",
    "AUDJPY": "AUDJPY=X",
    "EURJPY": "EURJPY=X",
    "EURGBP": "EURGBP=X",
    "EURAUD": "EURAUD=X",
    "GBPAUD": "GBPAUD=X",
    "EURCHF": "EURCHF=X",
    "CADJPY": "CADJPY=X",
    "CHFJPY": "CHFJPY=X",
    "NZDJPY": "NZDJPY=X",
}

# IB duration string mapping for common periods
_IB_DURATIONS = {
    "7d": "1 W",
    "14d": "2 W",
    "1mo": "1 M",
    "3mo": "3 M",
    "6mo": "6 M",
    "1y": "1 Y",
    "3y": "3 Y",
}

# IB bar size mapping
_IB_BAR_SIZES = {
    "1h": "1 hour",
    "1d": "1 day",
    "4h": "4 hours",
    "15m": "15 mins",
    "5m": "5 mins",
}

CSV_DIR = Path(__file__).parent


def fetch_from_ib(ib: IB, pair: str, period: str = "7d", interval: str = "1h") -> pd.DataFrame:
    """
    Fetch OHLCV data from IB TWS/Gateway.

    Args:
        ib: Connected ib_insync.IB instance
        pair: Pair name e.g. 'GBPJPY'
        period: Lookback period e.g. '7d', '1mo'
        interval: Candle interval e.g. '1h', '1d'

    Returns:
        DataFrame with columns: Open, High, Low, Close, Volume
        Index: DatetimeIndex (UTC)
    """
    symbol = pair[:3].upper()
    currency = pair[3:].upper()
    contract = Forex(pair=f"{symbol}{currency}")
    throttled_qualify_contracts(ib, contract)

    duration = _IB_DURATIONS.get(period, "1 W")
    bar_size = _IB_BAR_SIZES.get(interval, "1 hour")

    bars = throttled_req_historical_data(
        ib, contract, duration, bar_size,
        what_to_show="MIDPOINT", use_rth=False,
    )

    if not bars:
        raise RuntimeError(f"No IB historical data returned for {pair}")

    df = pd.DataFrame(
        [{"Datetime": b.date, "Open": b.open, "High": b.high,
          "Low": b.low, "Close": b.close, "Volume": b.volume} for b in bars]
    )
    df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
    df.set_index("Datetime", inplace=True)
    df.dropna(inplace=True)
    print(f"[Data] Fetched {len(df)} bars from IB for {pair} ({period}, {interval})")
    return df


def fetch_from_csv(pair: str, interval: str = "1h") -> pd.DataFrame:
    """
    Load OHLCV data from a local CSV file (data/{PAIR}_{interval}.csv).
    CSV must have columns: Datetime, Open, High, Low, Close, Volume
    """
    fname = CSV_DIR / f"{pair.upper()}_{interval}.csv"
    if not fname.exists():
        raise FileNotFoundError(
            f"CSV not found: {fname}\n"
            f"Download data and save as {fname} with columns: Datetime,Open,High,Low,Close,Volume"
        )
    df = pd.read_csv(fname, parse_dates=["Datetime"], index_col="Datetime")
    df.index = pd.to_datetime(df.index, utc=True)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.dropna(inplace=True)
    return df


def fetch_historical(pair: str, period: str = "3y", interval: str = "1h",
                     ib: IB | None = None) -> pd.DataFrame:
    """
    Fetch OHLCV data. Uses IB if a connection is provided, else yfinance.

    Args:
        pair: Pair name e.g. 'GBPJPY'
        period: Lookback period e.g. '7d', '3y'
        interval: Candle interval e.g. '1h', '1d'
        ib: Optional connected IB instance (used in live/paper mode)

    Returns:
        DataFrame with columns: Open, High, Low, Close, Volume
        Index: DatetimeIndex (UTC)
    """
    # Try IB first if connection is available
    if ib is not None and ib.isConnected():
        try:
            return fetch_from_ib(ib, pair, period, interval)
        except Exception as e:
            print(f"[Data] IB historical data failed for {pair}: {e} — falling back to yfinance")

    # Fall back to yfinance
    ticker = YAHOO_TICKERS.get(pair.upper())
    if not ticker:
        raise ValueError(f"Unsupported pair: {pair}. Supported: {list(YAHOO_TICKERS.keys())}")

    df = yf.download(ticker, period=period, interval=interval, auto_adjust=True, progress=False)

    if df.empty:
        raise RuntimeError(f"No data returned for {pair} ({ticker}). Check your internet connection.")

    # Flatten multi-level columns if present (yfinance sometimes returns them)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    df.index = pd.to_datetime(df.index, utc=True)
    df = df[["Open", "High", "Low", "Close", "Volume"]].copy()
    df.dropna(inplace=True)

    return df
