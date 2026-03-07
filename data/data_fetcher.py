"""
Fetches historical OHLCV data for backtesting via yfinance.
Forex pairs use the Yahoo Finance format: GBPJPY=X, AUDJPY=X

CSV fallback: place a file at data/GBPJPY_1h.csv (or AUDJPY_1h.csv) with
columns: Datetime, Open, High, Low, Close, Volume
This is useful if Yahoo Finance is unavailable or you have your own data.
"""
from pathlib import Path

import pandas as pd
import yfinance as yf

# Yahoo Finance ticker suffixes for forex pairs
YAHOO_TICKERS = {
    "GBPJPY": "GBPJPY=X",
    "AUDJPY": "AUDJPY=X",
}

CSV_DIR = Path(__file__).parent


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


def fetch_historical(pair: str, period: str = "3y", interval: str = "1h") -> pd.DataFrame:
    """
    Fetch OHLCV data from Yahoo Finance.

    Args:
        pair: Pair name e.g. 'GBPJPY'
        period: Lookback period e.g. '3y', '1y', '6mo'
        interval: Candle interval e.g. '1h', '1d'

    Returns:
        DataFrame with columns: Open, High, Low, Close, Volume
        Index: DatetimeIndex (UTC)
    """
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
