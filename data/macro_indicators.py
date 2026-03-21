"""
Macro Indicator Fetcher
-----------------------
Fetches daily macro data via yfinance for dip detection.
All tickers are free and don't require authentication.

Indicators:
  VIX        — CBOE Volatility Index (fear gauge)
  US 10Y     — US Treasury 10Y yield (monetary policy)
  Gold       — Gold futures (safe haven demand)
  Oil (WTI)  — Crude oil futures (demand/supply shock)
  AUD/USD    — Australian dollar (risk sentiment proxy)
  DXY        — US Dollar Index (global risk-off)
"""
import yfinance as yf
import pandas as pd


# yfinance ticker symbols for each macro indicator
MACRO_TICKERS = {
    "vix": "^VIX",
    "us_10y_yield": "^TNX",
    "gold": "GC=F",
    "oil_wti": "CL=F",
    "aud_usd": "AUDUSD=X",
    "dxy": "DX-Y.NYB",
}


def fetch_macro_snapshot() -> dict[str, float | None]:
    """
    Fetch current values for all macro indicators.
    Returns {indicator_name: current_value} with None for failed fetches.
    """
    snapshot = {}
    for name, ticker in MACRO_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period="5d")
            if hist.empty:
                snapshot[name] = None
                continue
            snapshot[name] = float(hist["Close"].iloc[-1])
        except Exception as e:
            print(f"[Macro] {name} ({ticker}): fetch failed — {e}")
            snapshot[name] = None

    return snapshot


def fetch_macro_history(days: int = 60) -> pd.DataFrame:
    """
    Fetch daily closing prices for all macro indicators over the past N days.
    Returns DataFrame with columns matching MACRO_TICKERS keys, indexed by date.
    """
    period = f"{days}d"
    frames = {}

    for name, ticker in MACRO_TICKERS.items():
        try:
            t = yf.Ticker(ticker)
            hist = t.history(period=period)
            if not hist.empty:
                frames[name] = hist["Close"]
        except Exception as e:
            print(f"[Macro] {name} ({ticker}): history fetch failed — {e}")

    if not frames:
        return pd.DataFrame()

    df = pd.DataFrame(frames)
    df.index = df.index.tz_localize(None)  # remove timezone for consistency
    return df


def compute_changes(snapshot: dict, history: pd.DataFrame, lookback: int = 5) -> dict[str, float | None]:
    """
    Compute percentage changes from N days ago for each indicator.
    Returns {indicator_5d_chg: pct_change} (e.g., vix_5d_chg: 0.15 means +15%).
    """
    changes = {}

    if history.empty or len(history) < lookback:
        for name in MACRO_TICKERS:
            changes[f"{name}_5d_chg"] = None
        return changes

    for name in MACRO_TICKERS:
        current = snapshot.get(name)
        if current is None or name not in history.columns:
            changes[f"{name}_5d_chg"] = None
            continue

        # Get value from lookback days ago
        past_values = history[name].dropna()
        if len(past_values) < lookback:
            changes[f"{name}_5d_chg"] = None
            continue

        past_val = float(past_values.iloc[-lookback])
        if past_val == 0:
            changes[f"{name}_5d_chg"] = None
            continue

        pct_change = (current - past_val) / past_val
        changes[f"{name}_5d_chg"] = round(pct_change, 4)

    return changes
