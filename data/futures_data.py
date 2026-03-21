"""
Futures Data Fetcher
--------------------
Historical data and position sizing for US index futures (ES, NQ, MES, MNQ).
Uses continuous contract (ContFuture) for seamless historical data.
"""
from ib_insync import IB, Future, ContFuture
import pandas as pd
from core.ib_rate_limiter import (
    throttled_qualify_contracts,
    throttled_req_historical_data,
    throttled_req_contract_details,
)


# Futures contract specifications
FUTURES_SPECS = {
    "ES": {
        "symbol": "ES",
        "exchange": "CME",
        "multiplier": 50,        # $50 per point
        "tick_size": 0.25,
        "description": "E-mini S&P 500",
    },
    "NQ": {
        "symbol": "NQ",
        "exchange": "CME",
        "multiplier": 20,        # $20 per point
        "tick_size": 0.25,
        "description": "E-mini NASDAQ 100",
    },
    "MES": {
        "symbol": "MES",
        "exchange": "CME",
        "multiplier": 5,         # $5 per point (micro)
        "tick_size": 0.25,
        "description": "Micro E-mini S&P 500",
    },
    "MNQ": {
        "symbol": "MNQ",
        "exchange": "CME",
        "multiplier": 2,         # $2 per point (micro)
        "tick_size": 0.25,
        "description": "Micro E-mini NASDAQ 100",
    },
    # Mini indices
    "YM": {
        "symbol": "YM",
        "exchange": "CBOT",
        "multiplier": 5,
        "tick_size": 1.0,
        "description": "E-mini Dow Jones",
    },
    "RTY": {
        "symbol": "RTY",
        "exchange": "CME",
        "multiplier": 50,
        "tick_size": 0.1,
        "description": "E-mini Russell 2000",
    },
    # Micro indices
    "MYM": {
        "symbol": "MYM",
        "exchange": "CBOT",
        "multiplier": 0.5,
        "tick_size": 1.0,
        "description": "Micro Dow Jones",
    },
    "M2K": {
        "symbol": "M2K",
        "exchange": "CME",
        "multiplier": 5,
        "tick_size": 0.1,
        "description": "Micro Russell 2000",
    },
    # Metals
    "GC": {
        "symbol": "GC",
        "exchange": "COMEX",
        "multiplier": 100,
        "tick_size": 0.10,
        "description": "Gold",
    },
    "MGC": {
        "symbol": "MGC",
        "exchange": "COMEX",
        "multiplier": 10,
        "tick_size": 0.10,
        "description": "Micro Gold",
    },
    "SI": {
        "symbol": "SI",
        "exchange": "COMEX",
        "multiplier": 5000,
        "tick_size": 0.005,
        "description": "Silver",
    },
    # Energy
    "CL": {
        "symbol": "CL",
        "exchange": "NYMEX",
        "multiplier": 1000,
        "tick_size": 0.01,
        "description": "Crude Oil",
    },
    "MCL": {
        "symbol": "MCL",
        "exchange": "NYMEX",
        "multiplier": 100,
        "tick_size": 0.01,
        "description": "Micro Crude Oil",
    },
    "NG": {
        "symbol": "NG",
        "exchange": "NYMEX",
        "multiplier": 10000,
        "tick_size": 0.001,
        "description": "Natural Gas",
    },
    # Bonds
    "ZB": {
        "symbol": "ZB",
        "exchange": "CBOT",
        "multiplier": 1000,
        "tick_size": 0.03125,    # 1/32
        "description": "30Y Treasury Bond",
    },
    "ZN": {
        "symbol": "ZN",
        "exchange": "CBOT",
        "multiplier": 1000,
        "tick_size": 0.015625,   # 1/64
        "description": "10Y Treasury Note",
    },
    "ZF": {
        "symbol": "ZF",
        "exchange": "CBOT",
        "multiplier": 1000,
        "tick_size": 0.0078125,  # 1/128
        "description": "5Y Treasury Note",
    },
    # Crypto
    "MBT": {
        "symbol": "MBT",
        "exchange": "CME",
        "multiplier": 5,
        "tick_size": 5.0,
        "description": "Micro Bitcoin",
    },
    "MET": {
        "symbol": "MET",
        "exchange": "CME",
        "multiplier": 0.1,
        "tick_size": 0.05,
        "description": "Micro Ether",
    },
}


def get_front_month_contract(ib: IB, symbol: str) -> Future:
    """Get the front-month (most liquid) futures contract for trading."""
    spec = FUTURES_SPECS.get(symbol.upper())
    if not spec:
        raise ValueError(f"Unknown futures symbol: {symbol}. Supported: {list(FUTURES_SPECS.keys())}")

    contract = Future(symbol=spec["symbol"], exchange=spec["exchange"])
    details = throttled_req_contract_details(ib, contract)
    if not details:
        raise RuntimeError(f"No contract details for {symbol}")

    # Sort by expiry, pick the nearest
    details.sort(key=lambda d: d.contract.lastTradeDateOrContractMonth)
    front = details[0].contract
    throttled_qualify_contracts(ib, front)
    return front


def fetch_futures_history(
    ib: IB,
    symbol: str,
    duration: str = "1 W",
    bar_size: str = "1 hour",
) -> pd.DataFrame:
    """Fetch historical OHLCV data for a futures contract."""
    spec = FUTURES_SPECS.get(symbol.upper())
    if not spec:
        raise ValueError(f"Unknown futures symbol: {symbol}")

    # Use continuous contract for clean historical data
    contract = ContFuture(symbol=spec["symbol"], exchange=spec["exchange"])
    throttled_qualify_contracts(ib, contract)

    bars = throttled_req_historical_data(
        ib, contract, duration, bar_size,
        what_to_show="TRADES", use_rth=False,
    )

    if not bars:
        raise RuntimeError(f"No historical data for {symbol}")

    df = pd.DataFrame([{
        "Datetime": b.date,
        "Open": b.open,
        "High": b.high,
        "Low": b.low,
        "Close": b.close,
        "Volume": b.volume,
    } for b in bars])
    df["Datetime"] = pd.to_datetime(df["Datetime"], utc=True)
    df.set_index("Datetime", inplace=True)
    df.dropna(inplace=True)
    print(f"[FuturesData] Fetched {len(df)} bars for {symbol} ({duration}, {bar_size})")
    return df


def calculate_futures_position_size(
    account_balance: float,
    risk_pct: float,
    stop_points: float,
    symbol: str,
) -> int:
    """Calculate number of contracts to trade based on risk."""
    spec = FUTURES_SPECS.get(symbol.upper())
    if not spec:
        raise ValueError(f"Unknown futures symbol: {symbol}")

    risk_usd = account_balance * risk_pct
    risk_per_contract = stop_points * spec["multiplier"]

    if risk_per_contract <= 0:
        return 0

    contracts = int(risk_usd / risk_per_contract)
    return max(1, contracts)
