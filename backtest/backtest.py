"""
London Breakout Backtester
---------------------------
Simulates the London Breakout strategy on historical H1 data.
Outputs: win rate, profit factor, max drawdown, Sharpe ratio, equity curve.

Usage:
    python -m backtest.backtest --pair GBPJPY --range-hours 6 --tp-mult 2.0
"""
import argparse
from pathlib import Path

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # non-interactive backend for servers
import matplotlib.pyplot as plt

from data.data_fetcher import fetch_historical
from strategy.london_breakout import PIP_SIZE, sydney_5pm_as_utc

RESULTS_DIR = Path("backtest/results")


def run_backtest(
    pair: str,
    asian_range_hours: int = 6,
    pip_buffer: int = 5,
    tp_multiplier: float = 2.0,
    risk_pct: float = 0.01,
    starting_balance: float = 10_000.0,
    period: str = "3y",
) -> dict:
    """
    Run a full backtest. Returns a dict of performance stats.
    """
    print(f"\n{'='*60}")
    print(f"  Backtesting {pair} | Range: {asian_range_hours}h | TP: {tp_multiplier}x")
    print(f"{'='*60}")

    df = fetch_historical(pair, period=period, interval="1h")
    pip = PIP_SIZE[pair.upper()]
    buffer = pip_buffer * pip
    min_range = 10 * pip

    trades = []
    balance = starting_balance
    equity_curve = [balance]

    # Get all unique trading days in the data
    df_utc = df.copy()
    df_utc.index = pd.to_datetime(df_utc.index, utc=True)

    # Get all 5pm Sydney times in UTC that exist in the dataset
    unique_dates = pd.to_datetime(df_utc.index.date).unique()

    for date in unique_dates:
        date_ts = pd.Timestamp(date)
        try:
            signal_time_utc = pd.Timestamp(sydney_5pm_as_utc(date_ts), tz="UTC")
        except Exception:
            continue

        # Build the Asian range window
        window_start = signal_time_utc - pd.Timedelta(hours=asian_range_hours)
        mask = (df_utc.index >= window_start) & (df_utc.index < signal_time_utc)
        window = df_utc.loc[mask]

        if len(window) < asian_range_hours // 2:
            continue

        range_high = float(window["High"].max())
        range_low = float(window["Low"].min())
        range_size = range_high - range_low

        if range_size < min_range:
            continue

        # Define breakout levels
        buy_entry = range_high + buffer
        buy_sl = range_low - buffer
        buy_sl_pips = (buy_entry - buy_sl) / pip
        buy_tp = buy_entry + (buy_sl_pips * tp_multiplier * pip)

        sell_entry = range_low - buffer
        sell_sl = range_high + buffer
        sell_sl_pips = (sell_sl - sell_entry) / pip
        sell_tp = sell_entry - (sell_sl_pips * tp_multiplier * pip)

        # Simulate on candles after signal_time for up to 6 hours
        sim_end = signal_time_utc + pd.Timedelta(hours=6)
        forward = df_utc.loc[
            (df_utc.index >= signal_time_utc) & (df_utc.index < sim_end)
        ]

        trade_taken = False
        for _, candle in forward.iterrows():
            if trade_taken:
                break

            # Check BUY STOP trigger
            if candle["High"] >= buy_entry:
                # Pass candles AFTER the trigger candle to avoid look-ahead bias
                next_candles = forward.loc[forward.index > candle.name]
                result, exit_price, result_pips = _simulate_trade(
                    "BUY", buy_entry, buy_sl, buy_tp, pip, next_candles
                )
                risk_amount = balance * risk_pct
                pnl = _calc_pnl(result, result_pips, buy_sl_pips, risk_amount)
                balance += pnl
                trades.append({
                    "date": date, "pair": pair, "direction": "BUY",
                    "result": result, "pips": result_pips, "pnl": pnl,
                    "balance": balance,
                })
                equity_curve.append(balance)
                trade_taken = True

            # Check SELL STOP trigger
            elif candle["Low"] <= sell_entry:
                next_candles = forward.loc[forward.index > candle.name]
                result, exit_price, result_pips = _simulate_trade(
                    "SELL", sell_entry, sell_sl, sell_tp, pip, next_candles
                )
                risk_amount = balance * risk_pct
                pnl = _calc_pnl(result, result_pips, sell_sl_pips, risk_amount)
                balance += pnl
                trades.append({
                    "date": date, "pair": pair, "direction": "SELL",
                    "result": result, "pips": result_pips, "pnl": pnl,
                    "balance": balance,
                })
                equity_curve.append(balance)
                trade_taken = True

    return _compute_stats(trades, equity_curve, starting_balance, pair,
                          asian_range_hours, tp_multiplier)


def _calc_pnl(result: str, result_pips: float, sl_pips: float, risk_amount: float) -> float:
    """Calculate P&L correctly for TP, SL, and TIME_EXIT outcomes."""
    if result == "TP":
        return risk_amount * (result_pips / sl_pips)
    elif result == "SL":
        return -risk_amount
    else:  # TIME_EXIT — partial win or loss based on actual pip movement
        return risk_amount * (result_pips / sl_pips)


def _simulate_trade(
    direction: str,
    entry: float,
    sl: float,
    tp: float,
    pip: float,
    forward: pd.DataFrame,
) -> tuple[str, float, float]:
    """
    Walk forward candle by candle to determine TP, SL, or TIME_EXIT.
    Returns (result, exit_price, pips).
    """
    for _, candle in forward.iterrows():
        if direction == "BUY":
            if candle["Low"] <= sl:
                return "SL", sl, (entry - sl) / pip
            if candle["High"] >= tp:
                return "TP", tp, (tp - entry) / pip
        else:  # SELL
            if candle["High"] >= sl:
                return "SL", sl, (sl - entry) / pip
            if candle["Low"] <= tp:
                return "TP", tp, (entry - tp) / pip

    # Time-based exit at last candle close
    last_close = float(forward.iloc[-1]["Close"])
    if direction == "BUY":
        pips = (last_close - entry) / pip
    else:
        pips = (entry - last_close) / pip
    return "TIME_EXIT", last_close, pips


def _compute_stats(
    trades: list[dict],
    equity_curve: list[float],
    starting_balance: float,
    pair: str,
    range_hours: int,
    tp_multiplier: float,
) -> dict:
    if not trades:
        print("No trades generated.")
        return {}

    df = pd.DataFrame(trades)
    total = len(df)
    wins = len(df[df["result"] == "TP"])
    losses = len(df[df["result"] == "SL"])
    win_rate = wins / total * 100

    gross_profit = df[df["pnl"] > 0]["pnl"].sum()
    gross_loss = abs(df[df["pnl"] < 0]["pnl"].sum())
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")
    net_pnl = df["pnl"].sum()
    final_balance = starting_balance + net_pnl

    # Max drawdown
    eq = pd.Series(equity_curve)
    rolling_max = eq.cummax()
    drawdown = (eq - rolling_max) / rolling_max * 100
    max_drawdown = drawdown.min()

    # Sharpe (simplified, daily returns)
    daily_returns = df.groupby("date")["pnl"].sum() / starting_balance
    sharpe = (daily_returns.mean() / daily_returns.std() * (252 ** 0.5)) if daily_returns.std() > 0 else 0

    stats = {
        "pair": pair,
        "range_hours": range_hours,
        "tp_multiplier": tp_multiplier,
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "win_rate_pct": round(win_rate, 1),
        "profit_factor": round(profit_factor, 2),
        "net_pnl": round(net_pnl, 2),
        "final_balance": round(final_balance, 2),
        "max_drawdown_pct": round(max_drawdown, 2),
        "sharpe_ratio": round(sharpe, 2),
    }

    _print_stats(stats)
    _save_equity_chart(equity_curve, pair, range_hours, tp_multiplier)
    return stats


def _print_stats(s: dict) -> None:
    print(f"\n  Pair:            {s['pair']}")
    print(f"  Total trades:    {s['total_trades']}")
    print(f"  Win rate:        {s['win_rate_pct']}%")
    print(f"  Profit factor:   {s['profit_factor']}")
    print(f"  Net P&L:         ${s['net_pnl']:,.2f}")
    print(f"  Final balance:   ${s['final_balance']:,.2f}")
    print(f"  Max drawdown:    {s['max_drawdown_pct']}%")
    print(f"  Sharpe ratio:    {s['sharpe_ratio']}")


def _save_equity_chart(equity: list[float], pair: str, range_hours: int, tp_mult: float) -> None:
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(12, 5))
    ax.plot(equity, color="#00b4d8", linewidth=1.5)
    ax.fill_between(range(len(equity)), equity, equity[0], alpha=0.15, color="#00b4d8")
    ax.set_title(f"{pair} — London Breakout Equity Curve (range={range_hours}h, TP={tp_mult}x)")
    ax.set_xlabel("Trade #")
    ax.set_ylabel("Account Balance ($)")
    ax.grid(True, alpha=0.3)
    fname = RESULTS_DIR / f"{pair}_range{range_hours}h_tp{tp_mult}x.png"
    fig.savefig(fname, dpi=120, bbox_inches="tight")
    plt.close(fig)
    print(f"\n  Equity curve saved → {fname}")


def run_optimisation(pair: str) -> None:
    """Grid search over range hours and TP multiplier."""
    print(f"\n{'='*60}")
    print(f"  OPTIMISATION GRID — {pair}")
    print(f"{'='*60}")
    results = []
    for hours in [4, 6, 8]:
        for tp_mult in [1.5, 2.0, 2.5, 3.0]:
            stats = run_backtest(pair, asian_range_hours=hours, tp_multiplier=tp_mult)
            if stats:
                results.append(stats)

    if not results:
        return

    df = pd.DataFrame(results)
    df = df.sort_values("profit_factor", ascending=False)
    print(f"\n{'='*60}")
    print("  TOP RESULTS BY PROFIT FACTOR:")
    print(df[["range_hours", "tp_multiplier", "win_rate_pct",
              "profit_factor", "net_pnl", "max_drawdown_pct"]].to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="London Breakout Backtester")
    parser.add_argument("--pair", default="GBPJPY", help="Pair e.g. GBPJPY")
    parser.add_argument("--range-hours", type=int, default=6)
    parser.add_argument("--tp-mult", type=float, default=2.0)
    parser.add_argument("--optimise", action="store_true", help="Run grid optimisation")
    args = parser.parse_args()

    if args.optimise:
        run_optimisation(args.pair)
    else:
        run_backtest(args.pair, asian_range_hours=args.range_hours, tp_multiplier=args.tp_mult)
