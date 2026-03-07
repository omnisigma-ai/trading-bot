"""
Trading Bot — Main Entry Point
--------------------------------
Runs the London Breakout strategy daily at 5pm Sydney time.
Connects to IB TWS (paper or live based on config).

Usage:
    python main.py              # start the live scheduler
    python main.py --once       # run the strategy once immediately (for testing)
"""
import argparse
import sys
from datetime import datetime

import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config.loader import load_config
from data.data_fetcher import fetch_historical
from execution.ib_trader import IBTrader
from logs.trade_logger import TradeLogger
from notifications import discord_notifier as discord
from risk.position_sizer import calculate_lot_size
from strategy.london_breakout import generate_both_signals, sydney_5pm_as_utc

import pandas as pd


def run_strategy(config: dict) -> None:
    """
    Full pipeline: fetch data → generate signals → size positions → place orders → log → notify.
    """
    mode = config["mode"]
    pairs = config["pairs"]
    risk_pct = config["risk_per_trade"]
    strategy_cfg = config["strategy"]
    ib_cfg = config["ib"]
    webhook_url = config["discord"]["webhook_url"]
    db_path = config.get("db_path", "logs/trades.db")

    print(f"\n[{datetime.now()}] Running strategy | Mode: {mode.upper()}")

    port = ib_cfg["paper_port"] if mode == "paper" else ib_cfg["live_port"]
    trader = IBTrader(host=ib_cfg["host"], port=port, client_id=ib_cfg["client_id"])
    logger = TradeLogger(db_path)

    try:
        trader.connect()
        account_balance = trader.get_account_balance()
        print(f"[IB] Account balance: ${account_balance:,.2f}")

        # Get current USD/JPY rate for position sizing (used for both pairs)
        try:
            usdjpy = trader.get_current_price("USDJPY")
        except Exception:
            usdjpy = 150.0  # fallback estimate
            print(f"[IB] Could not fetch USD/JPY, using fallback: {usdjpy}")

        daily_results = []
        now_utc = pd.Timestamp.now("UTC")

        for pair in pairs:
            print(f"\n[Strategy] Processing {pair}...")

            try:
                # Skip if already have an open position or pending orders for this pair
                open_positions = trader.get_open_positions()
                open_symbols = [p["symbol"] for p in open_positions]
                if pair[:3].upper() in open_symbols:
                    print(f"[Strategy] {pair}: Skipping — open position already exists")
                    discord.notify_error(webhook_url, f"{pair}: Skipped — open position already exists")
                    daily_results.append({"pair": pair, "result": "SKIPPED", "pips": 0, "pnl_usd": 0})
                    continue

                # Fetch recent H1 data (7 days is enough for signal generation)
                df = fetch_historical(pair, period="7d", interval="1h")

                signals = generate_both_signals(
                    pair=pair,
                    df=df,
                    as_of=now_utc,
                    asian_range_hours=strategy_cfg["asian_range_hours"],
                    pip_buffer=strategy_cfg["pip_buffer"],
                    tp_multiplier=strategy_cfg["tp_multiplier"],
                )

                if not signals:
                    print(f"[Strategy] {pair}: No signal (range too tight)")
                    discord.notify_no_signal(webhook_url, pair, "range too tight")
                    daily_results.append({"pair": pair, "result": "NO_SIGNAL", "pips": 0, "pnl_usd": 0})
                    continue

                # Place BUY STOP and SELL STOP as an OCA group —
                # IB cancels the unfilled side when the other triggers
                buy_signal = next(s for s in signals if s.direction == "BUY")
                sell_signal = next(s for s in signals if s.direction == "SELL")

                lot_size = calculate_lot_size(
                    pair=pair,
                    account_balance=account_balance,
                    risk_pct=risk_pct,
                    sl_pips=buy_signal.sl_pips,  # symmetric for both sides
                    quote_per_usd=usdjpy,
                )
                risk_usd = account_balance * risk_pct

                buy_id, sell_id = trader.place_oca_breakout(
                    buy_signal=buy_signal,
                    sell_signal=sell_signal,
                    lot_size=lot_size,
                    expire_hours=strategy_cfg["max_trade_duration_hours"],
                )

                # Log both pending orders
                for signal in [buy_signal, sell_signal]:
                    logger.log_trade_opened(
                        pair=pair,
                        direction=signal.direction,
                        entry_price=signal.entry,
                        stop_loss=signal.stop_loss,
                        take_profit=signal.take_profit,
                        lot_size=lot_size,
                        sl_pips=signal.sl_pips,
                        tp_pips=signal.tp_pips,
                        ib_order_id=buy_id if signal.direction == "BUY" else sell_id,
                    )
                    discord.notify_order_placed(
                        webhook_url=webhook_url,
                        pair=pair,
                        direction=signal.direction,
                        entry=signal.entry,
                        sl=signal.stop_loss,
                        tp=signal.take_profit,
                        sl_pips=signal.sl_pips,
                        tp_pips=signal.tp_pips,
                        lot_size=lot_size,
                        risk_usd=risk_usd,
                    )

                daily_results.append({"pair": pair, "result": "PENDING", "pips": 0, "pnl_usd": 0})

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")
                discord.notify_error(webhook_url, f"{pair}: `{e}`")

        # Send daily summary
        discord.notify_daily_summary(webhook_url, daily_results)

    except Exception as e:
        print(f"[FATAL] Strategy run failed: {e}")
        discord.notify_error(webhook_url, f"FATAL: `{e}`", fatal=True)
    finally:
        trader.disconnect()
        logger.close()


def main():
    parser = argparse.ArgumentParser(description="London Breakout Trading Bot")
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run strategy once immediately instead of scheduling",
    )
    args = parser.parse_args()

    config = load_config()
    schedule_cfg = config["schedule"]

    if args.once:
        print("[Bot] Running strategy once (--once flag)...")
        run_strategy(config)
        return

    # Parse schedule time e.g. "17:00"
    hour, minute = map(int, schedule_cfg["time"].split(":"))
    timezone = schedule_cfg["timezone"]

    scheduler = BlockingScheduler(timezone=pytz.timezone(timezone))
    scheduler.add_job(
        run_strategy,
        trigger=CronTrigger(hour=hour, minute=minute, timezone=timezone),
        args=[config],
        name="london_breakout",
    )

    print(f"[Bot] Scheduler started — running daily at {schedule_cfg['time']} {timezone}")
    print(f"[Bot] Mode: {config['mode'].upper()} | Pairs: {', '.join(config['pairs'])}")
    print("[Bot] Press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n[Bot] Stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
