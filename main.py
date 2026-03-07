"""
Trading Bot — Main Entry Point
--------------------------------
Runs the London Breakout strategy daily at 5pm Sydney time.
Connects to IB TWS (paper or live based on config).

Flow per session:
  1. Connect to IB TWS
  2. Fetch H1 data + generate Asian range signals
  3. Place OCA breakout orders (BUY STOP + SELL STOP with bracket SL/TP)
  4. Start TradeMonitor — subscribes to IB fill/cancel events
  5. Keep connection alive for max_trade_duration_hours
  6. Monitor fires Discord alerts + updates SQLite on every fill/expiry
  7. Send daily summary, disconnect

Usage:
    python main.py              # start the daily scheduler
    python main.py --once       # run once immediately (for testing)
"""
import argparse
import sys
from datetime import datetime

import pandas as pd
import pytz
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from config.loader import load_config
from data.data_fetcher import fetch_historical
from execution.ib_trader import IBTrader
from execution.trade_monitor import TradeMonitor
from logs.trade_logger import TradeLogger
from notifications import discord_notifier as discord
from risk.position_sizer import calculate_lot_size
from strategy.london_breakout import generate_both_signals


def run_strategy(config: dict) -> None:
    """
    Full pipeline: data → signals → orders → monitor → alerts → journal.
    Keeps IB connection alive for the full session duration.
    """
    mode = config["mode"]
    pairs = config["pairs"]
    risk_pct = config["risk_per_trade"]
    strategy_cfg = config["strategy"]
    ib_cfg = config["ib"]
    webhook_url = config["discord"]["webhook_url"]
    db_path = config.get("db_path", "logs/trades.db")
    session_hours = strategy_cfg["max_trade_duration_hours"]

    print(f"\n[{datetime.now()}] Running strategy | Mode: {mode.upper()}")

    port = ib_cfg["paper_port"] if mode == "paper" else ib_cfg["live_port"]
    trader = IBTrader(host=ib_cfg["host"], port=port, client_id=ib_cfg["client_id"])
    logger = TradeLogger(db_path)
    order_groups = []

    try:
        trader.connect()
        account_balance = trader.get_account_balance()
        print(f"[IB] Account balance: ${account_balance:,.2f}")

        # Fetch USD/JPY for accurate position sizing
        try:
            usdjpy = trader.get_current_price("USDJPY")
            print(f"[IB] USD/JPY rate: {usdjpy:.3f}")
        except Exception:
            usdjpy = 150.0
            print(f"[IB] Could not fetch USD/JPY, using fallback: {usdjpy}")

        now_utc = pd.Timestamp.now("UTC")

        for pair in pairs:
            print(f"\n[Strategy] Processing {pair}...")
            try:
                # Skip if open position already exists for this pair
                open_symbols = [p["symbol"] for p in trader.get_open_positions()]
                if pair[:3].upper() in open_symbols:
                    print(f"[Strategy] {pair}: Skipping — open position already exists")
                    discord.notify_error(webhook_url, f"{pair}: Skipped — open position already exists")
                    continue

                df = fetch_historical(pair, period="7d", interval="1h")
                signals = generate_both_signals(
                    pair=pair, df=df, as_of=now_utc,
                    asian_range_hours=strategy_cfg["asian_range_hours"],
                    pip_buffer=strategy_cfg["pip_buffer"],
                    tp_multiplier=strategy_cfg["tp_multiplier"],
                )

                if not signals:
                    print(f"[Strategy] {pair}: No signal (range too tight)")
                    discord.notify_no_signal(webhook_url, pair, "range too tight")
                    continue

                buy_signal = next(s for s in signals if s.direction == "BUY")
                sell_signal = next(s for s in signals if s.direction == "SELL")

                lot_size = calculate_lot_size(
                    pair=pair, account_balance=account_balance, risk_pct=risk_pct,
                    sl_pips=buy_signal.sl_pips, quote_per_usd=usdjpy,
                )
                risk_usd = account_balance * risk_pct

                # Place OCA bracket orders — returns group with all 6 order IDs
                group = trader.place_oca_breakout(
                    buy_signal=buy_signal, sell_signal=sell_signal,
                    lot_size=lot_size, expire_hours=session_hours,
                )

                # Log both pending sides to DB and store DB IDs in group
                group.buy_db_id = logger.log_trade_opened(
                    pair=pair, direction="BUY",
                    entry_price=buy_signal.entry, stop_loss=buy_signal.stop_loss,
                    take_profit=buy_signal.take_profit, lot_size=lot_size,
                    sl_pips=buy_signal.sl_pips, tp_pips=buy_signal.tp_pips,
                    ib_order_id=group.buy_entry_id,
                )
                group.sell_db_id = logger.log_trade_opened(
                    pair=pair, direction="SELL",
                    entry_price=sell_signal.entry, stop_loss=sell_signal.stop_loss,
                    take_profit=sell_signal.take_profit, lot_size=lot_size,
                    sl_pips=sell_signal.sl_pips, tp_pips=sell_signal.tp_pips,
                    ib_order_id=group.sell_entry_id,
                )

                # Discord: notify both pending orders
                for signal in [buy_signal, sell_signal]:
                    discord.notify_order_placed(
                        webhook_url=webhook_url, pair=pair, direction=signal.direction,
                        entry=signal.entry, sl=signal.stop_loss, tp=signal.take_profit,
                        sl_pips=signal.sl_pips, tp_pips=signal.tp_pips,
                        lot_size=lot_size, risk_usd=risk_usd,
                    )

                order_groups.append(group)

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")
                discord.notify_error(webhook_url, f"{pair}: `{e}`")

        if not order_groups:
            print("[Bot] No orders placed today — nothing to monitor.")
            discord.notify_daily_summary(webhook_url, [
                {"pair": p, "result": "NO_SIGNAL", "pips": 0, "pnl_usd": 0} for p in pairs
            ])
            return

        # ── Feedback loop: monitor fills for the full session duration ────────
        monitor = TradeMonitor(
            ib=trader.ib,
            logger=logger,
            webhook_url=webhook_url,
            order_groups=order_groups,
            quote_per_usd=usdjpy,
        )
        monitor.start()

        print(f"\n[Bot] Monitoring session for {session_hours} hours...")
        trader.ib.sleep(session_hours * 3600)   # runs IB event loop while waiting

        monitor.stop()

    except Exception as e:
        print(f"[FATAL] Strategy run failed: {e}")
        discord.notify_error(webhook_url, f"FATAL: `{e}`", fatal=True)
    finally:
        trader.disconnect()
        logger.close()


def main():
    parser = argparse.ArgumentParser(description="London Breakout Trading Bot")
    parser.add_argument("--once", action="store_true",
                        help="Run strategy once immediately instead of scheduling")
    args = parser.parse_args()

    config = load_config()
    schedule_cfg = config["schedule"]

    if args.once:
        print("[Bot] Running strategy once (--once flag)...")
        run_strategy(config)
        return

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
