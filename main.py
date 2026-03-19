"""
Trading Bot — Main Entry Point
--------------------------------
Runs the London Breakout strategy daily at 5pm Sydney time.
Connects to IB TWS (paper or live based on config).

Flow per session:
  1. Connect to IB TWS
  2. Fetch account balance + USD/AUD rate
  3. Log account snapshot + check risk limits
  4. Fetch H1 data + generate Asian range signals
  5. Commission viability check (skip if commission > X% of risk)
  6. Place OCA breakout orders (BUY STOP + SELL STOP with bracket SL/TP)
  7. Log signals to DB (traded and skipped)
  8. Start TradeMonitor — subscribes to IB fill/cancel events
  9. Keep connection alive for max_trade_duration_hours
  10. Monitor fires Telegram alerts + updates SQLite on every fill/expiry
  11. Send daily summary, disconnect

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
from notifications import telegram_notifier as notify
from risk.daily_limits import LimitBreached, check_limits
from risk.position_sizer import (
    calculate_lot_size,
    estimate_commission,
    check_commission_viability,
)
from strategy.london_breakout import generate_both_signals, PIP_SIZE


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
    tg_cfg = config.get("telegram", {})
    bot_token = tg_cfg.get("bot_token", "")
    chat_id = str(tg_cfg.get("chat_id", ""))
    db_path = config.get("db_path", "logs/trades.db")
    session_hours = strategy_cfg["max_trade_duration_hours"]
    limits_cfg = config.get("risk_limits", {})
    tax_cfg = config.get("tax", {})

    print(f"\n[{datetime.now()}] Running strategy | Mode: {mode.upper()}")

    port = ib_cfg["paper_port"] if mode == "paper" else ib_cfg["live_port"]
    trader = IBTrader(host=ib_cfg["host"], port=port, client_id=ib_cfg["client_id"])
    logger = TradeLogger(db_path)
    order_groups = []

    try:
        trader.connect()
        account_balance = trader.get_account_balance()
        print(f"[IB] Account balance: ${account_balance:,.2f}")

        # Fetch USD/JPY for position sizing
        try:
            usdjpy = trader.get_current_price("USDJPY")
            print(f"[IB] USD/JPY rate: {usdjpy:.3f}")
        except Exception:
            usdjpy = 150.0
            print(f"[IB] Could not fetch USD/JPY, using fallback: {usdjpy}")

        # Fetch USD/AUD for ATO tax conversion
        try:
            audusd = trader.get_current_price("AUDUSD")
            usd_aud_rate = round(1.0 / audusd, 4)
            print(f"[IB] USD/AUD rate: {usd_aud_rate:.4f} (AUD/USD: {audusd:.4f})")
        except Exception:
            usd_aud_rate = tax_cfg.get("default_usd_aud_rate", 1.54)
            print(f"[IB] Could not fetch AUD/USD, using fallback: {usd_aud_rate}")

        # Log account snapshot
        today_pnl = logger.get_today_pnl()
        open_count = len(trader.get_open_positions())
        logger.log_account_snapshot(
            net_liquidation=account_balance,
            net_liquidation_aud=round(account_balance * usd_aud_rate, 2),
            usd_aud_rate=usd_aud_rate,
            realised_pnl_today=today_pnl,
            open_positions=open_count,
        )

        # Check daily/weekly loss limits + minimum balance before doing anything
        try:
            check_limits(
                logger=logger,
                account_balance=account_balance,
                daily_loss_limit=limits_cfg.get("daily_loss_limit", 0.02),
                weekly_loss_limit=limits_cfg.get("weekly_loss_limit", 0.05),
                max_consecutive_losses=limits_cfg.get("max_consecutive_losses", 3),
                min_account_balance=limits_cfg.get("min_account_balance", 0),
            )
        except LimitBreached as e:
            print(f"[Risk] Limit breached — skipping session: {e}")
            notify.notify_error(bot_token, chat_id, str(e), fatal=False)
            # Log skipped signals for all pairs
            now_utc = pd.Timestamp.now("UTC")
            for pair in pairs:
                logger.log_signal(
                    pair=pair,
                    signal_date=now_utc.strftime("%Y-%m-%d"),
                    signal_time_utc=now_utc.isoformat(),
                    traded=False,
                    skip_reason=f"limit_breached: {e}",
                )
            return

        now_utc = pd.Timestamp.now("UTC")

        for pair in pairs:
            print(f"\n[Strategy] Processing {pair}...")
            try:
                # Skip if open position already exists for this pair
                open_symbols = [p["symbol"] for p in trader.get_open_positions()]
                if pair[:3].upper() in open_symbols:
                    print(f"[Strategy] {pair}: Skipping — open position already exists")
                    notify.notify_error(bot_token, chat_id, f"{pair}: Skipped — open position already exists")
                    logger.log_signal(
                        pair=pair,
                        signal_date=now_utc.strftime("%Y-%m-%d"),
                        signal_time_utc=now_utc.isoformat(),
                        traded=False,
                        skip_reason="open_position_exists",
                    )
                    continue

                df = fetch_historical(pair, period="7d", interval="1h", ib=trader.ib)
                signals = generate_both_signals(
                    pair=pair, df=df, as_of=now_utc,
                    asian_range_hours=strategy_cfg["asian_range_hours"],
                    pip_buffer=strategy_cfg["pip_buffer"],
                    tp_multiplier=strategy_cfg["tp_multiplier"],
                )

                if not signals:
                    print(f"[Strategy] {pair}: No signal (range too tight)")
                    notify.notify_no_signal(bot_token, chat_id, pair, "range too tight")
                    logger.log_signal(
                        pair=pair,
                        signal_date=now_utc.strftime("%Y-%m-%d"),
                        signal_time_utc=now_utc.isoformat(),
                        traded=False,
                        skip_reason="range_too_tight",
                    )
                    continue

                buy_signal = next(s for s in signals if s.direction == "BUY")
                sell_signal = next(s for s in signals if s.direction == "SELL")

                # USD-quoted pairs have pip value directly in USD (quote_per_usd=1)
                quote_rate = 1.0 if pair.upper().endswith("USD") else usdjpy
                lot_size = calculate_lot_size(
                    pair=pair, account_balance=account_balance, risk_pct=risk_pct,
                    sl_pips=buy_signal.sl_pips, quote_per_usd=quote_rate,
                )
                risk_usd = account_balance * risk_pct

                # Commission viability check for small accounts
                est_comm = estimate_commission(
                    lot_size=lot_size,
                    commission_per_lot=limits_cfg.get("commission_per_lot", 2.0),
                )
                max_comm_pct = limits_cfg.get("max_commission_pct", 0.10)
                is_viable, comm_pct = check_commission_viability(est_comm, risk_usd, max_comm_pct)

                if not is_viable:
                    reason = f"commission_too_high: {comm_pct*100:.1f}% of risk (max {max_comm_pct*100:.0f}%)"
                    print(f"[Risk] {pair}: Skipping — {reason}")
                    notify.notify_no_signal(bot_token, chat_id, pair, reason)
                    pip = PIP_SIZE.get(pair.upper(), 0.01)
                    range_size = (buy_signal.range_high - buy_signal.range_low) / pip
                    logger.log_signal(
                        pair=pair,
                        signal_date=now_utc.strftime("%Y-%m-%d"),
                        signal_time_utc=now_utc.isoformat(),
                        range_high=buy_signal.range_high,
                        range_low=buy_signal.range_low,
                        range_size_pips=round(range_size, 1),
                        buy_entry=buy_signal.entry, buy_sl=buy_signal.stop_loss, buy_tp=buy_signal.take_profit,
                        sell_entry=sell_signal.entry, sell_sl=sell_signal.stop_loss, sell_tp=sell_signal.take_profit,
                        sl_pips=buy_signal.sl_pips, tp_pips=buy_signal.tp_pips,
                        traded=False,
                        skip_reason=reason,
                    )
                    continue

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

                # Log signal as traded
                pip = PIP_SIZE.get(pair.upper(), 0.01)
                range_size = (buy_signal.range_high - buy_signal.range_low) / pip
                logger.log_signal(
                    pair=pair,
                    signal_date=now_utc.strftime("%Y-%m-%d"),
                    signal_time_utc=now_utc.isoformat(),
                    range_high=buy_signal.range_high,
                    range_low=buy_signal.range_low,
                    range_size_pips=round(range_size, 1),
                    buy_entry=buy_signal.entry, buy_sl=buy_signal.stop_loss, buy_tp=buy_signal.take_profit,
                    sell_entry=sell_signal.entry, sell_sl=sell_signal.stop_loss, sell_tp=sell_signal.take_profit,
                    sl_pips=buy_signal.sl_pips, tp_pips=buy_signal.tp_pips,
                    traded=True,
                    trade_id=group.buy_db_id,
                )

                # Log execution events for order placement
                for label, oid, sig in [
                    ("BUY", group.buy_entry_id, buy_signal),
                    ("SELL", group.sell_entry_id, sell_signal),
                ]:
                    db_id = group.buy_db_id if label == "BUY" else group.sell_db_id
                    logger.log_execution_event(
                        trade_id=db_id, ib_order_id=oid,
                        event_type="PLACED", event_time=datetime.utcnow().isoformat(),
                        order_type="ENTRY", price=sig.entry,
                        quantity=round(lot_size * 100_000),
                        notes=f"{label} STOP LMT @ {sig.entry}",
                    )

                # Telegram: notify both pending orders
                for signal in [buy_signal, sell_signal]:
                    notify.notify_order_placed(
                        bot_token=bot_token, chat_id=chat_id, pair=pair, direction=signal.direction,
                        entry=signal.entry, sl=signal.stop_loss, tp=signal.take_profit,
                        sl_pips=signal.sl_pips, tp_pips=signal.tp_pips,
                        lot_size=lot_size, risk_usd=risk_usd,
                    )

                order_groups.append(group)

            except Exception as e:
                print(f"[ERROR] {pair}: {e}")
                notify.notify_error(bot_token, chat_id, f"{pair}: `{e}`")

        if not order_groups:
            print("[Bot] No orders placed today — nothing to monitor.")
            notify.notify_daily_summary(bot_token, chat_id, [
                {"pair": p, "result": "NO_SIGNAL", "pips": 0, "pnl_usd": 0} for p in pairs
            ])
            return

        # ── Feedback loop: monitor fills for the full session duration ────────
        monitor = TradeMonitor(
            ib=trader.ib,
            logger=logger,
            bot_token=bot_token,
            chat_id=chat_id,
            order_groups=order_groups,
            quote_per_usd=usdjpy,
            usd_aud_rate=usd_aud_rate,
        )
        monitor.start()

        print(f"\n[Bot] Monitoring session for {session_hours} hours...")
        trader.ib.sleep(session_hours * 3600)   # runs IB event loop while waiting

        monitor.stop()

    except Exception as e:
        print(f"[FATAL] Strategy run failed: {e}")
        notify.notify_error(bot_token, chat_id, f"FATAL: `{e}`", fatal=True)
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
