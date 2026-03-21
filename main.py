"""
Wealth Builder Bot — Main Entry Point
--------------------------------------
24/7 multi-strategy trading bot with persistent IB connection.

Architecture:
  - ConnectionManager: persistent IB Gateway connection + auto-reconnect
  - ContinuousMonitor: always-on position watching, trailing stops, P&L alerts
  - StrategyScheduler: asyncio-based strategy scheduling (fixed + continuous)

Strategies are configured independently with their own schedules,
risk parameters, and exit strategies. The opportunity scorer
filters trades based on R:R ratio, win probability, and confluence.

Usage:
    python main.py              # start 24/7 continuous operation
    python main.py --once       # run all enabled strategies once (testing)
"""
import argparse
import asyncio
import signal
import sys
from datetime import datetime

import nest_asyncio
nest_asyncio.apply()

from config.loader import load_config
from core.connection_manager import ConnectionManager
from core.continuous_monitor import ContinuousMonitor
from core.strategy_scheduler import StrategyScheduler
from logs.trade_logger import TradeLogger
from notifications import telegram_notifier as notify
from health_server import start_health_server, state as health_state


def _send_startup_alert(config: dict) -> None:
    """Send Telegram alert when bot starts."""
    tg = config.get("telegram", {})
    bot_token = tg.get("bot_token", "")
    chat_id = str(tg.get("chat_id", ""))

    strategies = config.get("strategies", {})
    enabled = [name for name, cfg in strategies.items() if cfg.get("enabled", False)]
    mode = config["mode"].upper()

    realloc = config.get("reallocation", {})
    realloc_status = f"Realloc: {realloc.get('pct', 0.5)*100:.0f}%" if realloc.get("enabled") else "Realloc: OFF"

    msg = (
        f"\U0001f680 *WEALTH BUILDER BOT STARTED* (24/7)\n"
        f"Mode: `{mode}` | Strategies: `{', '.join(enabled)}`\n"
        f"{realloc_status} | Continuous monitoring: ON"
    )
    notify._send(bot_token, chat_id, msg)


async def async_main(config: dict) -> None:
    """Main async entry point — connects, starts monitoring, runs forever."""
    ib_cfg = config["ib"]
    tg_cfg = config.get("telegram", {})
    bot_token = tg_cfg.get("bot_token", "")
    chat_id = str(tg_cfg.get("chat_id", ""))
    db_path = config.get("db_path", "logs/trades.db")
    conn_cfg = config.get("connection", {})

    mode = config["mode"]
    port = ib_cfg["paper_port"] if mode == "paper" else ib_cfg["live_port"]

    # 1. Establish persistent connection
    conn = ConnectionManager(
        host=ib_cfg["host"],
        port=port,
        client_id=ib_cfg["client_id"],
        bot_token=bot_token,
        chat_id=chat_id,
        reconnect_delay_initial=conn_cfg.get("reconnect_delay_initial", 5),
        reconnect_delay_max=conn_cfg.get("reconnect_delay_max", 60),
    )
    await conn.connect()
    health_state.ib_connected = True

    # 2. Start continuous monitoring
    logger = TradeLogger(db_path)
    monitor = ContinuousMonitor(
        ib=conn.ib,
        logger=logger,
        config=config,
        bot_token=bot_token,
        chat_id=chat_id,
    )
    monitor.start()

    # 3. Recover existing positions
    await monitor.recover_existing_positions()

    # 4. Register reconnect handler
    async def _on_reconnect():
        health_state.ib_connected = True
        await monitor.restore_subscriptions()

    conn.on_reconnect(_on_reconnect)
    conn.ib.disconnectedEvent += lambda: setattr(health_state, 'ib_connected', False)

    # 5. Start strategy scheduler
    scheduler = StrategyScheduler(
        ib=conn.ib,
        monitor=monitor,
        config=config,
        logger=logger,
        bot_token=bot_token,
        chat_id=chat_id,
    )
    await scheduler.start()

    # 6. Start health server
    start_health_server(port=8082)
    health_state.scheduler_running = True

    # 7. Startup notification
    _send_startup_alert(config)

    enabled = [n for n, c in config.get("strategies", {}).items() if c.get("enabled", False)]
    print(f"[Bot] 24/7 continuous mode | Strategies: {', '.join(enabled)}")
    print(f"[Bot] Mode: {mode.upper()} | Port: {port}")
    print("[Bot] Press Ctrl+C to stop.\n")


def run_once(config: dict) -> None:
    """Run all enabled strategies once (testing mode)."""
    from core.strategy_scheduler import STRATEGIES, _build_strategy_registry
    from execution.ib_trader import IBTrader
    from strategy.opportunity_scorer import filter_opportunities
    from risk.daily_limits import LimitBreached, check_limits

    ib_cfg = config["ib"]
    tg_cfg = config.get("telegram", {})
    bot_token = tg_cfg.get("bot_token", "")
    chat_id = str(tg_cfg.get("chat_id", ""))
    db_path = config.get("db_path", "logs/trades.db")
    scoring_cfg = config.get("scoring", {})
    limits_cfg = config.get("risk_limits", {})

    mode = config["mode"]
    port = ib_cfg["paper_port"] if mode == "paper" else ib_cfg["live_port"]

    # Use asyncio event loop for ib_insync
    asyncio.set_event_loop(asyncio.new_event_loop())

    trader = IBTrader(ib_cfg["host"], port, ib_cfg["client_id"])
    logger = TradeLogger(db_path)

    try:
        trader.connect()
        account_balance = trader.get_account_balance()
        print(f"[IB] Account balance: ${account_balance:,.2f}")

        # Check limits
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
            print(f"[Risk] Limit breached: {e}")
            return

        # Generate signals from all enabled strategies
        strategies = _build_strategy_registry()
        all_intents = []
        for strat_name, strategy in strategies.items():
            strat_cfg = config.get("strategies", {}).get(strat_name, {})
            if not strat_cfg.get("enabled", False):
                continue
            print(f"\n[Strategy] Running {strat_name}...")
            try:
                intents = strategy.generate(config=config, ib=trader.ib, account_balance=account_balance)
                if intents:
                    print(f"[Strategy] {strat_name}: {len(intents)} intents")
                    all_intents.extend(intents)
                else:
                    print(f"[Strategy] {strat_name}: no signals")
            except Exception as e:
                print(f"[Strategy] {strat_name} error: {e}")

        if not all_intents:
            print("[Bot] No trade intents.")
            return

        # Score
        accepted, all_scores = filter_opportunities(
            intents=all_intents,
            min_score=scoring_cfg.get("min_asymmetry_score", 0.5),
            min_rr=scoring_cfg.get("min_rr_ratio", 1.5),
            min_ev=scoring_cfg.get("min_expected_value", 0.0),
            logger=logger,
            session_id=datetime.utcnow().strftime("%Y-%m-%dT%H:%M"),
        )
        for score in all_scores:
            print(f"[Score] {score.summary()}")

        print(f"\n[Bot] {len(accepted)} accepted / {len(all_intents)} total intents")

    except Exception as e:
        print(f"[FATAL] {e}")
        notify.notify_error(bot_token, chat_id, f"FATAL: `{e}`", fatal=True)
    finally:
        trader.disconnect()
        logger.close()


def main():
    parser = argparse.ArgumentParser(description="Wealth Builder Trading Bot")
    parser.add_argument("--once", action="store_true",
                        help="Run all enabled strategies once immediately")
    args = parser.parse_args()

    config = load_config()

    if args.once:
        print("[Bot] Running all strategies once (--once flag)...")
        run_once(config)
        return

    # 24/7 continuous mode
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Graceful shutdown on SIGTERM/SIGINT
    def _shutdown(sig, frame):
        tg = config.get("telegram", {})
        notify.notify_bot_shutdown(
            tg.get("bot_token", ""), str(tg.get("chat_id", "")),
            "Graceful shutdown",
        )
        print("\n[Bot] Shutting down...")
        loop.stop()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    try:
        loop.run_until_complete(async_main(config))
        loop.run_forever()
    except KeyboardInterrupt:
        print("\n[Bot] Stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
