"""
Wealth Builder Bot — Main Entry Point
--------------------------------------
Multi-strategy trading bot that:
  1. Runs multiple strategies (London Breakout forex + momentum stocks)
  2. Scores every opportunity for asymmetric risk:reward
  3. Manages exits with trailing stops and partial scale-outs
  4. Reallocates 50% of profits into long-term ETF investments

Strategies are configured independently with their own schedules,
risk parameters, and exit strategies. The opportunity scorer
filters trades based on R:R ratio, win probability, and confluence.

Usage:
    python main.py              # start the multi-strategy scheduler
    python main.py --once       # run all enabled strategies once (testing)
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
from execution.stock_trader import StockTrader, StockOrderGroup
from execution.reallocation import ProfitReallocator
from exits.exit_manager import ExitManager
from logs.trade_logger import TradeLogger
from notifications import telegram_notifier as notify
from risk.daily_limits import LimitBreached, check_limits
from risk.portfolio_risk import check_portfolio_risk, PortfolioRiskError
from risk.position_sizer import (
    calculate_lot_size,
    estimate_commission,
    check_commission_viability,
)
from strategy.base import TradeIntent
from strategy.london_breakout import (
    LondonBreakoutStrategy,
    generate_both_signals,
    PIP_SIZE,
)
from strategy.momentum_stocks import MomentumStocksStrategy
from strategy.opportunity_scorer import filter_opportunities, score_opportunity
from strategy.feature_tracker import FeatureDecision, log_decision, get_summary_report


# ── Strategy registry ────────────────────────────────────────────────────

STRATEGIES = {
    "london_breakout": LondonBreakoutStrategy(),
    "momentum_stocks": MomentumStocksStrategy(),
}


def run_strategy(config: dict) -> None:
    """
    Full pipeline: data → signals → scoring → orders → monitor → alerts.
    Keeps IB connection alive for the full session duration.
    """
    mode = config["mode"]
    ib_cfg = config["ib"]
    tg_cfg = config.get("telegram", {})
    bot_token = tg_cfg.get("bot_token", "")
    chat_id = str(tg_cfg.get("chat_id", ""))
    db_path = config.get("db_path", "logs/trades.db")
    limits_cfg = config.get("risk_limits", {})
    tax_cfg = config.get("tax", {})
    scoring_cfg = config.get("scoring", {})

    # Use longest session duration across enabled strategies
    session_hours = _get_max_session_hours(config)

    session_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
    print(f"\n[{datetime.now()}] Running strategy | Mode: {mode.upper()} | Session: {session_id}")

    port = ib_cfg["paper_port"] if mode == "paper" else ib_cfg["live_port"]
    trader = IBTrader(host=ib_cfg["host"], port=port, client_id=ib_cfg["client_id"])
    logger = TradeLogger(db_path)
    order_groups = []
    stock_groups = []

    try:
        trader.connect()
        account_balance = trader.get_account_balance()
        print(f"[IB] Account balance: ${account_balance:,.2f}")

        # Fetch rates
        try:
            usdjpy = trader.get_current_price("USDJPY")
        except Exception:
            usdjpy = 150.0

        try:
            audusd = trader.get_current_price("AUDUSD")
            usd_aud_rate = round(1.0 / audusd, 4)
        except Exception:
            usd_aud_rate = tax_cfg.get("default_usd_aud_rate", 1.54)

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

        # Check daily/weekly loss limits
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
            log_decision(logger, FeatureDecision(
                feature="risk_limits", symbol="ALL", strategy="session",
                decision="reject", rule=str(e), context={"balance": account_balance},
                session_id=session_id,
            ))
            return

        # ── Phase 1: Generate trade intents from all enabled strategies ────

        all_intents = []
        now_utc = pd.Timestamp.now("UTC")

        for strat_name, strategy in STRATEGIES.items():
            strat_cfg = config.get("strategies", {}).get(strat_name, {})
            if not strat_cfg.get("enabled", False):
                continue

            print(f"\n[Strategy] Running {strat_name}...")
            try:
                intents = strategy.generate(
                    config=config,
                    ib=trader.ib,
                    account_balance=account_balance,
                )
                if intents:
                    print(f"[Strategy] {strat_name}: {len(intents)} trade intents generated")
                    all_intents.extend(intents)
                else:
                    print(f"[Strategy] {strat_name}: no signals")
            except Exception as e:
                print(f"[Strategy] {strat_name} error: {e}")
                notify.notify_error(bot_token, chat_id, f"{strat_name}: `{e}`")

        if not all_intents:
            print("[Bot] No trade intents from any strategy.")
            notify._send(bot_token, chat_id, "\u26aa *NO SIGNALS* — no opportunities found across all strategies")
            return

        # ── Phase 2: Score all opportunities ────────────────────────────────

        accepted_intents, all_scores = filter_opportunities(
            intents=all_intents,
            min_score=scoring_cfg.get("min_asymmetry_score", 0.5),
            min_rr=scoring_cfg.get("min_rr_ratio", 1.5),
            min_ev=scoring_cfg.get("min_expected_value", 0.0),
            logger=logger,
            session_id=session_id,
        )

        # Log all scores
        for score in all_scores:
            print(f"[Score] {score.summary()}")
        notify.notify_score_report(bot_token, chat_id, all_scores)

        if not accepted_intents:
            print("[Bot] No intents passed the opportunity scorer.")
            notify._send(bot_token, chat_id, "\u274c *NO TRADES* — all setups rejected by opportunity scorer")
            return

        print(f"\n[Bot] {len(accepted_intents)} trades accepted out of {len(all_intents)} intents")

        # ── Phase 3: Execute accepted intents ───────────────────────────────

        exit_manager = ExitManager(config)
        stock_trader = StockTrader(trader.ib)
        open_positions = trader.get_open_positions()

        for intent in accepted_intents:
            try:
                # Portfolio risk check
                try:
                    check_portfolio_risk(
                        logger=logger,
                        account_balance=account_balance,
                        new_risk_usd=intent.risk_dollars,
                        instrument_type=intent.instrument_type,
                        config=config,
                        open_positions=open_positions,
                    )
                except PortfolioRiskError as e:
                    print(f"[Risk] Portfolio limit: {e}")
                    notify.notify_error(bot_token, chat_id, f"Portfolio risk: {e}")
                    log_decision(logger, FeatureDecision(
                        feature="portfolio_risk", symbol=intent.symbol,
                        strategy=intent.strategy, decision="reject",
                        rule=str(e), context={"risk_usd": intent.risk_dollars},
                        session_id=session_id,
                    ))
                    continue

                if intent.is_forex:
                    _execute_forex_intent(
                        intent, trader, logger, order_groups,
                        bot_token, chat_id, now_utc,
                        limits_cfg, usdjpy, account_balance,
                    )
                elif intent.is_stock:
                    _execute_stock_intent(
                        intent, stock_trader, logger, stock_groups,
                        exit_manager, bot_token, chat_id, now_utc,
                    )

            except Exception as e:
                print(f"[ERROR] {intent.symbol}: {e}")
                notify.notify_error(bot_token, chat_id, f"{intent.symbol}: `{e}`")

        if not order_groups and not stock_groups:
            print("[Bot] No orders placed — nothing to monitor.")
            return

        # ── Phase 4: Monitor fills ──────────────────────────────────────────

        # Set up reallocation engine
        reallocator = ProfitReallocator(
            ib=trader.ib, logger=logger, config=config,
            bot_token=bot_token, chat_id=chat_id,
        )

        if order_groups:
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

        # Monitoring loop with periodic updates
        update_interval = 15 * 60
        total_seconds = session_hours * 3600
        elapsed = 0

        print(f"\n[Bot] Monitoring session for {session_hours} hours (updates every 15 min)...")
        while elapsed < total_seconds:
            wait = min(update_interval, total_seconds - elapsed)
            trader.ib.sleep(wait)
            elapsed += wait

            # Position update
            try:
                positions = []
                for pos in trader.get_open_positions():
                    positions.append({
                        "pair": pos["symbol"],
                        "side": "LONG" if pos["position"] > 0 else "SHORT",
                        "entry": pos["avg_cost"],
                        "current": 0, "pnl": 0,
                    })
                bal = trader.get_account_balance()
                unrealised = 0.0
                for av in trader.ib.accountValues():
                    if av.tag == "UnrealizedPnL" and av.currency in ("USD", "AUD", "BASE"):
                        try:
                            unrealised = float(av.value)
                        except ValueError:
                            pass
                        break
                notify.notify_position_update(bot_token, chat_id, positions, unrealised, bal)
            except Exception as e:
                print(f"[Monitor] Position update failed: {e}")

        if order_groups:
            monitor.stop()

    except Exception as e:
        print(f"[FATAL] Strategy run failed: {e}")
        notify.notify_error(bot_token, chat_id, f"FATAL: `{e}`", fatal=True)
    finally:
        notify.notify_bot_shutdown(bot_token, chat_id, "Session ended")
        trader.disconnect()
        logger.close()


# ── Intent execution helpers ─────────────────────────────────────────────

def _execute_forex_intent(
    intent: TradeIntent,
    trader: IBTrader,
    logger: TradeLogger,
    order_groups: list,
    bot_token: str,
    chat_id: str,
    now_utc,
    limits_cfg: dict,
    usdjpy: float,
    account_balance: float,
) -> None:
    """Execute a forex trade intent (London Breakout style OCA bracket)."""
    pair = intent.symbol
    strategy_cfg = intent.metadata

    # Skip if position exists
    open_symbols = [p["symbol"] for p in trader.get_open_positions()]
    if pair[:3].upper() in open_symbols:
        print(f"[Exec] {pair}: Skipping — open position exists")
        logger.log_signal(
            pair=pair,
            signal_date=now_utc.strftime("%Y-%m-%d"),
            signal_time_utc=now_utc.isoformat(),
            traded=False,
            skip_reason="open_position_exists",
        )
        return

    # We need paired intents (BUY + SELL) for OCA breakout
    # Find the partner intent — if this is a BUY, skip (we process on SELL)
    if intent.direction == "BUY":
        return  # will be processed when we hit the SELL intent

    # Look for the matching BUY intent in order_groups context
    # Actually, for London Breakout the LondonBreakoutStrategy generates both
    # BUY and SELL intents. We need to process them as a pair.
    # The strategy generates them in order, so BUY comes before SELL.
    # We handle this by collecting paired intents in the main loop.
    # For now, use the legacy path which works with generate_both_signals.

    # Fall back to legacy execution for forex OCA brackets
    from strategy.london_breakout import generate_both_signals
    from data.data_fetcher import fetch_historical

    strat_cfg = intent.metadata
    risk_pct = intent.risk_dollars / account_balance if account_balance > 0 else 0.01

    df = fetch_historical(pair, period="7d", interval="1h", ib=trader.ib)
    signals = generate_both_signals(
        pair=pair, df=df, as_of=now_utc,
        asian_range_hours=strat_cfg.get("asian_range_hours", 6),
        pip_buffer=strat_cfg.get("pip_buffer", 5),
        tp_multiplier=strat_cfg.get("tp_multiplier", 2.0),
    )

    if not signals:
        return

    buy_signal = next(s for s in signals if s.direction == "BUY")
    sell_signal = next(s for s in signals if s.direction == "SELL")

    quote_rate = 1.0 if pair.upper().endswith("USD") else usdjpy
    lot_size = calculate_lot_size(
        pair=pair, account_balance=account_balance, risk_pct=risk_pct,
        sl_pips=buy_signal.sl_pips, quote_per_usd=quote_rate,
    )
    risk_usd = account_balance * risk_pct

    # Commission viability check
    est_comm = estimate_commission(lot_size, limits_cfg.get("commission_per_lot", 2.0))
    max_comm_pct = limits_cfg.get("max_commission_pct", 0.10)
    is_viable, comm_pct = check_commission_viability(est_comm, risk_usd, max_comm_pct)
    if not is_viable:
        print(f"[Risk] {pair}: Commission too high ({comm_pct*100:.1f}%)")
        log_decision(logger, FeatureDecision(
            feature="commission_check", symbol=pair,
            strategy=intent.strategy, decision="reject",
            rule=f"comm {comm_pct*100:.1f}% > max {max_comm_pct*100:.0f}%",
            context={"lot_size": lot_size, "commission": est_comm, "risk_usd": risk_usd},
            session_id=now_utc.strftime("%Y-%m-%dT%H:%M"),
        ))
        return

    # Place OCA bracket
    session_hours = strat_cfg.get("max_trade_duration_hours", 6)
    group = trader.place_oca_breakout(
        buy_signal=buy_signal, sell_signal=sell_signal,
        lot_size=lot_size, expire_hours=session_hours,
    )

    # Log trades
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

    # Log signal
    pip = PIP_SIZE.get(pair.upper(), 0.01)
    range_size = (buy_signal.range_high - buy_signal.range_low) / pip
    logger.log_signal(
        pair=pair,
        signal_date=now_utc.strftime("%Y-%m-%d"),
        signal_time_utc=now_utc.isoformat(),
        range_high=buy_signal.range_high, range_low=buy_signal.range_low,
        range_size_pips=round(range_size, 1),
        buy_entry=buy_signal.entry, buy_sl=buy_signal.stop_loss, buy_tp=buy_signal.take_profit,
        sell_entry=sell_signal.entry, sell_sl=sell_signal.stop_loss, sell_tp=sell_signal.take_profit,
        sl_pips=buy_signal.sl_pips, tp_pips=buy_signal.tp_pips,
        traded=True, trade_id=group.buy_db_id,
    )

    # Log execution events
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

    # Telegram notifications
    for signal in [buy_signal, sell_signal]:
        notify.notify_order_placed(
            bot_token=bot_token, chat_id=chat_id, pair=pair, direction=signal.direction,
            entry=signal.entry, sl=signal.stop_loss, tp=signal.take_profit,
            sl_pips=signal.sl_pips, tp_pips=signal.tp_pips,
            lot_size=lot_size, risk_usd=risk_usd,
        )

    order_groups.append(group)


def _execute_stock_intent(
    intent: TradeIntent,
    stock_trader: StockTrader,
    logger: TradeLogger,
    stock_groups: list,
    exit_manager: ExitManager,
    bot_token: str,
    chat_id: str,
    now_utc,
) -> None:
    """Execute a stock trade intent (bracket order)."""
    group = stock_trader.place_stock_bracket(intent)

    # Log trade
    group.db_trade_id = logger.log_trade_opened(
        pair=intent.symbol,
        direction=intent.direction,
        entry_price=intent.entry_price,
        stop_loss=intent.stop_loss,
        take_profit=intent.take_profit,
        lot_size=intent.quantity,  # shares for stocks
        sl_pips=0,
        tp_pips=0,
        ib_order_id=group.entry_order_id,
    )

    # Register with exit manager for trailing stops / partial exits
    exit_manager.register_trade(
        trade_db_id=group.db_trade_id,
        symbol=intent.symbol,
        strategy=intent.strategy,
        side=intent.direction,
        entry_price=intent.entry_price,
        stop_loss=intent.stop_loss,
        take_profit=intent.take_profit,
        quantity=intent.quantity,
        exit_strategy=intent.exit_strategy,
        trailing_config=intent.trailing_config,
        partial_exits=intent.partial_exits,
    )

    # Log execution event
    logger.log_execution_event(
        trade_id=group.db_trade_id,
        ib_order_id=group.entry_order_id,
        event_type="PLACED",
        event_time=datetime.utcnow().isoformat(),
        order_type="ENTRY",
        price=intent.entry_price,
        quantity=intent.quantity,
        notes=f"{intent.direction} {int(intent.quantity)} shares @ ${intent.entry_price:.2f}",
    )

    # Telegram
    notify.notify_stock_order(
        bot_token, chat_id,
        symbol=intent.symbol,
        direction=intent.direction,
        shares=int(intent.quantity),
        entry=intent.entry_price,
        sl=intent.stop_loss,
        tp=intent.take_profit,
        risk_usd=intent.risk_dollars,
        strategy=intent.strategy,
    )

    stock_groups.append(group)


# ── Reallocation job ─────────────────────────────────────────────────────

def run_reallocation(config: dict) -> None:
    """
    Weekly job to execute pending ETF purchases.
    Uses a short-lived IB connection — kept separate from the main trading
    connection because reallocation runs on its own schedule (Friday 10:30).
    The connection is lightweight and closed immediately after use.
    """
    ib_cfg = config["ib"]
    tg_cfg = config.get("telegram", {})
    bot_token = tg_cfg.get("bot_token", "")
    chat_id = str(tg_cfg.get("chat_id", ""))
    db_path = config.get("db_path", "logs/trades.db")

    mode = config["mode"]
    port = ib_cfg["paper_port"] if mode == "paper" else ib_cfg["live_port"]

    trader = IBTrader(host=ib_cfg["host"], port=port, client_id=ib_cfg["client_id"] + 10)
    logger = TradeLogger(db_path)

    try:
        trader.connect()

        reallocator = ProfitReallocator(
            ib=trader.ib, logger=logger, config=config,
            bot_token=bot_token, chat_id=chat_id,
        )
        reallocator.session_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")

        # Fetch USD/AUD rate
        try:
            audusd = trader.get_current_price("AUDUSD")
            usd_aud_rate = round(1.0 / audusd, 4)
        except Exception:
            usd_aud_rate = config.get("tax", {}).get("default_usd_aud_rate", 1.54)

        purchases = reallocator.execute_pending_purchases(usd_aud_rate)

        if purchases:
            print(f"[Realloc] Completed {len(purchases)} ETF purchases")
        else:
            print("[Realloc] No purchases needed")

        # Log portfolio snapshot
        try:
            balance = trader.get_account_balance()
            etf_invested = logger.get_total_etf_invested()
            pending = logger.get_pending_reallocation_total()
            logger.log_portfolio_snapshot(
                trading_balance_usd=balance,
                etf_value_usd=etf_invested,
                usd_aud_rate=usd_aud_rate,
                total_wealth_aud=round((balance + etf_invested) * usd_aud_rate, 2),
                pending_reallocation_usd=pending,
            )
        except Exception as e:
            print(f"[Realloc] Portfolio snapshot failed: {e}")

    except Exception as e:
        print(f"[Realloc] Error: {e}")
        notify.notify_error(bot_token, chat_id, f"Reallocation error: {e}")
    finally:
        trader.disconnect()
        logger.close()


# ── Feature health + counterfactual backfill ─────────────────────────────

def run_counterfactual_backfill(config: dict) -> None:
    """
    Nightly job to check what would have happened to rejected trades.
    Uses yfinance price history to estimate counterfactual outcomes.
    """
    db_path = config.get("db_path", "logs/trades.db")
    logger = TradeLogger(db_path)

    try:
        rejects = logger.get_unbackfilled_rejects(max_age_days=7)
        if not rejects:
            return

        import json
        from data.data_fetcher import fetch_historical

        backfilled = 0
        for r in rejects:
            try:
                ctx = json.loads(r["context_json"]) if r["context_json"] else {}
                entry = ctx.get("entry_price")
                sl = ctx.get("stop_loss")
                tp = ctx.get("take_profit")
                symbol = r["symbol"]

                if not all([entry, sl, tp, symbol]):
                    continue

                # Fetch price data since the decision
                df = fetch_historical(symbol, period="7d", interval="1h")
                if df is None or df.empty:
                    continue

                # Check if entry was hit
                high_reached = df["High"].max()
                low_reached = df["Low"].min()

                entry_hit = False
                if entry <= high_reached and entry >= low_reached:
                    entry_hit = True

                if not entry_hit:
                    logger.update_counterfactual(r["id"], "no_entry", None)
                    backfilled += 1
                    continue

                # Check which was hit first: TP or SL
                is_buy = tp > entry
                if is_buy:
                    tp_hit = high_reached >= tp
                    sl_hit = low_reached <= sl
                else:
                    tp_hit = low_reached <= tp
                    sl_hit = high_reached >= sl

                if tp_hit and not sl_hit:
                    pnl = abs(tp - entry)
                    logger.update_counterfactual(r["id"], "would_profit", round(pnl, 5))
                elif sl_hit and not tp_hit:
                    pnl = -abs(sl - entry)
                    logger.update_counterfactual(r["id"], "would_loss", round(pnl, 5))
                elif tp_hit and sl_hit:
                    # Both hit — ambiguous, mark as uncertain
                    logger.update_counterfactual(r["id"], "ambiguous", None)
                else:
                    # Neither hit yet — still pending
                    continue

                backfilled += 1

            except Exception as e:
                print(f"[Backfill] Error processing decision #{r['id']}: {e}")

        if backfilled:
            print(f"[Backfill] Updated {backfilled}/{len(rejects)} counterfactuals")

    except Exception as e:
        print(f"[Backfill] Error: {e}")
    finally:
        logger.close()


def run_feature_health_report(config: dict) -> None:
    """Weekly job to compute feature value scores and send Telegram digest."""
    tg_cfg = config.get("telegram", {})
    bot_token = tg_cfg.get("bot_token", "")
    chat_id = str(tg_cfg.get("chat_id", ""))
    db_path = config.get("db_path", "logs/trades.db")

    logger = TradeLogger(db_path)
    try:
        scores, diagnostics = get_summary_report(logger, lookback_days=90)

        if not scores:
            print("[FeatureHealth] No feature decisions to report")
            return

        notify.notify_feature_health(bot_token, chat_id, scores, diagnostics)
        print(f"[FeatureHealth] Report sent — {len(scores)} features, {len(diagnostics)} underperformers")

    except Exception as e:
        print(f"[FeatureHealth] Error: {e}")
        notify.notify_error(bot_token, chat_id, f"Feature health report failed: {e}")
    finally:
        logger.close()


# ── Helpers ──────────────────────────────────────────────────────────────

def _get_max_session_hours(config: dict) -> int:
    """Get the longest session duration across all enabled strategies."""
    max_hours = 6  # default
    strategies = config.get("strategies", {})
    for name, cfg in strategies.items():
        if cfg.get("enabled", False):
            hours = cfg.get("max_trade_duration_hours", 6)
            max_hours = max(max_hours, hours)
    # Also check legacy config
    legacy = config.get("strategy", {}).get("max_trade_duration_hours", 6)
    return max(max_hours, legacy)


def _send_startup_alert(config: dict) -> None:
    """Send Telegram alert when bot starts/restarts."""
    tg = config.get("telegram", {})
    bot_token = tg.get("bot_token", "")
    chat_id = str(tg.get("chat_id", ""))

    strategies = config.get("strategies", {})
    enabled = [name for name, cfg in strategies.items() if cfg.get("enabled", False)]
    mode = config["mode"].upper()

    realloc = config.get("reallocation", {})
    realloc_status = f"Realloc: {realloc.get('pct', 0.5)*100:.0f}%" if realloc.get("enabled") else "Realloc: OFF"

    msg = (
        f"\U0001f680 *WEALTH BUILDER BOT STARTED*\n"
        f"Mode: `{mode}` | Strategies: `{', '.join(enabled)}`\n"
        f"{realloc_status} | Scoring: min R:R {config.get('scoring', {}).get('min_rr_ratio', 1.5)}"
    )
    notify._send(bot_token, chat_id, msg)


def main():
    parser = argparse.ArgumentParser(description="Wealth Builder Trading Bot")
    parser.add_argument("--once", action="store_true",
                        help="Run all enabled strategies once immediately")
    args = parser.parse_args()

    config = load_config()

    if args.once:
        print("[Bot] Running all strategies once (--once flag)...")
        _send_startup_alert(config)
        run_strategy(config)
        return

    schedule_cfg = config.get("schedule", {})
    timezone = schedule_cfg.get("timezone", "Australia/Sydney")

    scheduler = BlockingScheduler(timezone=pytz.timezone(timezone))

    # Add strategy-specific schedules
    strategies = config.get("strategies", {})
    for strat_name, strat_cfg in strategies.items():
        if not strat_cfg.get("enabled", False):
            continue
        sched = strat_cfg.get("schedule", {})
        run_times = sched.get("run_times", schedule_cfg.get("run_times", ["17:00"]))
        tz = sched.get("timezone", timezone)
        for time_str in run_times:
            hour, minute = map(int, time_str.split(":"))
            scheduler.add_job(
                run_strategy,
                trigger=CronTrigger(hour=hour, minute=minute, timezone=tz),
                args=[config],
                name=f"{strat_name}_{time_str}",
            )

    # Add reallocation job
    if config.get("reallocation", {}).get("enabled", False):
        deploy_strategy = config.get("reallocation", {}).get("deploy_strategy", "weekly")
        if deploy_strategy == "dip":
            # Daily check at 21:00 Sydney (post-ASX close) — only deploys on dip
            scheduler.add_job(
                run_reallocation,
                trigger=CronTrigger(hour=21, minute=0, timezone=timezone),
                args=[config],
                name="reallocation_dip_check",
            )
        else:
            # Weekly on Fridays 10:30 AM Sydney (original behavior)
            scheduler.add_job(
                run_reallocation,
                trigger=CronTrigger(day_of_week="fri", hour=10, minute=30, timezone=timezone),
                args=[config],
                name="reallocation_weekly",
            )

    # Add nightly counterfactual backfill (22:00 Sydney)
    scheduler.add_job(
        run_counterfactual_backfill,
        trigger=CronTrigger(hour=22, minute=0, timezone=timezone),
        args=[config],
        name="counterfactual_backfill",
    )

    # Add weekly feature health report (Sunday 20:00 Sydney)
    scheduler.add_job(
        run_feature_health_report,
        trigger=CronTrigger(day_of_week="sun", hour=20, minute=0, timezone=timezone),
        args=[config],
        name="feature_health_report",
    )

    _send_startup_alert(config)

    enabled = [n for n, c in strategies.items() if c.get("enabled", False)]
    print(f"[Bot] Scheduler started — strategies: {', '.join(enabled)}")
    print(f"[Bot] Mode: {config['mode'].upper()}")
    print("[Bot] Press Ctrl+C to stop.\n")

    try:
        scheduler.start()
    except KeyboardInterrupt:
        print("\n[Bot] Stopped by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
