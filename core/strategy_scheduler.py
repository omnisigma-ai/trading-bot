"""
Strategy Scheduler
------------------
Asyncio-native replacement for APScheduler. Schedules strategy runs
at configured times and supports continuous scanning during active sessions.

Two scheduling modes per strategy:
  1. Fixed schedule: run at specific times (e.g. "08:00", "17:00")
  2. Continuous scan: scan every N minutes during the strategy's active session
"""
import asyncio
from datetime import datetime, timedelta, time as dtime

import pytz

from ib_insync import IB

from config.loader import load_config
from core.continuous_monitor import ContinuousMonitor
from execution.ib_trader import IBTrader
from execution.stock_trader import StockTrader
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
from health_server import state as health_state


# ── Strategy Registry ─────────────────────────────────────────────────────

def _build_strategy_registry():
    """Build strategy registry, importing optional strategies safely."""
    registry = {
        "london_breakout": LondonBreakoutStrategy(),
        "momentum_stocks": MomentumStocksStrategy(),
    }
    try:
        from strategy.session_breakout import SessionBreakoutStrategy
        registry["asian_breakout"] = SessionBreakoutStrategy()
    except ImportError:
        pass
    try:
        from strategy.session_trend import SessionTrendStrategy
        registry["session_trend"] = SessionTrendStrategy()
    except ImportError:
        pass
    try:
        from strategy.futures_breakout import FuturesBreakoutStrategy
        registry["futures_breakout"] = FuturesBreakoutStrategy()
    except ImportError:
        pass
    try:
        from strategy.forex_mean_reversion import ForexMeanReversionStrategy
        registry["forex_mean_reversion"] = ForexMeanReversionStrategy()
    except ImportError:
        pass
    return registry


STRATEGIES = _build_strategy_registry()


# ── Session Utilities ─────────────────────────────────────────────────────

def _is_in_session(session_cfg: dict, sessions: dict) -> bool:
    """Check if current UTC time is within a session window."""
    session_name = session_cfg.get("session")
    if not session_name:
        return True  # no session restriction = always active

    session = sessions.get(session_name)
    if not session:
        return True

    now = datetime.utcnow().time()
    start = dtime.fromisoformat(session["start_utc"])
    end = dtime.fromisoformat(session["end_utc"])

    if start <= end:
        return start <= now <= end
    else:
        # Session spans midnight (e.g. asian: 22:00-06:00)
        return now >= start or now <= end


def _seconds_until(time_str: str, tz_name: str) -> float:
    """Seconds until the next occurrence of time_str in timezone."""
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    hour, minute = map(int, time_str.split(":"))
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return (target - now).total_seconds()


def _seconds_until_weekday(day_of_week: int, time_str: str, tz_name: str) -> float:
    """Seconds until next occurrence of day_of_week (0=Mon) at time_str."""
    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    hour, minute = map(int, time_str.split(":"))
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

    days_ahead = day_of_week - now.weekday()
    if days_ahead < 0 or (days_ahead == 0 and target <= now):
        days_ahead += 7
    target += timedelta(days=days_ahead)
    return (target - now).total_seconds()


class StrategyScheduler:
    """Asyncio-native strategy scheduling."""

    def __init__(
        self,
        ib: IB,
        monitor: ContinuousMonitor,
        config: dict,
        logger: TradeLogger,
        bot_token: str = "",
        chat_id: str = "",
    ):
        self.ib = ib
        self.monitor = monitor
        self.config = config
        self.logger = logger
        self.bot_token = bot_token
        self.chat_id = chat_id
        self._tasks: list[asyncio.Task] = []

    async def start(self) -> None:
        """Register all scheduled strategy runs and support jobs."""
        strategies = self.config.get("strategies", {})
        schedule_cfg = self.config.get("schedule", {})
        timezone = schedule_cfg.get("timezone", "Australia/Sydney")
        sessions = self.config.get("sessions", {})

        for strat_name, strat_cfg in strategies.items():
            if not strat_cfg.get("enabled", False):
                continue

            sched = strat_cfg.get("schedule", {})
            run_times = sched.get("run_times", schedule_cfg.get("run_times", ["17:00"]))
            tz = sched.get("timezone", timezone)

            # Fixed schedule: run at specific times
            for time_str in run_times:
                task = asyncio.create_task(
                    self._daily_loop(strat_name, time_str, tz),
                    name=f"sched_{strat_name}_{time_str}",
                )
                self._tasks.append(task)

            # Continuous scan: scan every N minutes during active session
            if strat_cfg.get("continuous_scan", False):
                interval = strat_cfg.get("scan_interval_minutes", 15)
                task = asyncio.create_task(
                    self._continuous_scan_loop(strat_name, strat_cfg, interval, sessions),
                    name=f"scan_{strat_name}",
                )
                self._tasks.append(task)

        # Schedule support jobs
        self._schedule_support_jobs(timezone)

        enabled = [n for n, c in strategies.items() if c.get("enabled", False)]
        print(f"[Scheduler] Strategies: {', '.join(enabled)}")
        print(f"[Scheduler] {len(self._tasks)} scheduled tasks running")

    def _schedule_support_jobs(self, timezone: str) -> None:
        """Schedule reallocation, counterfactual backfill, feature health."""
        realloc_cfg = self.config.get("reallocation", {})
        if realloc_cfg.get("enabled", False):
            deploy = realloc_cfg.get("deploy_strategy", "weekly")
            if deploy == "dip":
                task = asyncio.create_task(
                    self._daily_job_loop("reallocation", "21:00", timezone, self._run_reallocation),
                )
            else:
                task = asyncio.create_task(
                    self._weekly_job_loop("reallocation", 4, "10:30", timezone, self._run_reallocation),
                )
            self._tasks.append(task)

        # Nightly counterfactual backfill at 22:00
        task = asyncio.create_task(
            self._daily_job_loop("backfill", "22:00", timezone, self._run_backfill),
        )
        self._tasks.append(task)

        # Weekly feature health report (Sunday 20:00)
        task = asyncio.create_task(
            self._weekly_job_loop("feature_health", 6, "20:00", timezone, self._run_feature_health),
        )
        self._tasks.append(task)

    # ── Scheduling Loops ──────────────────────────────────────────────────

    async def _daily_loop(self, strat_name: str, time_str: str, tz: str) -> None:
        """Run a strategy at the same time every day."""
        while True:
            wait = _seconds_until(time_str, tz)
            print(f"[Scheduler] {strat_name} next run in {wait/3600:.1f}h ({time_str} {tz})")
            await asyncio.sleep(wait)
            await self._run_strategy_pipeline(strat_name)

    async def _continuous_scan_loop(
        self, strat_name: str, strat_cfg: dict,
        interval_min: int, sessions: dict,
    ) -> None:
        """Scan for setups every N minutes during active session."""
        while True:
            if _is_in_session(strat_cfg, sessions):
                await self._run_strategy_pipeline(strat_name)
            await asyncio.sleep(interval_min * 60)

    async def _daily_job_loop(
        self, name: str, time_str: str, tz: str, job_fn,
    ) -> None:
        """Run a support job at the same time every day."""
        while True:
            wait = _seconds_until(time_str, tz)
            await asyncio.sleep(wait)
            try:
                job_fn()
            except Exception as e:
                print(f"[Scheduler] {name} failed: {e}")

    async def _weekly_job_loop(
        self, name: str, day_of_week: int, time_str: str,
        tz: str, job_fn,
    ) -> None:
        """Run a support job weekly on a specific day."""
        while True:
            wait = _seconds_until_weekday(day_of_week, time_str, tz)
            await asyncio.sleep(wait)
            try:
                job_fn()
            except Exception as e:
                print(f"[Scheduler] {name} failed: {e}")

    # ── Strategy Pipeline ─────────────────────────────────────────────────

    async def _run_strategy_pipeline(self, strat_name: str) -> None:
        """Run the signal→score→execute pipeline for a single strategy.

        Runs synchronously in the main event loop thread so ib_insync
        sync methods (which internally use the event loop) work correctly.
        nest_asyncio allows re-entrant event loop calls.
        """
        self._pipeline_sync(strat_name)

    def _pipeline_sync(self, strat_name: str) -> None:
        """Synchronous strategy pipeline (runs in thread executor)."""
        config = self.config
        strategies = config.get("strategies", {})
        strat_cfg = strategies.get(strat_name, {})
        scoring_cfg = config.get("scoring", {})
        limits_cfg = config.get("risk_limits", {})
        tax_cfg = config.get("tax", {})

        session_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")
        health_state.record_strategy_run()

        print(f"\n[{datetime.now()}] Running {strat_name} | Session: {session_id}")

        if not self.ib.isConnected():
            print(f"[{strat_name}] IB not connected — skipping")
            return

        try:
            trader = IBTrader(self.ib)
            account_balance = trader.get_account_balance()

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

            self.monitor.update_rates(usdjpy, usd_aud_rate)

            # Check daily/weekly loss limits
            try:
                check_limits(
                    logger=self.logger,
                    account_balance=account_balance,
                    daily_loss_limit=limits_cfg.get("daily_loss_limit", 0.02),
                    weekly_loss_limit=limits_cfg.get("weekly_loss_limit", 0.05),
                    max_consecutive_losses=limits_cfg.get("max_consecutive_losses", 3),
                    min_account_balance=limits_cfg.get("min_account_balance", 0),
                )
            except LimitBreached as e:
                print(f"[Risk] Limit breached — skipping: {e}")
                notify.notify_error(self.bot_token, self.chat_id, str(e), fatal=False)
                return

            # Phase 1: Generate trade intents
            strategy = STRATEGIES.get(strat_name)
            if not strategy:
                print(f"[Scheduler] Unknown strategy: {strat_name}")
                return

            intents = strategy.generate(
                config=config, ib=self.ib, account_balance=account_balance,
            )

            if not intents:
                print(f"[{strat_name}] No signals")
                return

            print(f"[{strat_name}] {len(intents)} trade intents generated")

            # Phase 2: Score opportunities
            accepted, all_scores = filter_opportunities(
                intents=intents,
                min_score=scoring_cfg.get("min_asymmetry_score", 0.5),
                min_rr=scoring_cfg.get("min_rr_ratio", 1.5),
                min_ev=scoring_cfg.get("min_expected_value", 0.0),
                logger=self.logger,
                session_id=session_id,
            )

            for score in all_scores:
                print(f"[Score] {score.summary()}")
            notify.notify_score_report(self.bot_token, self.chat_id, all_scores)

            if not accepted:
                print(f"[{strat_name}] No intents passed scoring")
                return

            # Phase 3: Execute accepted intents
            stock_trader = StockTrader(self.ib)
            open_positions = trader.get_open_positions()
            now_utc = datetime.utcnow()

            for intent in accepted:
                try:
                    check_portfolio_risk(
                        logger=self.logger,
                        account_balance=account_balance,
                        new_risk_usd=intent.risk_dollars,
                        instrument_type=intent.instrument_type,
                        config=config,
                        open_positions=open_positions,
                    )
                except PortfolioRiskError as e:
                    print(f"[Risk] Portfolio limit: {e}")
                    notify.notify_error(self.bot_token, self.chat_id, f"Portfolio risk: {e}")
                    continue

                try:
                    if intent.is_forex:
                        self._execute_forex_intent(
                            intent, trader, now_utc, limits_cfg,
                            usdjpy, account_balance, session_id,
                        )
                    elif intent.is_stock:
                        self._execute_stock_intent(intent, stock_trader, now_utc)
                    elif intent.is_futures:
                        self._execute_futures_intent(intent, now_utc)
                except Exception as e:
                    print(f"[ERROR] {intent.symbol}: {e}")
                    notify.notify_error(self.bot_token, self.chat_id, f"{intent.symbol}: `{e}`")

        except Exception as e:
            health_state.record_error(str(e))
            print(f"[ERROR] {strat_name} pipeline failed: {e}")
            notify.notify_error(self.bot_token, self.chat_id, f"{strat_name}: `{e}`")

    def _execute_forex_intent(
        self, intent: TradeIntent, trader: IBTrader,
        now_utc, limits_cfg: dict, usdjpy: float,
        account_balance: float, session_id: str,
    ) -> None:
        """Execute a forex trade intent."""
        pair = intent.symbol
        strat_cfg = intent.metadata

        # Skip if position or pending order exists (IB positions + DB open trades)
        open_symbols = [p["symbol"] for p in trader.get_open_positions()]
        if pair[:3].upper() in open_symbols:
            print(f"[Exec] {pair}: Skipping — open position exists")
            return

        # Check DB for pending/unfilled orders on this pair
        db_open = self.logger.get_open_trades()
        db_pairs = {t.get("pair", "") for t in db_open}
        if pair in db_pairs:
            print(f"[Exec] {pair}: Skipping — pending order in DB")
            return

        # Process on SELL intent (BUY comes first in pair)
        if intent.direction == "BUY":
            return

        # Generate OCA bracket signals
        from data.data_fetcher import fetch_historical
        risk_pct = intent.risk_dollars / account_balance if account_balance > 0 else 0.01

        df = fetch_historical(pair, period="7d", interval="1h", ib=self.ib)
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

        # Commission check
        est_comm = estimate_commission(lot_size, limits_cfg.get("commission_per_lot", 2.0))
        max_comm_pct = limits_cfg.get("max_commission_pct", 0.10)
        is_viable, comm_pct = check_commission_viability(est_comm, risk_usd, max_comm_pct)
        if not is_viable:
            print(f"[Risk] {pair}: Commission too high ({comm_pct*100:.1f}%)")
            return

        # Place OCA bracket
        session_hours = strat_cfg.get("max_trade_duration_hours", 6)
        group = trader.place_oca_breakout(
            buy_signal=buy_signal, sell_signal=sell_signal,
            lot_size=lot_size, expire_hours=session_hours,
        )

        # Log trades
        group.buy_db_id = self.logger.log_trade_opened(
            pair=pair, direction="BUY",
            entry_price=buy_signal.entry, stop_loss=buy_signal.stop_loss,
            take_profit=buy_signal.take_profit, lot_size=lot_size,
            sl_pips=buy_signal.sl_pips, tp_pips=buy_signal.tp_pips,
            ib_order_id=group.buy_entry_id,
        )
        group.sell_db_id = self.logger.log_trade_opened(
            pair=pair, direction="SELL",
            entry_price=sell_signal.entry, stop_loss=sell_signal.stop_loss,
            take_profit=sell_signal.take_profit, lot_size=lot_size,
            sl_pips=sell_signal.sl_pips, tp_pips=sell_signal.tp_pips,
            ib_order_id=group.sell_entry_id,
        )

        # Log signal and execution events
        pip = PIP_SIZE.get(pair.upper(), 0.01)
        range_size = (buy_signal.range_high - buy_signal.range_low) / pip
        self.logger.log_signal(
            pair=pair, signal_date=now_utc.strftime("%Y-%m-%d"),
            signal_time_utc=now_utc.isoformat(),
            range_high=buy_signal.range_high, range_low=buy_signal.range_low,
            range_size_pips=round(range_size, 1),
            buy_entry=buy_signal.entry, buy_sl=buy_signal.stop_loss, buy_tp=buy_signal.take_profit,
            sell_entry=sell_signal.entry, sell_sl=sell_signal.stop_loss, sell_tp=sell_signal.take_profit,
            sl_pips=buy_signal.sl_pips, tp_pips=buy_signal.tp_pips,
            traded=True, trade_id=group.buy_db_id,
        )

        for label, oid, sig in [
            ("BUY", group.buy_entry_id, buy_signal),
            ("SELL", group.sell_entry_id, sell_signal),
        ]:
            db_id = group.buy_db_id if label == "BUY" else group.sell_db_id
            self.logger.log_execution_event(
                trade_id=db_id, ib_order_id=oid,
                event_type="PLACED", event_time=datetime.utcnow().isoformat(),
                order_type="ENTRY", price=sig.entry,
                quantity=round(lot_size * 100_000),
                notes=f"{label} STOP LMT @ {sig.entry}",
            )

        # Telegram
        for signal in [buy_signal, sell_signal]:
            notify.notify_order_placed(
                bot_token=self.bot_token, chat_id=self.chat_id,
                pair=pair, direction=signal.direction,
                entry=signal.entry, sl=signal.stop_loss, tp=signal.take_profit,
                sl_pips=signal.sl_pips, tp_pips=signal.tp_pips,
                lot_size=lot_size, risk_usd=risk_usd,
            )

        # Register with continuous monitor
        self.monitor.add_breakout_group(group)

    def _execute_stock_intent(
        self, intent: TradeIntent, stock_trader: StockTrader, now_utc,
    ) -> None:
        """Execute a stock trade intent."""
        group = stock_trader.place_stock_bracket(intent)

        group.db_trade_id = self.logger.log_trade_opened(
            pair=intent.symbol, direction=intent.direction,
            entry_price=intent.entry_price, stop_loss=intent.stop_loss,
            take_profit=intent.take_profit, lot_size=intent.quantity,
            sl_pips=0, tp_pips=0, ib_order_id=group.entry_order_id,
        )

        # Register with exit manager via monitor
        self.monitor.exit_manager.register_trade(
            trade_db_id=group.db_trade_id, symbol=intent.symbol,
            strategy=intent.strategy, side=intent.direction,
            entry_price=intent.entry_price, stop_loss=intent.stop_loss,
            take_profit=intent.take_profit, quantity=intent.quantity,
            exit_strategy=intent.exit_strategy,
            trailing_config=intent.trailing_config,
            partial_exits=intent.partial_exits,
        )

        self.logger.log_execution_event(
            trade_id=group.db_trade_id, ib_order_id=group.entry_order_id,
            event_type="PLACED", event_time=datetime.utcnow().isoformat(),
            order_type="ENTRY", price=intent.entry_price,
            quantity=intent.quantity,
            notes=f"{intent.direction} {int(intent.quantity)} shares @ ${intent.entry_price:.2f}",
        )

        notify.notify_stock_order(
            self.bot_token, self.chat_id,
            symbol=intent.symbol, direction=intent.direction,
            shares=int(intent.quantity), entry=intent.entry_price,
            sl=intent.stop_loss, tp=intent.take_profit,
            risk_usd=intent.risk_dollars, strategy=intent.strategy,
        )

        self.monitor.add_stock_group(group)

    def _execute_futures_intent(
        self, intent: TradeIntent, now_utc,
    ) -> None:
        """Execute a futures trade intent."""
        from execution.futures_trader import FuturesTrader

        futures_trader = FuturesTrader(self.ib)
        group = futures_trader.place_futures_bracket(intent)

        group.db_trade_id = self.logger.log_trade_opened(
            pair=intent.symbol, direction=intent.direction,
            entry_price=intent.entry_price, stop_loss=intent.stop_loss,
            take_profit=intent.take_profit, lot_size=intent.quantity,
            sl_pips=0, tp_pips=0, ib_order_id=group.entry_order_id,
        )

        # Register with exit manager via monitor
        self.monitor.exit_manager.register_trade(
            trade_db_id=group.db_trade_id, symbol=intent.symbol,
            strategy=intent.strategy, side=intent.direction,
            entry_price=intent.entry_price, stop_loss=intent.stop_loss,
            take_profit=intent.take_profit, quantity=intent.quantity,
            exit_strategy=intent.exit_strategy,
            trailing_config=intent.trailing_config,
        )

        self.logger.log_execution_event(
            trade_id=group.db_trade_id, ib_order_id=group.entry_order_id,
            event_type="PLACED", event_time=datetime.utcnow().isoformat(),
            order_type="ENTRY", price=intent.entry_price,
            quantity=int(intent.quantity),
            notes=f"{intent.direction} {int(intent.quantity)} {intent.symbol} @ {intent.entry_price:.2f}",
        )

        notify.notify_futures_order(
            self.bot_token, self.chat_id,
            symbol=intent.symbol, direction=intent.direction,
            contracts=int(intent.quantity), entry=intent.entry_price,
            sl=intent.stop_loss, tp=intent.take_profit,
            risk_usd=intent.risk_dollars, strategy=intent.strategy,
        )

        # Reuse stock group tracking for fill monitoring
        self.monitor.add_stock_group(group)

    # ── Support Jobs ──────────────────────────────────────────────────────

    def _run_reallocation(self) -> None:
        """Execute pending ETF purchases (runs in executor)."""
        from execution.reallocation import ProfitReallocator
        from execution.ib_trader import IBTrader

        config = self.config
        if not self.ib.isConnected():
            return

        trader = IBTrader(self.ib)
        logger = TradeLogger(config.get("db_path", "logs/trades.db"))

        try:
            reallocator = ProfitReallocator(
                ib=self.ib, logger=logger, config=config,
                bot_token=self.bot_token, chat_id=self.chat_id,
            )
            reallocator.session_id = datetime.utcnow().strftime("%Y-%m-%dT%H:%M")

            try:
                audusd = trader.get_current_price("AUDUSD")
                usd_aud_rate = round(1.0 / audusd, 4)
            except Exception:
                usd_aud_rate = config.get("tax", {}).get("default_usd_aud_rate", 1.54)

            purchases = reallocator.execute_pending_purchases(usd_aud_rate)
            if purchases:
                print(f"[Realloc] Completed {len(purchases)} ETF purchases")

        except Exception as e:
            print(f"[Realloc] Error: {e}")
            notify.notify_error(self.bot_token, self.chat_id, f"Reallocation error: {e}")
        finally:
            logger.close()

    def _run_backfill(self) -> None:
        """Nightly counterfactual backfill (runs in executor)."""
        import json
        from data.data_fetcher import fetch_historical

        db_path = self.config.get("db_path", "logs/trades.db")
        logger = TradeLogger(db_path)

        try:
            rejects = logger.get_unbackfilled_rejects(max_age_days=7)
            if not rejects:
                return

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

                    df = fetch_historical(symbol, period="7d", interval="1h")
                    if df is None or df.empty:
                        continue

                    high_reached = df["High"].max()
                    low_reached = df["Low"].min()

                    entry_hit = entry <= high_reached and entry >= low_reached
                    if not entry_hit:
                        logger.update_counterfactual(r["id"], "no_entry", None)
                        backfilled += 1
                        continue

                    is_buy = tp > entry
                    if is_buy:
                        tp_hit = high_reached >= tp
                        sl_hit = low_reached <= sl
                    else:
                        tp_hit = low_reached <= tp
                        sl_hit = high_reached >= sl

                    if tp_hit and not sl_hit:
                        logger.update_counterfactual(r["id"], "would_profit", round(abs(tp - entry), 5))
                    elif sl_hit and not tp_hit:
                        logger.update_counterfactual(r["id"], "would_loss", round(-abs(sl - entry), 5))
                    elif tp_hit and sl_hit:
                        logger.update_counterfactual(r["id"], "ambiguous", None)

                    backfilled += 1
                except Exception as e:
                    print(f"[Backfill] Error #{r['id']}: {e}")

            if backfilled:
                print(f"[Backfill] Updated {backfilled}/{len(rejects)} counterfactuals")

        except Exception as e:
            print(f"[Backfill] Error: {e}")
        finally:
            logger.close()

    def _run_feature_health(self) -> None:
        """Weekly feature health report (runs in executor)."""
        db_path = self.config.get("db_path", "logs/trades.db")
        logger = TradeLogger(db_path)

        try:
            scores, diagnostics = get_summary_report(logger, lookback_days=90)
            if scores:
                notify.notify_feature_health(self.bot_token, self.chat_id, scores, diagnostics)
                print(f"[FeatureHealth] Report sent — {len(scores)} features")
        except Exception as e:
            print(f"[FeatureHealth] Error: {e}")
            notify.notify_error(self.bot_token, self.chat_id, f"Feature health failed: {e}")
        finally:
            logger.close()
