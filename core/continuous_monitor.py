"""
Continuous Monitor
------------------
Always-on position and order monitoring that runs 24/7.

Combines:
- Fill/cancel event tracking (from TradeMonitor logic)
- ExitManager evaluation on every tick (trailing stops, breakeven, partials)
- P&L alerts every 5 pips of movement
- Periodic position sync from IB

Replaces the session-scoped TradeMonitor + standalone position_watcher.py.
"""
import asyncio
from datetime import datetime

from ib_insync import IB, Fill, Trade, Ticker, Forex, Contract

from execution.ib_trader import BreakoutOrderGroup
from execution.stock_trader import StockOrderGroup
from exits.exit_manager import ExitManager, ExitAction
from logs.trade_logger import TradeLogger
from notifications import telegram_notifier as notify
from strategy.london_breakout import PIP_SIZE
from risk.position_sizer import pip_value_per_lot


class ContinuousMonitor:
    """24/7 position and order monitoring with exit strategy execution."""

    def __init__(
        self,
        ib: IB,
        logger: TradeLogger,
        config: dict,
        bot_token: str = "",
        chat_id: str = "",
    ):
        self.ib = ib
        self.logger = logger
        self.config = config
        self.bot_token = bot_token
        self.chat_id = chat_id

        self.exit_manager = ExitManager(config)

        # Order tracking maps (same structure as TradeMonitor)
        self._entry_ids: dict[int, BreakoutOrderGroup] = {}
        self._entry_side: dict[int, str] = {}
        self._tp_to_entry: dict[int, int] = {}
        self._sl_to_entry: dict[int, int] = {}
        self._filled_entries: set[int] = set()
        self._resolved_entries: set[int] = set()

        # Stock order tracking
        self._stock_groups: dict[int, StockOrderGroup] = {}

        # P&L tracking
        self._open_positions: dict[int, dict] = {}
        self._last_pnl_alert: dict[str, float] = {}
        self._pnl_alert_threshold = 5.0

        # Market data subscriptions
        self._subscribed_contracts: dict[str, Contract] = {}

        # SL order tracking for exit actions
        self._sl_order_ids: dict[int, int] = {}  # trade_db_id → IB SL order_id

        # Exchange rate cache
        self._quote_per_usd = 150.0
        self._usd_aud_rate = 1.54

    def start(self) -> None:
        """Register persistent IB event handlers."""
        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.orderStatusEvent += self._on_order_status
        self.ib.pendingTickersEvent += self._on_ticker_update
        print("[Monitor] Continuous monitoring started")

        # Start background tasks
        asyncio.get_event_loop().create_task(self._position_sync_loop())
        asyncio.get_event_loop().create_task(self._position_update_loop())

    def update_rates(self, quote_per_usd: float, usd_aud_rate: float) -> None:
        """Update exchange rate cache (called by strategy scheduler)."""
        self._quote_per_usd = quote_per_usd
        self._usd_aud_rate = usd_aud_rate

    def add_breakout_group(self, group: BreakoutOrderGroup) -> None:
        """Register a new forex OCA breakout order group for monitoring."""
        self._entry_ids[group.buy_entry_id] = group
        self._entry_side[group.buy_entry_id] = "BUY"
        self._tp_to_entry[group.buy_tp_id] = group.buy_entry_id
        self._sl_to_entry[group.buy_sl_id] = group.buy_entry_id
        self._sl_order_ids[group.buy_db_id] = group.buy_sl_id

        self._entry_ids[group.sell_entry_id] = group
        self._entry_side[group.sell_entry_id] = "SELL"
        self._tp_to_entry[group.sell_tp_id] = group.sell_entry_id
        self._sl_to_entry[group.sell_sl_id] = group.sell_entry_id
        self._sl_order_ids[group.sell_db_id] = group.sell_sl_id

    def add_stock_group(self, group: StockOrderGroup) -> None:
        """Register a stock order group for monitoring."""
        if hasattr(group, 'entry_order_id'):
            self._stock_groups[group.entry_order_id] = group
            if hasattr(group, 'sl_order_id') and hasattr(group, 'db_trade_id'):
                self._sl_order_ids[group.db_trade_id] = group.sl_order_id

    async def recover_existing_positions(self) -> None:
        """On startup, reconcile DB open trades against IB state.

        Trades with matching IB orders/positions are recovered.
        Orphaned DB trades (no IB order) are marked as expired.
        """
        open_trades = self.logger.get_open_trades()
        if not open_trades:
            print("[Monitor] No open trades to recover")
            return

        # Build set of active IB order IDs
        ib_order_ids = set()
        for t in self.ib.openTrades():
            ib_order_ids.add(t.order.orderId)

        # Build set of IB position symbols
        ib_position_symbols = set()
        for pos in self.ib.positions():
            if pos.position != 0:
                ib_position_symbols.add(pos.contract.localSymbol.replace(".", ""))

        recovered = 0
        expired = 0
        for trade in open_trades:
            trade_id = trade["id"]
            pair = trade.get("pair", "")
            ib_order_id = trade.get("ib_order_id")

            # Check if this trade has a matching IB order or filled position
            has_ib_order = ib_order_id and ib_order_id in ib_order_ids
            has_ib_position = pair in ib_position_symbols

            if not has_ib_order and not has_ib_position:
                # Orphaned — mark as expired in DB
                self.logger.log_trade_closed(
                    trade_id, exit_price=0, result="EXPIRED_STALE",
                    pips=0, pnl_usd=0,
                )
                expired += 1
                continue

            try:
                self.exit_manager.register_trade(
                    trade_db_id=trade_id,
                    symbol=pair,
                    strategy=trade.get("strategy", "london_breakout"),
                    side=trade.get("direction", "BUY"),
                    entry_price=trade.get("fill_price") or trade.get("entry_price", 0),
                    stop_loss=trade.get("stop_loss", 0),
                    take_profit=trade.get("take_profit", 0),
                    quantity=trade.get("lot_size", 0),
                    exit_strategy=trade.get("exit_strategy", "fixed"),
                )
                print(f"[Monitor] Recovered trade #{trade_id}: {pair} {trade.get('direction')}")

                # Subscribe to market data for this pair
                if pair and pair not in self._subscribed_contracts:
                    await self._subscribe_pair(pair)

                recovered += 1
            except Exception as e:
                print(f"[Monitor] Failed to recover trade #{trade_id}: {e}")

        print(f"[Monitor] Recovered {recovered} trades, expired {expired} stale trades")

    async def _subscribe_pair(self, pair: str) -> None:
        """Subscribe to market data for a forex pair (async-safe)."""
        if pair in self._subscribed_contracts:
            return
        try:
            contract = Forex(pair=pair)
            await self.ib.qualifyContractsAsync(contract)
            self.ib.reqMktData(contract)
            self._subscribed_contracts[pair] = contract
            print(f"[Monitor] Subscribed to {pair} market data")
        except Exception as e:
            print(f"[Monitor] Failed to subscribe to {pair}: {e}")

    async def restore_subscriptions(self) -> None:
        """Re-subscribe to market data after reconnection."""
        pairs = list(self._subscribed_contracts.keys())
        self._subscribed_contracts.clear()
        for pair in pairs:
            await self._subscribe_pair(pair)
        print(f"[Monitor] Restored {len(pairs)} market data subscriptions")

    # ── IB Event Handlers ─────────────────────────────────────────────────

    def _on_exec_details(self, trade: Trade, fill: Fill) -> None:
        """Handle order fill events — entry fills, TP fills, SL fills."""
        order_id = fill.execution.orderId
        pair = fill.contract.localSymbol.replace(".", "")
        fill_price = fill.execution.price
        fill_time = fill.execution.time
        commission = fill.commissionReport.commission if fill.commissionReport else 0

        # Entry fill
        if order_id in self._entry_ids:
            self._handle_entry_fill(order_id, pair, fill_price, fill_time, commission)
            return

        # TP fill
        if order_id in self._tp_to_entry:
            entry_id = self._tp_to_entry[order_id]
            self._handle_tp_fill(entry_id, pair, fill_price, fill_time, commission)
            return

        # SL fill
        if order_id in self._sl_to_entry:
            entry_id = self._sl_to_entry[order_id]
            self._handle_sl_fill(entry_id, pair, fill_price, fill_time, commission)
            return

    def _handle_entry_fill(
        self, order_id: int, pair: str, fill_price: float,
        fill_time, commission: float,
    ) -> None:
        """Process an entry order fill."""
        group = self._entry_ids[order_id]
        side = self._entry_side[order_id]
        db_id = group.buy_db_id if side == "BUY" else group.sell_db_id
        signal = group.buy_signal if side == "BUY" else group.sell_signal

        self._filled_entries.add(order_id)

        # Calculate slippage
        expected = signal.entry
        pip_size = PIP_SIZE.get(pair.upper(), 0.0001)
        slippage_pips = abs(fill_price - expected) / pip_size

        # Update DB
        self.logger.log_trade_filled(
            trade_id=db_id,
            fill_price=fill_price,
            slippage_pips=round(slippage_pips, 2),
            commission=commission,
            fill_time=str(fill_time),
        )
        self.logger.log_execution_event(
            trade_id=db_id, ib_order_id=order_id,
            event_type="FILLED", event_time=str(fill_time),
            order_type="ENTRY", price=fill_price,
            notes=f"{side} filled @ {fill_price} (slip: {slippage_pips:.1f} pips)",
        )

        # Track position for P&L alerts
        self._open_positions[order_id] = {
            "pair": pair, "side": side, "entry_price": fill_price,
            "lot_size": group.lot_size, "db_id": db_id,
        }
        self._last_pnl_alert[pair] = 0.0

        # Subscribe to market data (schedule async from sync callback)
        asyncio.get_event_loop().create_task(self._subscribe_pair(pair))

        # Register with exit manager
        self.exit_manager.register_trade(
            trade_db_id=db_id, symbol=pair, strategy="london_breakout",
            side=side, entry_price=fill_price,
            stop_loss=signal.stop_loss, take_profit=signal.take_profit,
            quantity=group.lot_size, exit_strategy="trailing",
            trailing_config=self.config.get("strategies", {}).get(
                "london_breakout", {},
            ).get("trailing_stop"),
        )

        # Notify
        notify.notify_order_filled(self.bot_token, self.chat_id, pair, side, fill_price)
        print(f"[Monitor] FILLED: {pair} {side} @ {fill_price}")

    def _handle_tp_fill(
        self, entry_id: int, pair: str, fill_price: float,
        fill_time, commission: float,
    ) -> None:
        """Process a take-profit fill."""
        group = self._entry_ids[entry_id]
        side = self._entry_side[entry_id]
        db_id = group.buy_db_id if side == "BUY" else group.sell_db_id
        signal = group.buy_signal if side == "BUY" else group.sell_signal

        pip_size = PIP_SIZE.get(pair.upper(), 0.0001)
        pips = abs(fill_price - signal.entry) / pip_size
        pv = pip_value_per_lot(pair, self._quote_per_usd)
        pnl_usd = pips * pv * group.lot_size

        self.logger.log_trade_closed(db_id, fill_price, "TP", round(pips, 1), round(pnl_usd, 2))
        self.logger.log_execution_event(
            trade_id=db_id, ib_order_id=0,
            event_type="CLOSED", event_time=str(fill_time),
            order_type="TP", price=fill_price,
            notes=f"TP hit +{pips:.1f} pips (+${pnl_usd:.2f})",
        )

        self._resolved_entries.add(entry_id)
        self._open_positions.pop(entry_id, None)
        self.exit_manager.unregister_trade(db_id)

        notify.notify_tp_hit(self.bot_token, self.chat_id, pair, side, pips, pnl_usd)
        print(f"[Monitor] TP HIT: {pair} {side} +{pips:.1f} pips (+${pnl_usd:.2f})")

    def _handle_sl_fill(
        self, entry_id: int, pair: str, fill_price: float,
        fill_time, commission: float,
    ) -> None:
        """Process a stop-loss fill."""
        group = self._entry_ids[entry_id]
        side = self._entry_side[entry_id]
        db_id = group.buy_db_id if side == "BUY" else group.sell_db_id
        signal = group.buy_signal if side == "BUY" else group.sell_signal

        pip_size = PIP_SIZE.get(pair.upper(), 0.0001)
        pips = abs(fill_price - signal.entry) / pip_size
        pv = pip_value_per_lot(pair, self._quote_per_usd)
        pnl_usd = pips * pv * group.lot_size

        self.logger.log_trade_closed(db_id, fill_price, "SL", round(-pips, 1), round(-pnl_usd, 2))
        self.logger.log_execution_event(
            trade_id=db_id, ib_order_id=0,
            event_type="CLOSED", event_time=str(fill_time),
            order_type="SL", price=fill_price,
            notes=f"SL hit -{pips:.1f} pips (-${pnl_usd:.2f})",
        )

        self._resolved_entries.add(entry_id)
        self._open_positions.pop(entry_id, None)
        self.exit_manager.unregister_trade(db_id)

        notify.notify_sl_hit(self.bot_token, self.chat_id, pair, side, pips, pnl_usd)
        print(f"[Monitor] SL HIT: {pair} {side} -{pips:.1f} pips (-${pnl_usd:.2f})")

    def _on_order_status(self, trade: Trade) -> None:
        """Handle order status changes (cancellations, GTD expiry)."""
        order_id = trade.order.orderId
        status = trade.orderStatus.status

        if status != "Cancelled":
            return

        if order_id in self._entry_ids and order_id not in self._filled_entries:
            group = self._entry_ids[order_id]
            side = self._entry_side[order_id]
            db_id = group.buy_db_id if side == "BUY" else group.sell_db_id

            # Check if partner was filled (OCA cancellation) vs GTD expiry
            partner_id = group.sell_entry_id if side == "BUY" else group.buy_entry_id
            if partner_id in self._filled_entries:
                # OCA cancellation — partner filled, this is normal
                self.logger.log_execution_event(
                    trade_id=db_id, ib_order_id=order_id,
                    event_type="OCA_CANCELLED",
                    event_time=datetime.utcnow().isoformat(),
                    order_type="ENTRY",
                    notes=f"{side} cancelled (OCA — partner triggered)",
                )
                self.logger.log_trade_closed(db_id, exit_price=0, result="OCA_CANCEL", pips=0, pnl_usd=0)
            else:
                # GTD expiry — no trigger
                self.logger.log_execution_event(
                    trade_id=db_id, ib_order_id=order_id,
                    event_type="EXPIRED",
                    event_time=datetime.utcnow().isoformat(),
                    order_type="ENTRY",
                    notes="GTD expired — no trigger",
                )
                self.logger.log_trade_closed(db_id, exit_price=0, result="NO_TRIGGER", pips=0, pnl_usd=0)

            self._resolved_entries.add(order_id)

    def _on_ticker_update(self, tickers: list[Ticker]) -> None:
        """On every tick: P&L alerts + exit strategy evaluation."""
        for ticker in tickers:
            mid = ticker.midpoint()
            if not mid or mid != mid:  # NaN check
                continue

            contract = ticker.contract
            pair = contract.localSymbol.replace(".", "") if contract else ""
            if not pair:
                continue

            # P&L alerts for open positions
            self._check_pnl_alert(pair, mid)

            # Exit strategy evaluation (trailing stops, breakeven, partials)
            actions = self.exit_manager.on_tick(pair, mid)
            for action in actions:
                self._execute_exit_action(action)

    def _check_pnl_alert(self, pair: str, mid: float) -> None:
        """Send P&L alert every 5 pips of movement."""
        for entry_id, pos in self._open_positions.items():
            if pos["pair"] != pair:
                continue

            pip_size = PIP_SIZE.get(pair.upper(), 0.0001)
            if pos["side"] == "BUY":
                pips = (mid - pos["entry_price"]) / pip_size
            else:
                pips = (pos["entry_price"] - mid) / pip_size

            prev = self._last_pnl_alert.get(pair, 0.0)
            if abs(pips - prev) >= self._pnl_alert_threshold:
                pv = pip_value_per_lot(pair, self._quote_per_usd)
                pnl_usd = pips * pv * pos["lot_size"]
                sign = "+" if pips >= 0 else ""
                arrow = "\U0001f7e2" if pips >= 0 else "\U0001f534"
                msg = (
                    f"{arrow} *P&L UPDATE* \u2014 {pair} `{pos['side']}`\n"
                    f"Entry: `{pos['entry_price']:.5f}` | Now: `{mid:.5f}`\n"
                    f"{sign}{pips:.1f} pips | {sign}${pnl_usd:.2f}"
                )
                notify._send(self.bot_token, self.chat_id, msg)
                self._last_pnl_alert[pair] = pips

    def _execute_exit_action(self, action: ExitAction) -> None:
        """Execute an exit action by modifying orders on IB."""
        if action.action_type == "modify_stop":
            sl_order_id = self._sl_order_ids.get(action.trade_db_id)
            if sl_order_id is None:
                print(f"[Monitor] Cannot modify SL — no order ID for trade #{action.trade_db_id}")
                return

            # Find the existing order and modify it
            for trade in self.ib.openTrades():
                if trade.order.orderId == sl_order_id:
                    trade.order.auxPrice = action.new_stop
                    self.ib.placeOrder(trade.contract, trade.order)
                    print(f"[Monitor] Modified SL for trade #{action.trade_db_id}: {action.new_stop} ({action.reason})")

                    # Get the symbol for notification
                    state = self.exit_manager.active_trades.get(action.trade_db_id)
                    symbol = state.symbol if state else "?"
                    notify.notify_exit_action(
                        self.bot_token, self.chat_id,
                        symbol=symbol, action="modify_stop",
                        details=f"SL moved to `{action.new_stop:.5f}` ({action.reason})",
                    )
                    break

        elif action.action_type == "partial_close":
            state = self.exit_manager.active_trades.get(action.trade_db_id)
            if not state:
                return

            # Schedule async partial close (called from sync ticker callback)
            asyncio.get_event_loop().create_task(
                self._partial_close_async(action, state)
            )

    async def _partial_close_async(self, action: ExitAction, state) -> None:
        """Execute a partial close using async IB methods."""
        try:
            contract = Forex(pair=state.symbol)
            await self.ib.qualifyContractsAsync(contract)
            from ib_insync import Order as IBOrder
            close_action = "SELL" if state.side == "BUY" else "BUY"
            order = IBOrder(
                action=close_action,
                totalQuantity=action.close_quantity,
                orderType="MKT",
            )
            self.ib.placeOrder(contract, order)
            print(
                f"[Monitor] Partial close {action.close_pct}% of "
                f"{state.symbol} ({action.reason})"
            )
            notify.notify_exit_action(
                self.bot_token, self.chat_id,
                symbol=state.symbol, action="partial_close",
                details=f"Closed {action.close_pct}% ({action.reason})",
            )
        except Exception as e:
            print(f"[Monitor] Partial close failed: {e}")

    # ── Background Tasks ──────────────────────────────────────────────────

    async def _position_sync_loop(self) -> None:
        """Every 60s, sync positions from IB to catch any drift."""
        while True:
            await asyncio.sleep(60)
            try:
                if not self.ib.isConnected():
                    continue
                positions = self.ib.positions()
                open_count = sum(1 for p in positions if p.position != 0)
                if open_count > 0:
                    # Ensure we're subscribed to market data for all open positions
                    for pos in positions:
                        if pos.position != 0:
                            pair = pos.contract.localSymbol.replace(".", "")
                            if pair and pair not in self._subscribed_contracts:
                                await self._subscribe_pair(pair)
            except Exception:
                pass

    async def _position_update_loop(self) -> None:
        """Every 15 minutes, send a rich health check to Telegram."""
        while True:
            await asyncio.sleep(15 * 60)
            try:
                if not self.ib.isConnected():
                    continue

                # Gather IB positions with unrealised P&L from portfolio()
                positions = []
                for item in self.ib.portfolio():
                    if item.position == 0:
                        continue
                    pair = item.contract.localSymbol.replace(".", "")
                    side = "LONG" if item.position > 0 else "SHORT"
                    qty = abs(item.position)
                    pos_pnl = item.unrealizedPNL or 0.0

                    positions.append({
                        "pair": pair,
                        "side": side,
                        "qty": qty,
                        "unrealised_pnl": pos_pnl,
                    })

                # Account values — AUD-denominated account
                bal = 0.0
                unrealised = 0.0
                currency = "AUD"
                for av in self.ib.accountValues():
                    if av.tag == "NetLiquidation" and av.currency == "AUD":
                        try:
                            bal = float(av.value)
                        except ValueError:
                            pass
                    elif av.tag == "NetLiquidation" and av.currency == "USD" and bal == 0:
                        try:
                            bal = float(av.value)
                            currency = "USD"
                        except ValueError:
                            pass
                    if av.tag == "UnrealizedPnL" and av.currency == "AUD":
                        try:
                            unrealised = float(av.value)
                        except ValueError:
                            pass

                # DB queries
                daily_pnl = self.logger.get_today_pnl()
                weekly_pnl = self.logger.get_weekly_pnl()
                all_time = self.logger.get_all_time_pnl()
                top_wins, top_losses = self.logger.get_top_trades(5)
                open_trades = self.logger.get_open_trades()

                notify.notify_health_check(
                    self.bot_token, self.chat_id,
                    account_balance=bal,
                    unrealised_pnl=unrealised,
                    positions=positions,
                    daily_pnl=daily_pnl,
                    weekly_pnl=weekly_pnl,
                    all_time=all_time,
                    top_wins=top_wins,
                    top_losses=top_losses,
                    open_trade_count=len(open_trades),
                    pending_order_count=len(open_trades) - len(positions),
                    currency=currency,
                )
            except Exception as e:
                print(f"[Monitor] Health check failed: {e}")
