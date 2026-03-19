"""
Trade Monitor
-------------
Subscribes to IB order events after placing breakout orders.
Provides the feedback loop:

  Entry fills   → notify "ORDER FILLED", update DB with fill quality
  TP fills      → notify "TP HIT", close trade in DB
  SL fills      → notify "SL HIT", close trade in DB
  GTD expires   → notify "NO TRIGGER", cancel trade in DB
  End of session→ send daily summary

Captures: commissions, slippage, fill timestamps, AUD conversion,
and logs all events to the execution_events audit trail.

Uses ib_insync's event system (execDetailsEvent + orderStatusEvent +
commissionReportEvent). The IB connection stays alive after order
placement via ib.sleep(), which runs the event loop while waiting.
"""
from datetime import datetime, timedelta

from ib_insync import IB, Fill, Trade, Ticker

from execution.ib_trader import BreakoutOrderGroup
from logs.trade_logger import TradeLogger
from notifications import telegram_notifier as notify
from strategy.london_breakout import PIP_SIZE
from risk.position_sizer import pip_value_per_lot


def _settlement_date(trade_date: datetime) -> str:
    """Calculate T+2 settlement date (skip weekends)."""
    days_added = 0
    current = trade_date
    while days_added < 2:
        current += timedelta(days=1)
        if current.weekday() < 5:  # Mon-Fri
            days_added += 1
    return current.strftime("%Y-%m-%d")


class TradeMonitor:
    def __init__(
        self,
        ib: IB,
        logger: TradeLogger,
        bot_token: str,
        chat_id: str,
        order_groups: list[BreakoutOrderGroup],
        quote_per_usd: float = 150.0,
        usd_aud_rate: float = 1.54,
    ):
        self.ib = ib
        self.logger = logger
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.quote_per_usd = quote_per_usd
        self.usd_aud_rate = usd_aud_rate

        # Build lookup maps from order ID → role info
        self._entry_ids: dict[int, BreakoutOrderGroup] = {}   # entry order id → group
        self._entry_side: dict[int, str] = {}                 # entry order id → 'BUY'|'SELL'
        self._tp_to_entry: dict[int, int] = {}                # tp order id → entry order id
        self._sl_to_entry: dict[int, int] = {}                # sl order id → entry order id
        self._filled_entries: set[int] = set()                # entry IDs that got filled
        self._resolved_entries: set[int] = set()              # entry IDs fully closed (TP/SL/cancel)

        for group in order_groups:
            self._entry_ids[group.buy_entry_id] = group
            self._entry_side[group.buy_entry_id] = "BUY"
            self._tp_to_entry[group.buy_tp_id] = group.buy_entry_id
            self._sl_to_entry[group.buy_sl_id] = group.buy_entry_id

            self._entry_ids[group.sell_entry_id] = group
            self._entry_side[group.sell_entry_id] = "SELL"
            self._tp_to_entry[group.sell_tp_id] = group.sell_entry_id
            self._sl_to_entry[group.sell_sl_id] = group.sell_entry_id

        self._daily_results: list[dict] = []

        # Live P&L tracking for open positions
        self._open_positions: dict[int, dict] = {}  # entry_id → {pair, side, entry_price, lot_size, last_pnl_pips}
        self._pnl_alert_threshold = 5.0  # alert every 5 pips of movement
        self._subscribed_pairs: set[str] = set()

    def start(self) -> None:
        """Register IB event callbacks."""
        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.orderStatusEvent += self._on_order_status
        self.ib.pendingTickersEvent += self._on_ticker_update
        print("[Monitor] Trade monitor started — watching for fills...")

    def stop(self) -> None:
        """Unregister callbacks and send daily summary."""
        self.ib.execDetailsEvent -= self._on_exec_details
        self.ib.orderStatusEvent -= self._on_order_status
        self.ib.pendingTickersEvent -= self._on_ticker_update

        # Any entries still not resolved by now = no trigger (GTD expired)
        for entry_id, group in self._entry_ids.items():
            if entry_id not in self._resolved_entries:
                side = self._entry_side[entry_id]
                db_id = group.buy_db_id if side == "BUY" else group.sell_db_id
                self.logger.log_trade_closed(db_id, exit_price=0, result="NO_TRIGGER", pips=0, pnl_usd=0)
                self.logger.log_execution_event(
                    trade_id=db_id, ib_order_id=entry_id,
                    event_type="EXPIRED", event_time=datetime.utcnow().isoformat(),
                    order_type="ENTRY", notes="GTD expired — no trigger",
                )
                self._resolved_entries.add(entry_id)

        # Send daily summary (deduplicate by pair — only report the triggered side)
        seen_pairs = set()
        for r in self._daily_results:
            if r["pair"] not in seen_pairs:
                seen_pairs.add(r["pair"])
        notify.notify_daily_summary(self.bot_token, self.chat_id, self._daily_results)
        print("[Monitor] Trade monitor stopped.")

    # ── event handlers ────────────────────────────────────────────────────────

    def _on_exec_details(self, trade: Trade, fill: Fill) -> None:
        """Fired every time an order gets a fill execution."""
        order_id = trade.order.orderId
        fill_price = fill.execution.price
        fill_time = fill.execution.time.isoformat() if fill.execution.time else datetime.utcnow().isoformat()

        # Extract commission (guard against IB's 1e10 placeholder)
        commission = 0.0
        commission_currency = "USD"
        if fill.commissionReport and fill.commissionReport.commission < 1e6:
            commission = fill.commissionReport.commission
            commission_currency = fill.commissionReport.currency or "USD"

        # Entry filled → position just opened
        if order_id in self._entry_ids:
            group = self._entry_ids[order_id]
            side = self._entry_side[order_id]
            signal = group.buy_signal if side == "BUY" else group.sell_signal
            db_id = group.buy_db_id if side == "BUY" else group.sell_db_id
            self._filled_entries.add(order_id)

            # Calculate slippage
            pip = PIP_SIZE.get(group.pair.upper(), 0.01)
            if side == "BUY":
                slippage_pips = (fill_price - signal.entry) / pip
            else:
                slippage_pips = (signal.entry - fill_price) / pip

            # Update trade record with fill quality
            self.logger.update_entry_fill(
                trade_id=db_id,
                fill_price=fill_price,
                slippage_pips=round(slippage_pips, 2),
                commission_entry=commission,
                entry_fill_time=fill_time,
            )

            # Audit trail
            self.logger.log_execution_event(
                trade_id=db_id, ib_order_id=order_id,
                event_type="FILLED", event_time=fill_time,
                order_type="ENTRY", price=fill_price,
                quantity=fill.execution.shares,
                commission=commission, commission_currency=commission_currency,
                ib_exec_id=str(fill.execution.execId) if fill.execution.execId else None,
            )

            notify.notify_order_filled(self.bot_token, self.chat_id, group.pair, side, fill_price)
            print(f"[Monitor] Entry filled: {group.pair} {side} @ {fill_price} (slippage: {slippage_pips:+.1f} pips)")

            # Start live P&L tracking for this position
            self._open_positions[order_id] = {
                "pair": group.pair, "side": side,
                "entry_price": fill_price, "lot_size": group.lot_size,
                "last_alert_pips": 0.0,
            }
            self._subscribe_price(group.pair)

        # TP filled → trade won
        elif order_id in self._tp_to_entry:
            entry_id = self._tp_to_entry[order_id]
            group = self._entry_ids[entry_id]
            side = self._entry_side[entry_id]
            signal = group.buy_signal if side == "BUY" else group.sell_signal
            db_id = group.buy_db_id if side == "BUY" else group.sell_db_id

            pips, pnl = self._calc_pnl(side, signal.entry, fill_price, group.pair, group.lot_size)
            pnl_aud = round(pnl * self.usd_aud_rate, 2)
            settle = _settlement_date(datetime.utcnow())

            self.logger.log_trade_closed(
                db_id, fill_price, "TP", pips, pnl,
                commission_exit=commission, exit_fill_time=fill_time,
                pnl_aud=pnl_aud, usd_aud_rate=self.usd_aud_rate,
                settlement_date=settle,
            )
            self.logger.log_execution_event(
                trade_id=db_id, ib_order_id=order_id,
                event_type="FILLED", event_time=fill_time,
                order_type="TP", price=fill_price,
                quantity=fill.execution.shares,
                commission=commission, commission_currency=commission_currency,
                ib_exec_id=str(fill.execution.execId) if fill.execution.execId else None,
            )

            notify.notify_tp_hit(self.bot_token, self.chat_id, group.pair, side, pips, pnl)
            self._resolved_entries.add(entry_id)
            self._open_positions.pop(entry_id, None)
            self._daily_results.append({"pair": group.pair, "result": "TP", "pips": pips, "pnl_usd": pnl})
            print(f"[Monitor] TP hit: {group.pair} {side} @ {fill_price} | +{pips:.1f} pips | +${pnl:.2f} (A${pnl_aud:.2f})")

        # SL filled → trade lost
        elif order_id in self._sl_to_entry:
            entry_id = self._sl_to_entry[order_id]
            group = self._entry_ids[entry_id]
            side = self._entry_side[entry_id]
            signal = group.buy_signal if side == "BUY" else group.sell_signal
            db_id = group.buy_db_id if side == "BUY" else group.sell_db_id

            pips, pnl = self._calc_pnl(side, signal.entry, fill_price, group.pair, group.lot_size)
            pnl_aud = round(pnl * self.usd_aud_rate, 2)
            settle = _settlement_date(datetime.utcnow())

            self.logger.log_trade_closed(
                db_id, fill_price, "SL", abs(pips), pnl,
                commission_exit=commission, exit_fill_time=fill_time,
                pnl_aud=pnl_aud, usd_aud_rate=self.usd_aud_rate,
                settlement_date=settle,
            )
            self.logger.log_execution_event(
                trade_id=db_id, ib_order_id=order_id,
                event_type="FILLED", event_time=fill_time,
                order_type="SL", price=fill_price,
                quantity=fill.execution.shares,
                commission=commission, commission_currency=commission_currency,
                ib_exec_id=str(fill.execution.execId) if fill.execution.execId else None,
            )

            notify.notify_sl_hit(self.bot_token, self.chat_id, group.pair, side, abs(pips), pnl)
            self._resolved_entries.add(entry_id)
            self._open_positions.pop(entry_id, None)
            self._daily_results.append({"pair": group.pair, "result": "SL", "pips": abs(pips), "pnl_usd": pnl})
            print(f"[Monitor] SL hit: {group.pair} {side} @ {fill_price} | {pips:.1f} pips | ${pnl:.2f} (A${pnl_aud:.2f})")

    def _on_order_status(self, trade: Trade) -> None:
        """Fired when an order's status changes (e.g. Cancelled)."""
        order_id = trade.order.orderId
        status = trade.orderStatus.status

        if status != "Cancelled":
            return

        # Entry cancelled — was it OCA-cancelled (partner filled) or GTD expired?
        if order_id in self._entry_ids:
            if order_id in self._filled_entries:
                return  # this entry was the one that filled — ignore its own cancel event

            if order_id in self._resolved_entries:
                return  # already handled

            # Check if the OCA partner of this entry was filled
            group = self._entry_ids[order_id]
            side = self._entry_side[order_id]
            partner_id = group.sell_entry_id if side == "BUY" else group.buy_entry_id
            db_id = group.buy_db_id if side == "BUY" else group.sell_db_id

            if partner_id in self._filled_entries:
                # OCA cancellation — the other side triggered, this is normal
                self.logger.log_execution_event(
                    trade_id=db_id, ib_order_id=order_id,
                    event_type="CANCELLED", event_time=datetime.utcnow().isoformat(),
                    order_type="ENTRY", notes="OCA cancelled — partner filled",
                )
                self._resolved_entries.add(order_id)
                return

            # Neither side filled and this entry is cancelled → GTD expired = no trigger today
            self.logger.log_trade_closed(db_id, exit_price=0, result="NO_TRIGGER", pips=0, pnl_usd=0)
            self.logger.log_execution_event(
                trade_id=db_id, ib_order_id=order_id,
                event_type="EXPIRED", event_time=datetime.utcnow().isoformat(),
                order_type="ENTRY", notes="GTD expired — no breakout",
            )
            notify.notify_no_signal(self.bot_token, self.chat_id, group.pair, "no breakout today")
            self._resolved_entries.add(order_id)
            self._daily_results.append({"pair": group.pair, "result": "NO_SIGNAL", "pips": 0, "pnl_usd": 0})
            print(f"[Monitor] No trigger: {group.pair} {side} order expired")

    # ── helpers ───────────────────────────────────────────────────────────────

    def _subscribe_price(self, pair: str) -> None:
        """Subscribe to real-time price updates for a pair (async-safe)."""
        if pair in self._subscribed_pairs:
            return
        from ib_insync import Forex
        symbol = pair[:3].upper()
        currency = pair[3:].upper()
        contract = Forex(pair=f"{symbol}{currency}")
        # Use async version to avoid 'event loop already running' error
        import asyncio
        loop = asyncio.get_event_loop()
        loop.create_task(self._async_subscribe(contract, pair))

    async def _async_subscribe(self, contract, pair: str) -> None:
        """Async helper to subscribe to market data."""
        try:
            await self.ib.qualifyContractsAsync(contract)
            self.ib.reqMktData(contract)
            self._subscribed_pairs.add(pair)
            print(f"[Monitor] Subscribed to {pair} price updates")
        except Exception as e:
            print(f"[Monitor] Failed to subscribe to {pair}: {e}")

    def _on_ticker_update(self, tickers: set[Ticker]) -> None:
        """Fired on price updates — check P&L for open positions."""
        if not self._open_positions:
            return

        for ticker in tickers:
            if not ticker.midpoint() or ticker.midpoint() != ticker.midpoint():
                continue
            mid = ticker.midpoint()
            pair_symbol = ticker.contract.localSymbol.replace(".", "")  # "AUD.USD" → "AUDUSD"

            for entry_id, pos in list(self._open_positions.items()):
                if pos["pair"].upper() != pair_symbol.upper():
                    continue

                pip = PIP_SIZE.get(pos["pair"].upper(), 0.0001)
                if pos["side"] == "BUY":
                    current_pips = (mid - pos["entry_price"]) / pip
                else:
                    current_pips = (pos["entry_price"] - mid) / pip

                # Alert every N pips of movement from last alert
                pips_since_alert = current_pips - pos["last_alert_pips"]
                if abs(pips_since_alert) >= self._pnl_alert_threshold:
                    quote_rate = 1.0 if pos["pair"].upper().endswith("USD") else self.quote_per_usd
                    pv = pip_value_per_lot(pos["pair"], quote_rate)
                    pnl_usd = current_pips * pv * pos["lot_size"]

                    sign = "+" if current_pips >= 0 else ""
                    arrow = "\U0001f7e2" if current_pips >= 0 else "\U0001f534"
                    msg = (
                        f"{arrow} *P&L UPDATE* — {pos['pair']} `{pos['side']}`\n"
                        f"Entry: `{pos['entry_price']}` | Now: `{mid}`\n"
                        f"{sign}{current_pips:.1f} pips | {sign}${pnl_usd:.2f}"
                    )
                    notify._send(self.bot_token, self.chat_id, msg)
                    pos["last_alert_pips"] = current_pips
                    print(f"[Monitor] P&L update: {pos['pair']} {pos['side']} {sign}{current_pips:.1f} pips ({sign}${pnl_usd:.2f})")

    def _calc_pnl(
        self, side: str, entry_price: float, exit_price: float, pair: str, lot_size: float
    ) -> tuple[float, float]:
        """Calculate pips and USD P&L from actual fill prices."""
        pip = PIP_SIZE.get(pair.upper(), 0.0001)
        # USD-quoted pairs have pip value directly in USD
        quote_rate = 1.0 if pair.upper().endswith("USD") else self.quote_per_usd
        pv = pip_value_per_lot(pair, quote_rate)

        if side == "BUY":
            pips = (exit_price - entry_price) / pip
        else:
            pips = (entry_price - exit_price) / pip

        pnl_usd = pips * pv * lot_size
        return round(pips, 1), round(pnl_usd, 2)
