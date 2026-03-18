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

from ib_insync import IB, Fill, Trade

from execution.ib_trader import BreakoutOrderGroup
from logs.trade_logger import TradeLogger
from notifications import discord_notifier as discord
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
        webhook_url: str,
        order_groups: list[BreakoutOrderGroup],
        quote_per_usd: float = 150.0,
        usd_aud_rate: float = 1.54,
    ):
        self.ib = ib
        self.logger = logger
        self.webhook_url = webhook_url
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

    def start(self) -> None:
        """Register IB event callbacks."""
        self.ib.execDetailsEvent += self._on_exec_details
        self.ib.orderStatusEvent += self._on_order_status
        print("[Monitor] Trade monitor started — watching for fills...")

    def stop(self) -> None:
        """Unregister callbacks and send daily summary."""
        self.ib.execDetailsEvent -= self._on_exec_details
        self.ib.orderStatusEvent -= self._on_order_status

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
        discord.notify_daily_summary(self.webhook_url, self._daily_results)
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

            discord.notify_order_filled(self.webhook_url, group.pair, side, fill_price)
            print(f"[Monitor] Entry filled: {group.pair} {side} @ {fill_price} (slippage: {slippage_pips:+.1f} pips)")

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

            discord.notify_tp_hit(self.webhook_url, group.pair, side, pips, pnl)
            self._resolved_entries.add(entry_id)
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

            discord.notify_sl_hit(self.webhook_url, group.pair, side, abs(pips), pnl)
            self._resolved_entries.add(entry_id)
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
            discord.notify_no_signal(self.webhook_url, group.pair, "no breakout today")
            self._resolved_entries.add(order_id)
            self._daily_results.append({"pair": group.pair, "result": "NO_SIGNAL", "pips": 0, "pnl_usd": 0})
            print(f"[Monitor] No trigger: {group.pair} {side} order expired")

    # ── helpers ───────────────────────────────────────────────────────────────

    def _calc_pnl(
        self, side: str, entry_price: float, exit_price: float, pair: str, lot_size: float
    ) -> tuple[float, float]:
        """Calculate pips and USD P&L from actual fill prices."""
        pip = PIP_SIZE.get(pair.upper(), 0.01)
        pv = pip_value_per_lot(pair, self.quote_per_usd)

        if side == "BUY":
            pips = (exit_price - entry_price) / pip
        else:
            pips = (entry_price - exit_price) / pip

        pnl_usd = pips * pv * lot_size
        return round(pips, 1), round(pnl_usd, 2)
