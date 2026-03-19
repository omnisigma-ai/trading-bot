"""
IB Trader
---------
Connects to Interactive Brokers TWS via ib_insync.
Places OCA breakout orders (BUY STOP + SELL STOP as One-Cancels-All group)
with attached bracket SL/TP for GBP/JPY and AUD/JPY.

Paper trading port: 7497
Live trading port:  7496

Requires IB TWS or IB Gateway to be running before bot starts.
"""
from dataclasses import dataclass, field
from datetime import datetime, timedelta

from ib_insync import IB, Forex, Order, util

from strategy.london_breakout import PIP_SIZE, Signal


@dataclass
class BreakoutOrderGroup:
    """Holds all order IDs for one pair's OCA breakout group."""
    pair: str
    oca_group: str
    lot_size: float
    buy_signal: Signal
    sell_signal: Signal
    # Entry order IDs (the STP LMT orders)
    buy_entry_id: int = 0
    sell_entry_id: int = 0
    # Child order IDs (TP + SL attached to each entry)
    buy_tp_id: int = 0
    buy_sl_id: int = 0
    sell_tp_id: int = 0
    sell_sl_id: int = 0
    # DB trade record IDs (populated after logging)
    buy_db_id: int = 0
    sell_db_id: int = 0


class IBTrader:
    def __init__(self, host: str = "127.0.0.1", port: int = 7497, client_id: int = 1):
        self.host = host
        self.port = port
        self.client_id = client_id
        self.ib = IB()

    def connect(self) -> None:
        self.ib.connect(self.host, self.port, clientId=self.client_id)
        print(f"[IB] Connected to TWS at {self.host}:{self.port}")

    def disconnect(self) -> None:
        self.ib.disconnect()
        print("[IB] Disconnected from TWS")

    def get_account_balance(self) -> float:
        """Returns the net liquidation value in the account's base currency.

        Tries USD first (standard), then falls back to BASE or any currency
        reported by IB (handles AUD-denominated paper accounts).
        """
        fallback = None
        for av in self.ib.accountValues():
            if av.tag == "NetLiquidation":
                if av.currency == "USD":
                    return float(av.value)
                if av.currency == "BASE":
                    fallback = float(av.value)
                elif fallback is None and av.currency not in ("", "BASE"):
                    fallback = float(av.value)
        if fallback is not None:
            return fallback
        raise RuntimeError("Could not retrieve account balance from IB.")

    def get_current_price(self, pair: str) -> float:
        """Fetch current mid price for a forex pair."""
        contract = self._get_contract(pair)
        self.ib.qualifyContracts(contract)
        tickers = self.ib.reqTickers(contract)
        if not tickers:
            raise RuntimeError(f"No ticker data returned for {pair}")
        price = tickers[0].midpoint()
        if price != price:  # NaN check
            raise RuntimeError(f"Could not get valid price for {pair}")
        return price

    def place_oca_breakout(
        self,
        buy_signal: Signal,
        sell_signal: Signal,
        lot_size: float,
        expire_hours: int = 6,
    ) -> BreakoutOrderGroup:
        """
        Places a London Breakout OCA group:
          - BUY STOP LMT with attached TP limit + SL stop
          - SELL STOP LMT with attached TP limit + SL stop

        IB automatically cancels the unfilled side when one triggers.
        Returns a BreakoutOrderGroup with all 6 order IDs for the trade monitor.
        """
        contract = self._get_contract(buy_signal.pair)
        self.ib.qualifyContracts(contract)

        units = round(lot_size * 100_000)
        gtd = (datetime.utcnow() + timedelta(hours=expire_hours)).strftime("%Y%m%d %H:%M:%S") + " UTC"
        oca_group = f"{buy_signal.pair}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

        group = BreakoutOrderGroup(
            pair=buy_signal.pair,
            oca_group=oca_group,
            lot_size=lot_size,
            buy_signal=buy_signal,
            sell_signal=sell_signal,
        )

        # --- BUY STOP LMT (entry) ---
        pip = PIP_SIZE.get(buy_signal.pair.upper(), 0.0001)
        slippage_guard = 5 * pip  # 5 pips of slippage allowance
        buy_entry = self._make_stop_limit_order(
            action="BUY", units=units,
            stop_price=buy_signal.entry,
            limit_price=buy_signal.entry + slippage_guard,
            gtd=gtd, oca_group=oca_group, transmit=False,
        )
        buy_entry_trade = self.ib.placeOrder(contract, buy_entry)
        self.ib.sleep(0.5)
        group.buy_entry_id = buy_entry_trade.order.orderId

        # BUY TP (limit child)
        buy_tp_order = self._make_child_limit(
            "SELL", units, buy_signal.take_profit, gtd, group.buy_entry_id, transmit=False
        )
        buy_tp_trade = self.ib.placeOrder(contract, buy_tp_order)
        self.ib.sleep(0.2)
        group.buy_tp_id = buy_tp_trade.order.orderId

        # BUY SL (stop child)
        buy_sl_order = self._make_child_stop(
            "SELL", units, buy_signal.stop_loss, gtd, group.buy_entry_id, transmit=False
        )
        buy_sl_trade = self.ib.placeOrder(contract, buy_sl_order)
        self.ib.sleep(0.2)
        group.buy_sl_id = buy_sl_trade.order.orderId

        # --- SELL STOP LMT (entry) ---
        sell_entry = self._make_stop_limit_order(
            action="SELL", units=units,
            stop_price=sell_signal.entry,
            limit_price=sell_signal.entry - slippage_guard,
            gtd=gtd, oca_group=oca_group, transmit=False,
        )
        sell_entry_trade = self.ib.placeOrder(contract, sell_entry)
        self.ib.sleep(0.5)
        group.sell_entry_id = sell_entry_trade.order.orderId

        # SELL TP (limit child)
        sell_tp_order = self._make_child_limit(
            "BUY", units, sell_signal.take_profit, gtd, group.sell_entry_id, transmit=False
        )
        sell_tp_trade = self.ib.placeOrder(contract, sell_tp_order)
        self.ib.sleep(0.2)
        group.sell_tp_id = sell_tp_trade.order.orderId

        # SELL SL (stop child) — transmit=True sends the entire group
        sell_sl_order = self._make_child_stop(
            "BUY", units, sell_signal.stop_loss, gtd, group.sell_entry_id, transmit=True
        )
        sell_sl_trade = self.ib.placeOrder(contract, sell_sl_order)
        self.ib.sleep(0.2)
        group.sell_sl_id = sell_sl_trade.order.orderId

        self.ib.sleep(1)
        print(
            f"[IB] OCA breakout placed: {buy_signal.pair} | "
            f"BUY STOP @ {buy_signal.entry} | SELL STOP @ {sell_signal.entry} | "
            f"OCA group: {oca_group}"
        )
        return group

    def cancel_order(self, order_id: int) -> None:
        for trade in self.ib.openTrades():
            if trade.order.orderId == order_id:
                self.ib.cancelOrder(trade.order)
                print(f"[IB] Cancelled order {order_id}")
                return
        print(f"[IB] Order {order_id} not found (may already be filled/cancelled)")

    def get_open_positions(self) -> list[dict]:
        return [
            {"symbol": pos.contract.symbol, "position": pos.position, "avg_cost": pos.avgCost}
            for pos in self.ib.positions()
            if pos.position != 0
        ]

    # ── private helpers ───────────────────────────────────────────────────────

    def _make_stop_limit_order(
        self, action: str, units: int, stop_price: float, limit_price: float,
        gtd: str, oca_group: str, transmit: bool,
    ) -> Order:
        o = Order()
        o.action = action
        o.orderType = "STP LMT"
        o.auxPrice = stop_price      # stop trigger
        o.lmtPrice = limit_price     # limit after trigger (slippage guard)
        o.totalQuantity = units
        o.tif = "GTD"
        o.goodTillDate = gtd
        o.ocaGroup = oca_group
        o.ocaType = 1                # cancel other orders in OCA group on fill
        o.transmit = transmit
        return o

    def _make_child_limit(
        self, action: str, units: int, price: float, gtd: str, parent_id: int, transmit: bool,
    ) -> Order:
        o = Order()
        o.action = action
        o.orderType = "LMT"
        o.lmtPrice = price
        o.totalQuantity = units
        o.tif = "GTD"
        o.goodTillDate = gtd
        o.parentId = parent_id
        o.transmit = transmit
        return o

    def _make_child_stop(
        self, action: str, units: int, price: float, gtd: str, parent_id: int, transmit: bool,
    ) -> Order:
        o = Order()
        o.action = action
        o.orderType = "STP"
        o.auxPrice = price
        o.totalQuantity = units
        o.tif = "GTD"
        o.goodTillDate = gtd
        o.parentId = parent_id
        o.transmit = transmit
        return o

    def _get_contract(self, pair: str) -> Forex:
        symbol = pair[:3].upper()
        currency = pair[3:].upper()
        return Forex(pair=f"{symbol}{currency}")
