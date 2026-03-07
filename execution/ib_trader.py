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
from datetime import datetime, timedelta

from ib_insync import IB, Forex, Order, util

from strategy.london_breakout import Signal


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
        """Returns the net liquidation value in USD."""
        for av in self.ib.accountValues():
            if av.tag == "NetLiquidation" and av.currency == "USD":
                return float(av.value)
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
    ) -> tuple[int, int]:
        """
        Places a London Breakout OCA group:
          - BUY STOP LMT with attached TP limit + SL stop
          - SELL STOP LMT with attached TP limit + SL stop

        IB automatically cancels the unfilled side when one triggers.
        Returns (buy_order_id, sell_order_id).
        """
        contract = self._get_contract(buy_signal.pair)
        self.ib.qualifyContracts(contract)

        units = round(lot_size * 100_000)
        gtd = (datetime.utcnow() + timedelta(hours=expire_hours)).strftime("%Y%m%d %H:%M:%S") + " UTC"
        oca_group = f"{buy_signal.pair}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

        # --- BUY STOP LMT (entry) ---
        buy_entry = self._make_stop_limit_order(
            action="BUY",
            units=units,
            stop_price=buy_signal.entry,
            limit_price=buy_signal.entry + (5 * 0.01),  # 5 pip slippage allowance
            gtd=gtd,
            oca_group=oca_group,
            transmit=False,
        )
        buy_trade = self.ib.placeOrder(contract, buy_entry)
        self.ib.sleep(0.5)
        buy_id = buy_trade.order.orderId

        # BUY TP (limit child)
        buy_tp = self._make_child_limit("SELL", units, buy_signal.take_profit, gtd, buy_id, transmit=False)
        self.ib.placeOrder(contract, buy_tp)

        # BUY SL (stop child)
        buy_sl = self._make_child_stop("SELL", units, buy_signal.stop_loss, gtd, buy_id, transmit=False)
        self.ib.placeOrder(contract, buy_sl)

        # --- SELL STOP LMT (entry) ---
        sell_entry = self._make_stop_limit_order(
            action="SELL",
            units=units,
            stop_price=sell_signal.entry,
            limit_price=sell_signal.entry - (5 * 0.01),  # 5 pip slippage allowance
            gtd=gtd,
            oca_group=oca_group,
            transmit=False,
        )
        sell_trade = self.ib.placeOrder(contract, sell_entry)
        self.ib.sleep(0.5)
        sell_id = sell_trade.order.orderId

        # SELL TP (limit child)
        sell_tp = self._make_child_limit("BUY", units, sell_signal.take_profit, gtd, sell_id, transmit=False)
        self.ib.placeOrder(contract, sell_tp)

        # SELL SL (stop child) — transmit=True sends the entire group
        sell_sl = self._make_child_stop("BUY", units, sell_signal.stop_loss, gtd, sell_id, transmit=True)
        self.ib.placeOrder(contract, sell_sl)

        self.ib.sleep(1)
        print(
            f"[IB] OCA breakout placed: {buy_signal.pair} | "
            f"BUY STOP @ {buy_signal.entry} | SELL STOP @ {sell_signal.entry} | "
            f"OCA group: {oca_group}"
        )
        return buy_id, sell_id

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
