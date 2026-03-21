"""
Futures Trader
--------------
Bracket order placement for US index futures (ES, NQ, MES, MNQ).
Follows the same parent-child order pattern as stock_trader.py.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta

from ib_insync import IB, Order

from data.futures_data import get_front_month_contract, FUTURES_SPECS
from strategy.base import TradeIntent


@dataclass
class FuturesOrderGroup:
    """Holds all order IDs for a futures trade."""
    symbol: str
    strategy: str
    contracts: int
    direction: str
    entry_order_id: int = 0
    tp_order_id: int = 0
    sl_order_id: int = 0
    db_trade_id: int = 0


class FuturesTrader:
    """Places futures bracket orders via IB."""

    def __init__(self, ib: IB):
        self.ib = ib

    def place_futures_bracket(
        self,
        intent: TradeIntent,
        expire_hours: int = 8,
    ) -> FuturesOrderGroup:
        """
        Place a bracket order for a futures trade.

        Entry (parent) + SL (child stop) + TP (child limit).
        Returns FuturesOrderGroup with order IDs.
        """
        contract = get_front_month_contract(self.ib, intent.symbol)
        contracts = max(1, int(intent.quantity))
        gtd = (datetime.utcnow() + timedelta(hours=expire_hours)).strftime(
            "%Y%m%d %H:%M:%S"
        ) + " UTC"

        group = FuturesOrderGroup(
            symbol=intent.symbol,
            strategy=intent.strategy,
            contracts=contracts,
            direction=intent.direction,
        )

        # Entry order
        entry_order = Order()
        entry_order.action = intent.direction
        entry_order.totalQuantity = contracts
        entry_order.transmit = False

        if intent.entry_type == "STOP":
            entry_order.orderType = "STP"
            entry_order.auxPrice = round(intent.entry_price, 2)
            entry_order.tif = "GTD"
            entry_order.goodTillDate = gtd
        elif intent.entry_type == "LIMIT":
            entry_order.orderType = "LMT"
            entry_order.lmtPrice = round(intent.entry_price, 2)
            entry_order.tif = "GTD"
            entry_order.goodTillDate = gtd
        else:  # MARKET
            entry_order.orderType = "MKT"
            entry_order.tif = "DAY"

        entry_trade = self.ib.placeOrder(contract, entry_order)
        self.ib.sleep(0.5)
        group.entry_order_id = entry_trade.order.orderId

        exit_action = "SELL" if intent.direction == "BUY" else "BUY"

        # TP order (limit, child of entry)
        if intent.take_profit > 0:
            tp_order = Order()
            tp_order.action = exit_action
            tp_order.orderType = "LMT"
            tp_order.lmtPrice = round(intent.take_profit, 2)
            tp_order.totalQuantity = contracts
            tp_order.tif = "GTC"
            tp_order.parentId = group.entry_order_id
            tp_order.transmit = False

            tp_trade = self.ib.placeOrder(contract, tp_order)
            self.ib.sleep(0.2)
            group.tp_order_id = tp_trade.order.orderId

        # SL order (stop, child of entry, transmit=True sends the group)
        sl_order = Order()
        sl_order.action = exit_action
        sl_order.orderType = "STP"
        sl_order.auxPrice = round(intent.stop_loss, 2)
        sl_order.totalQuantity = contracts
        sl_order.tif = "GTC"
        sl_order.parentId = group.entry_order_id
        sl_order.transmit = True  # transmits the whole group

        sl_trade = self.ib.placeOrder(contract, sl_order)
        self.ib.sleep(0.5)
        group.sl_order_id = sl_trade.order.orderId

        spec = FUTURES_SPECS.get(intent.symbol.upper(), {})
        print(
            f"[FuturesTrader] Bracket placed: {intent.direction} {contracts} "
            f"{intent.symbol} ({spec.get('description', '')}) | "
            f"SL {intent.stop_loss:.2f} | TP {intent.take_profit:.2f}"
        )
        return group
