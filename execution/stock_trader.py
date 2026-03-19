"""
Stock & ETF Trader
------------------
Handles order placement for stocks and ETFs via Interactive Brokers.
Supports bracket orders (entry + SL + TP) for US stock trades
and simple market buys for ASX ETF reallocation purchases (AUD).
"""
from dataclasses import dataclass
from datetime import datetime, timedelta

from ib_insync import IB, Stock, Order

from strategy.base import TradeIntent


@dataclass
class StockOrderGroup:
    """Holds all order IDs for a stock trade."""
    symbol: str
    strategy: str
    shares: int
    direction: str
    entry_order_id: int = 0
    tp_order_id: int = 0
    sl_order_id: int = 0
    db_trade_id: int = 0


class StockTrader:
    """Places stock and ETF orders via IB."""

    def __init__(self, ib: IB):
        self.ib = ib

    def place_stock_bracket(
        self,
        intent: TradeIntent,
        expire_hours: int = 24,
    ) -> StockOrderGroup:
        """
        Place a bracket order for a stock trade.

        For BUY: market entry + SL stop + TP limit
        For SELL: market entry + SL stop + TP limit

        Returns StockOrderGroup with order IDs.
        """
        contract = Stock(symbol=intent.symbol, exchange="SMART", currency="USD")
        self.ib.qualifyContracts(contract)

        shares = int(intent.quantity)
        gtd = (datetime.utcnow() + timedelta(hours=expire_hours)).strftime("%Y%m%d %H:%M:%S") + " UTC"

        group = StockOrderGroup(
            symbol=intent.symbol,
            strategy=intent.strategy,
            shares=shares,
            direction=intent.direction,
        )

        # Entry order (market or limit)
        if intent.entry_type == "MARKET":
            entry_order = Order()
            entry_order.action = intent.direction
            entry_order.orderType = "MKT"
            entry_order.totalQuantity = shares
            entry_order.tif = "DAY"
            entry_order.transmit = False
        elif intent.entry_type == "LIMIT":
            entry_order = Order()
            entry_order.action = intent.direction
            entry_order.orderType = "LMT"
            entry_order.lmtPrice = intent.entry_price
            entry_order.totalQuantity = shares
            entry_order.tif = "GTD"
            entry_order.goodTillDate = gtd
            entry_order.transmit = False
        else:
            raise ValueError(f"Unsupported entry type for stocks: {intent.entry_type}")

        entry_trade = self.ib.placeOrder(contract, entry_order)
        self.ib.sleep(0.5)
        group.entry_order_id = entry_trade.order.orderId

        # TP order (limit, child of entry)
        exit_action = "SELL" if intent.direction == "BUY" else "BUY"

        if intent.take_profit > 0:
            tp_order = Order()
            tp_order.action = exit_action
            tp_order.orderType = "LMT"
            tp_order.lmtPrice = round(intent.take_profit, 2)
            tp_order.totalQuantity = shares
            tp_order.tif = "GTC"
            tp_order.parentId = group.entry_order_id
            tp_order.transmit = False

            tp_trade = self.ib.placeOrder(contract, tp_order)
            self.ib.sleep(0.2)
            group.tp_order_id = tp_trade.order.orderId

        # SL order (stop, child of entry, transmit=True to send group)
        sl_order = Order()
        sl_order.action = exit_action
        sl_order.orderType = "STP"
        sl_order.auxPrice = round(intent.stop_loss, 2)
        sl_order.totalQuantity = shares
        sl_order.tif = "GTC"
        sl_order.parentId = group.entry_order_id
        sl_order.transmit = True  # transmits the whole group

        sl_trade = self.ib.placeOrder(contract, sl_order)
        self.ib.sleep(0.5)
        group.sl_order_id = sl_trade.order.orderId

        print(
            f"[StockTrader] Bracket placed: {intent.direction} {shares} {intent.symbol} | "
            f"SL ${intent.stop_loss:.2f} | TP ${intent.take_profit:.2f}"
        )
        return group

    def place_etf_buy(
        self,
        symbol: str,
        shares: int,
        exchange: str = "SMART",
        currency: str = "USD",
    ) -> int:
        """
        Place a simple market buy for ETF reallocation.
        For AU-domiciled ETFs: exchange="ASX", currency="AUD".
        Returns the IB order ID.
        """
        contract = Stock(symbol=symbol, exchange=exchange, currency=currency)
        self.ib.qualifyContracts(contract)

        order = Order()
        order.action = "BUY"
        order.orderType = "MKT"
        order.totalQuantity = shares
        order.tif = "DAY"

        trade = self.ib.placeOrder(contract, order)
        self.ib.sleep(0.5)
        order_id = trade.order.orderId

        print(f"[StockTrader] ETF buy: {shares} {symbol} on {exchange} ({currency}) (order {order_id})")
        return order_id

    def modify_stop(
        self,
        order_id: int,
        new_stop_price: float,
        symbol: str = None,
    ) -> bool:
        """
        Modify an existing stop order price (for trailing stops).
        Returns True if modification was submitted.
        """
        for trade in self.ib.openTrades():
            if trade.order.orderId == order_id:
                trade.order.auxPrice = round(new_stop_price, 2)
                self.ib.placeOrder(trade.contract, trade.order)
                return True

        print(f"[StockTrader] Order {order_id} not found for modification")
        return False

    def get_stock_price(self, symbol: str, exchange: str = "SMART", currency: str = "USD") -> float:
        """Get current price for a stock/ETF. Use exchange='ASX', currency='AUD' for ASX ETFs."""
        contract = Stock(symbol=symbol, exchange=exchange, currency=currency)
        self.ib.qualifyContracts(contract)
        tickers = self.ib.reqTickers(contract)
        if not tickers:
            raise RuntimeError(f"No ticker data for {symbol}")
        mid = tickers[0].midpoint()
        if mid != mid:  # NaN
            mid = tickers[0].last
        return float(mid)

    def close_partial(
        self,
        symbol: str,
        shares: int,
        direction: str,
    ) -> int:
        """
        Close a partial position with a market order.
        direction should be opposite of the original trade.
        Returns order ID.
        """
        contract = Stock(symbol=symbol, exchange="SMART", currency="USD")
        self.ib.qualifyContracts(contract)

        order = Order()
        order.action = "SELL" if direction == "BUY" else "BUY"
        order.orderType = "MKT"
        order.totalQuantity = shares
        order.tif = "DAY"

        trade = self.ib.placeOrder(contract, order)
        self.ib.sleep(0.5)
        print(f"[StockTrader] Partial close: {order.action} {shares} {symbol}")
        return trade.order.orderId
