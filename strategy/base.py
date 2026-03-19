"""
Strategy Base
-------------
Common interface for all trading strategies. Each strategy produces
TradeIntent objects that the execution layer dispatches to the
appropriate order handler (forex, stock, ETF).
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import pandas as pd
from ib_insync import IB


@dataclass
class TradeIntent:
    """Universal trade instruction produced by any strategy."""
    strategy: str               # "london_breakout", "momentum_stocks"
    instrument_type: str        # "forex", "stock", "etf"
    symbol: str                 # "AUDUSD", "AAPL", "IVV"
    direction: str              # "BUY" or "SELL"
    entry_type: str             # "MARKET", "LIMIT", "STOP", "STOP_LIMIT"
    entry_price: float          # 0 for MARKET orders
    stop_loss: float
    take_profit: float          # initial TP target (0 = no fixed TP)
    risk_pips: float = 0.0      # forex only; 0 for stocks
    risk_dollars: float = 0.0   # absolute dollar risk
    quantity: float = 0.0       # lots (forex) or shares (stocks)
    exit_strategy: str = "fixed"  # "fixed", "trailing", "partial_scale_out"
    trailing_config: dict = field(default_factory=dict)
    partial_exits: list = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    @property
    def is_forex(self) -> bool:
        return self.instrument_type == "forex"

    @property
    def is_stock(self) -> bool:
        return self.instrument_type == "stock"

    @property
    def is_etf(self) -> bool:
        return self.instrument_type == "etf"


class BaseStrategy(ABC):
    """Abstract base for all trading strategies."""

    name: str = "base"

    @abstractmethod
    def generate(
        self,
        config: dict,
        ib: IB | None = None,
        account_balance: float = 0.0,
    ) -> list[TradeIntent]:
        """
        Analyse data and return trade intents for this session.

        Args:
            config: Full bot config dict (strategy reads its own section)
            ib: Optional connected IB instance for data/price fetching
            account_balance: Current account balance in USD

        Returns:
            List of TradeIntent objects (can be empty if no signal)
        """
        ...

    @abstractmethod
    def get_schedule(self, config: dict) -> list[dict]:
        """
        Return schedule entries for this strategy.

        Returns:
            List of dicts with 'hour', 'minute', 'timezone' keys
        """
        ...
