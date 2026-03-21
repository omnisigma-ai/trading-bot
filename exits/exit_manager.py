"""
Exit Manager
------------
Orchestrates all exit strategy evaluation for active trades.
Integrates with TradeMonitor — called on every price tick to
evaluate trailing stops, partial exits, and breakeven moves.
"""
from dataclasses import dataclass, field
from datetime import datetime

import pandas as pd

from exits.trailing_stop import TrailingStop, create_trailing_stop
from exits.partial_exit import PartialExitManager
from strategy.london_breakout import PIP_SIZE


@dataclass
class ExitState:
    """Tracks the exit management state for one active trade."""
    trade_db_id: int
    symbol: str
    strategy: str
    side: str                       # "BUY" or "SELL"
    entry_price: float
    original_stop: float
    current_stop: float
    original_tp: float
    quantity: float                 # original position size
    remaining_quantity: float       # after partial exits
    exit_strategy: str              # "fixed", "trailing", "partial_scale_out"

    # Components (initialised by ExitManager based on exit_strategy)
    trailing: TrailingStop | None = None
    partial_manager: PartialExitManager | None = None

    # State
    breakeven_activated: bool = False
    last_stop_update: str = ""      # ISO timestamp of last SL modification

    @property
    def risk_distance(self) -> float:
        if self.side == "BUY":
            return self.entry_price - self.original_stop
        else:
            return self.original_stop - self.entry_price


@dataclass
class ExitAction:
    """Instruction returned by ExitManager for the trade monitor to execute."""
    trade_db_id: int
    action_type: str                # "modify_stop", "partial_close", "close_all"
    new_stop: float = 0.0           # for modify_stop
    close_quantity: float = 0.0     # for partial_close
    close_pct: float = 0.0         # percentage being closed
    reallocate: bool = False        # flag for profit reallocation engine
    reason: str = ""


class ExitManager:
    """
    Manages exit strategies for all active trades.

    Usage:
        manager = ExitManager(config)
        manager.register_trade(db_id, intent, fill_price)
        # On each tick:
        actions = manager.on_tick(symbol, price, history)
        # Execute actions via TradeMonitor
    """

    def __init__(self, config: dict):
        self.config = config
        exit_cfg = config.get("exit_defaults", {})
        self.breakeven_at_rr = exit_cfg.get("breakeven_at_rr", 1.0)
        self.trailing_activation_rr = exit_cfg.get("trailing_activation_rr", 1.5)
        self.min_stop_change_pips = exit_cfg.get("min_stop_change_pips", 1.0)
        self.active_trades: dict[int, ExitState] = {}  # trade_db_id → ExitState

    def register_trade(
        self,
        trade_db_id: int,
        symbol: str,
        strategy: str,
        side: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        quantity: float,
        exit_strategy: str = "fixed",
        trailing_config: dict = None,
        partial_exits: list = None,
    ) -> None:
        """Register a newly filled trade for exit management."""
        pip_size = PIP_SIZE.get(symbol.upper())
        if pip_size is None:
            # Futures fallback
            try:
                from data.futures_data import FUTURES_SPECS
                spec = FUTURES_SPECS.get(symbol.upper())
                pip_size = spec["tick_size"] if spec else 0.01
            except ImportError:
                pip_size = 0.01

        # Create trailing stop if configured
        trailing = None
        if exit_strategy in ("trailing", "partial_scale_out") and trailing_config:
            trailing = create_trailing_stop(trailing_config, pip_size)

        # Create partial exit manager if configured
        partial_mgr = None
        if exit_strategy == "partial_scale_out" and partial_exits:
            partial_mgr = PartialExitManager(partial_exits)

        state = ExitState(
            trade_db_id=trade_db_id,
            symbol=symbol,
            strategy=strategy,
            side=side,
            entry_price=entry_price,
            original_stop=stop_loss,
            current_stop=stop_loss,
            original_tp=take_profit,
            quantity=quantity,
            remaining_quantity=quantity,
            exit_strategy=exit_strategy,
            trailing=trailing,
            partial_manager=partial_mgr,
        )
        self.active_trades[trade_db_id] = state
        print(f"[ExitMgr] Registered {symbol} {side} (exit: {exit_strategy})")

    def recover_from_db(self, logger) -> int:
        """Load active trades from DB and register them for exit management.

        Returns count of recovered trades.
        """
        open_trades = logger.get_open_trades()
        count = 0
        for trade in open_trades:
            fill_price = trade.get("fill_price") or trade.get("entry_price")
            if not fill_price:
                continue
            self.register_trade(
                trade_db_id=trade["id"],
                symbol=trade.get("pair", ""),
                strategy=trade.get("strategy", ""),
                side=trade.get("direction", "BUY"),
                entry_price=fill_price,
                stop_loss=trade.get("stop_loss", 0),
                take_profit=trade.get("take_profit", 0),
                quantity=trade.get("lot_size", 0),
                exit_strategy=trade.get("exit_strategy", "fixed"),
            )
            count += 1
        return count

    def unregister_trade(self, trade_db_id: int) -> None:
        """Remove a trade from active management (closed)."""
        self.active_trades.pop(trade_db_id, None)

    def on_tick(
        self,
        symbol: str,
        price: float,
        history: pd.DataFrame | None = None,
    ) -> list[ExitAction]:
        """
        Evaluate all exit rules for trades matching this symbol.

        Returns list of ExitAction instructions for the trade monitor.
        """
        actions = []

        for db_id, state in list(self.active_trades.items()):
            if state.symbol.upper() != symbol.upper():
                continue

            if state.exit_strategy == "fixed":
                # Fixed TP/SL — only check breakeven
                action = self._check_breakeven(state, price)
                if action:
                    actions.append(action)
                continue

            # Check breakeven first
            be_action = self._check_breakeven(state, price)
            if be_action:
                actions.append(be_action)

            # Check partial exits
            if state.partial_manager and state.partial_manager.has_pending_levels:
                partial_actions = self._check_partial_exits(state, price)
                actions.extend(partial_actions)

            # Check trailing stop
            if state.trailing:
                trail_action = self._check_trailing(state, price, history)
                if trail_action:
                    actions.append(trail_action)

        return actions

    def _check_breakeven(self, state: ExitState, price: float) -> ExitAction | None:
        """Move SL to breakeven when trade reaches 1R of profit."""
        if state.breakeven_activated:
            return None

        risk = state.risk_distance
        if risk <= 0:
            return None

        if state.side == "BUY":
            profit = price - state.entry_price
        else:
            profit = state.entry_price - price

        if profit >= risk * self.breakeven_at_rr:
            state.breakeven_activated = True
            # Move stop to entry (breakeven)
            new_stop = state.entry_price

            # Only if this actually moves the stop forward
            if state.side == "BUY" and new_stop <= state.current_stop:
                return None
            if state.side == "SELL" and new_stop >= state.current_stop:
                return None

            state.current_stop = new_stop
            state.last_stop_update = datetime.utcnow().isoformat()

            return ExitAction(
                trade_db_id=state.trade_db_id,
                action_type="modify_stop",
                new_stop=new_stop,
                reason=f"breakeven at {self.breakeven_at_rr}R",
            )

        return None

    def _check_trailing(
        self,
        state: ExitState,
        price: float,
        history: pd.DataFrame | None,
    ) -> ExitAction | None:
        """Evaluate trailing stop and return modify action if needed."""
        risk = state.risk_distance
        if risk <= 0 or state.trailing is None:
            return None

        # Only activate trailing after reaching activation threshold
        if state.side == "BUY":
            profit = price - state.entry_price
        else:
            profit = state.entry_price - price

        if profit < risk * self.trailing_activation_rr:
            return None  # not yet profitable enough to trail

        new_stop = state.trailing.calculate_stop(
            current_price=price,
            side=state.side,
            entry_price=state.entry_price,
            current_stop=state.current_stop,
            history=history,
        )

        # Ensure stop only moves forward
        if state.side == "BUY" and new_stop <= state.current_stop:
            return None
        if state.side == "SELL" and new_stop >= state.current_stop:
            return None

        # Throttle: only modify if change is >= min_stop_change_pips
        pip_size = PIP_SIZE.get(state.symbol.upper())
        if pip_size is None:
            try:
                from data.futures_data import FUTURES_SPECS
                spec = FUTURES_SPECS.get(state.symbol.upper())
                pip_size = spec["tick_size"] if spec else 0.01
            except ImportError:
                pip_size = 0.01
        change_pips = abs(new_stop - state.current_stop) / pip_size
        if change_pips < self.min_stop_change_pips:
            return None

        state.current_stop = new_stop
        state.last_stop_update = datetime.utcnow().isoformat()

        return ExitAction(
            trade_db_id=state.trade_db_id,
            action_type="modify_stop",
            new_stop=new_stop,
            reason=f"trailing stop ({type(state.trailing).__name__})",
        )

    def _check_partial_exits(
        self,
        state: ExitState,
        price: float,
    ) -> list[ExitAction]:
        """Check and return partial exit actions."""
        if state.partial_manager is None:
            return []

        triggered = state.partial_manager.check(
            current_price=price,
            entry_price=state.entry_price,
            stop_loss=state.original_stop,
            side=state.side,
        )

        actions = []
        for level in triggered:
            close_qty = state.quantity * (level.pct / 100.0)
            state.remaining_quantity -= close_qty

            actions.append(ExitAction(
                trade_db_id=state.trade_db_id,
                action_type="partial_close",
                close_quantity=close_qty,
                close_pct=level.pct,
                reallocate=(level.action == "reallocate"),
                reason=f"partial exit {level.pct}% at {level.at_rr}R",
            ))

        return actions
