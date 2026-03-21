"""
Session Breakout Strategy
-------------------------
Generic session range breakout that works for any forex session.
Configurable session window, pairs, lookback hours, and pip buffer.

This is the same pattern as London Breakout (range consolidation →
breakout entry) but parameterized for different sessions:
  - Asian: tighter ranges on AUD/JPY pairs, shorter lookback
  - London: the classic breakout, wider ranges on EUR/GBP pairs
  - NY: overlap breakouts on major pairs

Reuses generate_both_signals() from london_breakout.py.
"""
from strategy.base import BaseStrategy, TradeIntent
from strategy.london_breakout import generate_both_signals, PIP_SIZE
from data.data_fetcher import fetch_historical
from risk.position_sizer import calculate_lot_size


class SessionBreakoutStrategy(BaseStrategy):
    """Configurable session range breakout strategy."""

    name = "asian_breakout"

    def generate(
        self,
        config: dict,
        ib=None,
        account_balance: float = 0.0,
    ) -> list[TradeIntent]:
        strat_cfg = config.get("strategies", {}).get(self.name, {})
        if not strat_cfg.get("enabled", False):
            return []

        pairs = strat_cfg.get("pairs", ["AUDUSD"])
        risk_pct = strat_cfg.get("risk_per_trade", 0.01)
        range_hours = strat_cfg.get("range_lookback_hours", 4)
        pip_buffer = strat_cfg.get("pip_buffer", 3)
        tp_multiplier = strat_cfg.get("tp_multiplier", 2.0)
        exit_strategy = strat_cfg.get("exit_strategy", "trailing")
        trailing_config = strat_cfg.get("trailing_stop", {})

        import pandas as pd
        now_utc = pd.Timestamp.now("UTC")

        intents = []
        for pair in pairs:
            try:
                df = fetch_historical(pair, period="7d", interval="1h", ib=ib)
                signals = generate_both_signals(
                    pair=pair, df=df, as_of=now_utc,
                    asian_range_hours=range_hours,
                    pip_buffer=pip_buffer,
                    tp_multiplier=tp_multiplier,
                )

                if not signals:
                    continue

                for sig in signals:
                    intents.append(TradeIntent(
                        strategy=self.name,
                        instrument_type="forex",
                        symbol=pair,
                        direction=sig.direction,
                        entry_type="STOP_LIMIT",
                        entry_price=sig.entry,
                        stop_loss=sig.stop_loss,
                        take_profit=sig.take_profit,
                        risk_pips=sig.sl_pips,
                        risk_dollars=account_balance * risk_pct,
                        exit_strategy=exit_strategy,
                        trailing_config=trailing_config,
                        metadata=strat_cfg,
                    ))

            except Exception as e:
                print(f"[{self.name}] {pair} error: {e}")

        return intents

    def get_schedule(self, config: dict) -> list[dict]:
        strat_cfg = config.get("strategies", {}).get(self.name, {})
        sched = strat_cfg.get("schedule", {})
        tz = sched.get("timezone", "UTC")
        entries = []
        for time_str in sched.get("run_times", ["23:00"]):
            hour, minute = map(int, time_str.split(":"))
            entries.append({"hour": hour, "minute": minute, "timezone": tz})
        return entries
