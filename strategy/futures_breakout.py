"""
Futures Breakout Strategy
-------------------------
Session breakout on ES/NQ micro futures at London open and NY open.

Logic:
  1. Calculate overnight consolidation range (last N hours)
  2. Place BUY stop above range + SELL stop below range
  3. SL: other side of range + buffer, TP: 2x risk
  4. Very liquid — minimal slippage concerns

Uses micro contracts (MES/MNQ) by default for paper testing.
"""
import pandas as pd

from strategy.base import BaseStrategy, TradeIntent
from data.futures_data import (
    fetch_futures_history,
    calculate_futures_position_size,
    FUTURES_SPECS,
)


class FuturesBreakoutStrategy(BaseStrategy):
    """US index futures session breakout strategy."""

    name = "futures_breakout"

    def generate(
        self,
        config: dict,
        ib=None,
        account_balance: float = 0.0,
    ) -> list[TradeIntent]:
        if ib is None or not ib.isConnected():
            print("[FuturesBreakout] No IB connection — skipping")
            return []

        strat_cfg = config.get("strategies", {}).get(self.name, {})
        if not strat_cfg.get("enabled", False):
            return []

        symbols = strat_cfg.get("symbols", ["MES", "MNQ"])
        risk_pct = strat_cfg.get("risk_per_trade", 0.005)
        range_hours = strat_cfg.get("range_hours", 6)
        buffer_points = strat_cfg.get("buffer_points", 2.0)
        tp_multiplier = strat_cfg.get("tp_multiplier", 2.0)
        min_range = strat_cfg.get("min_range_points", 5.0)
        exit_strategy = strat_cfg.get("exit_strategy", "trailing")
        trailing_config = strat_cfg.get("trailing_stop", {})

        now_utc = pd.Timestamp.now("UTC")
        intents = []

        for symbol in symbols:
            try:
                df = fetch_futures_history(
                    ib, symbol, duration="1 W", bar_size="1 hour",
                )
                if df is None or len(df) < range_hours:
                    continue

                # Calculate range from last N hours
                window_start = now_utc - pd.Timedelta(hours=range_hours)
                mask = (df.index >= window_start) & (df.index < now_utc)
                window = df.loc[mask]

                if len(window) < max(2, range_hours // 2):
                    print(f"[FuturesBreakout] {symbol}: Insufficient bars in range window ({len(window)})")
                    continue

                range_high = float(window["High"].max())
                range_low = float(window["Low"].min())
                range_size = range_high - range_low

                if range_size < min_range:
                    print(f"[FuturesBreakout] {symbol}: Range too small ({range_size:.1f} < {min_range})")
                    continue

                spec = FUTURES_SPECS[symbol.upper()]
                risk_usd = account_balance * risk_pct

                # BUY breakout above range
                buy_entry = range_high + buffer_points
                buy_sl = range_low - buffer_points
                buy_sl_points = buy_entry - buy_sl
                buy_tp = buy_entry + buy_sl_points * tp_multiplier
                buy_contracts = calculate_futures_position_size(
                    account_balance, risk_pct, buy_sl_points, symbol,
                )

                # SELL breakout below range
                sell_entry = range_low - buffer_points
                sell_sl = range_high + buffer_points
                sell_sl_points = sell_sl - sell_entry
                sell_tp = sell_entry - sell_sl_points * tp_multiplier
                sell_contracts = calculate_futures_position_size(
                    account_balance, risk_pct, sell_sl_points, symbol,
                )

                for direction, entry, sl, tp, contracts in [
                    ("BUY", buy_entry, buy_sl, buy_tp, buy_contracts),
                    ("SELL", sell_entry, sell_sl, sell_tp, sell_contracts),
                ]:
                    if contracts < 1:
                        continue

                    intents.append(TradeIntent(
                        strategy=self.name,
                        instrument_type="futures",
                        symbol=symbol.upper(),
                        direction=direction,
                        entry_type="STOP",
                        entry_price=round(entry, 2),
                        stop_loss=round(sl, 2),
                        take_profit=round(tp, 2),
                        risk_pips=0,
                        risk_dollars=risk_usd,
                        quantity=contracts,
                        exit_strategy=exit_strategy,
                        trailing_config=trailing_config,
                        metadata={
                            "range_high": range_high,
                            "range_low": range_low,
                            "range_size": range_size,
                            "multiplier": spec["multiplier"],
                        },
                    ))

                print(
                    f"[FuturesBreakout] {symbol}: Range {range_low:.2f}-{range_high:.2f} "
                    f"({range_size:.1f} pts) | "
                    f"BUY>{buy_entry:.2f} SELL<{sell_entry:.2f}"
                )

            except Exception as e:
                print(f"[FuturesBreakout] {symbol}: {e}")

        return intents

    def get_schedule(self, config: dict) -> list[dict]:
        strat_cfg = config.get("strategies", {}).get(self.name, {})
        sched = strat_cfg.get("schedule", {})
        tz = sched.get("timezone", "UTC")
        entries = []
        for t in sched.get("run_times", ["07:30", "13:30"]):
            hour, minute = map(int, t.split(":"))
            entries.append({"hour": hour, "minute": minute, "timezone": tz})
        return entries
