"""
Session Trend Strategy
----------------------
NY session trend continuation — trades the continuation of London's
directional move after a retracement.

Logic:
  1. At NY session, check London session's directional move (07:00-12:00 UTC)
  2. If London moved >20 pips in one direction, wait for pullback
  3. Entry: LIMIT at 50% retracement of London's move
  4. SL: beyond London session extreme
  5. TP: 2x SL distance (continuation target)

Uses LIMIT entry (wait for pullback) instead of STOP entry.
Only runs during NY session (12:00-21:00 UTC).
"""
from datetime import datetime, timedelta

import pandas as pd

from strategy.base import BaseStrategy, TradeIntent
from strategy.london_breakout import PIP_SIZE
from data.data_fetcher import fetch_historical


class SessionTrendStrategy(BaseStrategy):
    """NY session trend continuation strategy."""

    name = "session_trend"

    def generate(
        self,
        config: dict,
        ib=None,
        account_balance: float = 0.0,
    ) -> list[TradeIntent]:
        strat_cfg = config.get("strategies", {}).get(self.name, {})
        if not strat_cfg.get("enabled", False):
            return []

        pairs = strat_cfg.get("pairs", ["EURUSD", "GBPUSD"])
        risk_pct = strat_cfg.get("risk_per_trade", 0.008)
        min_move_pips = strat_cfg.get("min_london_move_pips", 20)
        retracement_pct = strat_cfg.get("retracement_pct", 0.50)
        tp_multiplier = strat_cfg.get("tp_multiplier", 2.0)
        exit_strategy = strat_cfg.get("exit_strategy", "trailing")
        trailing_config = strat_cfg.get("trailing_stop", {})

        now_utc = pd.Timestamp.now("UTC")
        intents = []

        for pair in pairs:
            try:
                intent = self._analyze_pair(
                    pair=pair, now_utc=now_utc, ib=ib,
                    account_balance=account_balance, risk_pct=risk_pct,
                    min_move_pips=min_move_pips,
                    retracement_pct=retracement_pct,
                    tp_multiplier=tp_multiplier,
                    exit_strategy=exit_strategy,
                    trailing_config=trailing_config,
                    strat_cfg=strat_cfg,
                )
                if intent:
                    intents.append(intent)
            except Exception as e:
                print(f"[session_trend] {pair} error: {e}")

        return intents

    def _analyze_pair(
        self, pair: str, now_utc, ib,
        account_balance: float, risk_pct: float,
        min_move_pips: float, retracement_pct: float,
        tp_multiplier: float, exit_strategy: str,
        trailing_config: dict, strat_cfg: dict,
    ) -> TradeIntent | None:
        """Analyze one pair for London trend continuation."""
        df = fetch_historical(pair, period="3d", interval="1h", ib=ib)
        if df is None or len(df) < 10:
            return None

        pip_size = PIP_SIZE.get(pair.upper(), 0.0001)

        # Find London session range (07:00-12:00 UTC today)
        today = now_utc.normalize()
        london_start = today + pd.Timedelta(hours=7)
        london_end = today + pd.Timedelta(hours=12)

        london_bars = df[(df.index >= london_start) & (df.index < london_end)]
        if len(london_bars) < 3:
            return None

        london_high = london_bars["High"].max()
        london_low = london_bars["Low"].min()
        london_open = london_bars.iloc[0]["Open"]
        london_close = london_bars.iloc[-1]["Close"]

        # Determine London's direction
        london_move = london_close - london_open
        london_move_pips = abs(london_move) / pip_size

        if london_move_pips < min_move_pips:
            return None  # London didn't move enough

        if london_move > 0:
            # London was bullish — trade BUY continuation
            direction = "BUY"
            # Entry at retracement
            entry = london_close - abs(london_move) * retracement_pct
            sl = london_low - 5 * pip_size  # below London low
            sl_distance = entry - sl
            tp = entry + sl_distance * tp_multiplier
        else:
            # London was bearish — trade SELL continuation
            direction = "SELL"
            entry = london_close + abs(london_move) * retracement_pct
            sl = london_high + 5 * pip_size  # above London high
            sl_distance = sl - entry
            tp = entry - sl_distance * tp_multiplier

        sl_pips = sl_distance / pip_size
        tp_pips = sl_pips * tp_multiplier

        # Sanity checks
        if sl_pips < 5 or sl_pips > 100:
            return None
        if tp_pips < 10:
            return None

        risk_usd = account_balance * risk_pct

        return TradeIntent(
            strategy=self.name,
            instrument_type="forex",
            symbol=pair,
            direction=direction,
            entry_type="LIMIT",
            entry_price=round(entry, 5),
            stop_loss=round(sl, 5),
            take_profit=round(tp, 5),
            risk_pips=round(sl_pips, 1),
            risk_dollars=risk_usd,
            exit_strategy=exit_strategy,
            trailing_config=trailing_config,
            metadata=strat_cfg,
        )

    def get_schedule(self, config: dict) -> list[dict]:
        strat_cfg = config.get("strategies", {}).get(self.name, {})
        sched = strat_cfg.get("schedule", {})
        tz = sched.get("timezone", "UTC")
        entries = []
        for time_str in sched.get("run_times", ["13:00"]):
            hour, minute = map(int, time_str.split(":"))
            entries.append({"hour": hour, "minute": minute, "timezone": tz})
        return entries
