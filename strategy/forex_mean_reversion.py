"""
Forex Mean Reversion Strategy
------------------------------
Fades overextended moves when price reaches Bollinger Band extremes
with RSI confirmation.

Logic:
  1. Calculate 20-period Bollinger Bands (2 std dev) on 1h candles
  2. Calculate 14-period RSI
  3. BUY when: price <= lower band AND RSI < 30 (oversold)
  4. SELL when: price >= upper band AND RSI > 70 (overbought)
  5. Entry: LIMIT at the band level
  6. SL: 1.5x ATR beyond the extreme
  7. TP: 20 SMA (middle band) — natural mean reversion target

Key difference from breakout strategies:
  - Higher win rate (~60%) but smaller R:R (typically 1:1 to 1.5:1)
  - Works in ranging markets (complements breakouts in trending markets)
  - Counter-trend: different correlation profile from breakouts
"""
import pandas as pd

from strategy.base import BaseStrategy, TradeIntent
from strategy.london_breakout import PIP_SIZE
from data.data_fetcher import fetch_historical
from data.stock_data import calculate_atr, calculate_rsi


class ForexMeanReversionStrategy(BaseStrategy):
    """Forex mean reversion using RSI + Bollinger Bands."""

    name = "forex_mean_reversion"

    def generate(
        self,
        config: dict,
        ib=None,
        account_balance: float = 0.0,
    ) -> list[TradeIntent]:
        strat_cfg = config.get("strategies", {}).get(self.name, {})
        if not strat_cfg.get("enabled", False):
            return []

        pairs = strat_cfg.get("pairs", ["EURUSD", "GBPUSD", "AUDUSD"])
        risk_pct = strat_cfg.get("risk_per_trade", 0.005)
        bb_period = strat_cfg.get("bb_period", 20)
        bb_std = strat_cfg.get("bb_std_dev", 2.0)
        rsi_period = strat_cfg.get("rsi_period", 14)
        rsi_oversold = strat_cfg.get("rsi_oversold", 30)
        rsi_overbought = strat_cfg.get("rsi_overbought", 70)
        atr_sl_multiplier = strat_cfg.get("atr_sl_multiplier", 1.5)
        exit_strategy = strat_cfg.get("exit_strategy", "trailing")
        trailing_config = strat_cfg.get("trailing_stop", {})

        intents = []
        for pair in pairs:
            try:
                intent = self._analyze_pair(
                    pair=pair, ib=ib, account_balance=account_balance,
                    risk_pct=risk_pct, bb_period=bb_period, bb_std=bb_std,
                    rsi_period=rsi_period, rsi_oversold=rsi_oversold,
                    rsi_overbought=rsi_overbought,
                    atr_sl_multiplier=atr_sl_multiplier,
                    exit_strategy=exit_strategy,
                    trailing_config=trailing_config,
                    strat_cfg=strat_cfg,
                )
                if intent:
                    intents.append(intent)
            except Exception as e:
                print(f"[MeanReversion] {pair}: {e}")

        return intents

    def _analyze_pair(
        self, pair: str, ib, account_balance: float, risk_pct: float,
        bb_period: int, bb_std: float, rsi_period: int,
        rsi_oversold: int, rsi_overbought: int,
        atr_sl_multiplier: float, exit_strategy: str,
        trailing_config: dict, strat_cfg: dict,
    ) -> TradeIntent | None:
        """Analyze one pair for mean reversion setup."""
        df = fetch_historical(pair, period="1mo", interval="1h", ib=ib)
        if df is None or len(df) < bb_period + 5:
            return None

        pip_size = PIP_SIZE.get(pair.upper(), 0.0001)

        # Bollinger Bands
        sma = df["Close"].rolling(bb_period).mean()
        std = df["Close"].rolling(bb_period).std()
        upper_band = sma + bb_std * std
        lower_band = sma - bb_std * std

        # RSI
        rsi = calculate_rsi(df, rsi_period)

        # ATR for stop placement
        atr = calculate_atr(df, period=14)

        latest = df.iloc[-1]
        latest_close = float(latest["Close"])
        latest_rsi = float(rsi.iloc[-1]) if pd.notna(rsi.iloc[-1]) else 50
        latest_atr = float(atr.iloc[-1]) if pd.notna(atr.iloc[-1]) else 0
        latest_upper = float(upper_band.iloc[-1])
        latest_lower = float(lower_band.iloc[-1])
        latest_sma = float(sma.iloc[-1])

        if latest_atr <= 0:
            return None

        direction = None
        entry = None
        sl = None
        tp = None

        if latest_close <= latest_lower and latest_rsi < rsi_oversold:
            direction = "BUY"
            entry = latest_lower
            sl = entry - atr_sl_multiplier * latest_atr
            tp = latest_sma  # mean reversion target
        elif latest_close >= latest_upper and latest_rsi > rsi_overbought:
            direction = "SELL"
            entry = latest_upper
            sl = entry + atr_sl_multiplier * latest_atr
            tp = latest_sma

        if direction is None:
            return None

        sl_pips = abs(entry - sl) / pip_size
        tp_pips = abs(tp - entry) / pip_size

        # Sanity checks
        if sl_pips < 5 or sl_pips > 100:
            return None
        if tp_pips < 5:
            return None

        risk_usd = account_balance * risk_pct
        dec = 3 if pip_size >= 0.01 else 5

        print(
            f"[MeanReversion] {pair} {direction}: RSI={latest_rsi:.0f} | "
            f"BB=[{latest_lower:.{dec}f}, {latest_sma:.{dec}f}, {latest_upper:.{dec}f}] | "
            f"Entry={entry:.{dec}f} SL={sl:.{dec}f} TP={tp:.{dec}f}"
        )

        return TradeIntent(
            strategy=self.name,
            instrument_type="forex",
            symbol=pair,
            direction=direction,
            entry_type="LIMIT",
            entry_price=round(entry, dec),
            stop_loss=round(sl, dec),
            take_profit=round(tp, dec),
            risk_pips=round(sl_pips, 1),
            risk_dollars=risk_usd,
            exit_strategy=exit_strategy,
            trailing_config=trailing_config,
            metadata={
                "rsi": round(latest_rsi, 1),
                "bb_upper": round(latest_upper, dec),
                "bb_lower": round(latest_lower, dec),
                "bb_sma": round(latest_sma, dec),
                "atr": round(latest_atr, dec),
                "tp_pips": round(tp_pips, 1),
                "mean_reversion_signal": True,
                **strat_cfg,
            },
        )

    def get_schedule(self, config: dict) -> list[dict]:
        strat_cfg = config.get("strategies", {}).get(self.name, {})
        sched = strat_cfg.get("schedule", {})
        tz = sched.get("timezone", "UTC")
        entries = []
        for t in sched.get("run_times", ["08:00", "14:00", "20:00"]):
            hour, minute = map(int, t.split(":"))
            entries.append({"hour": hour, "minute": minute, "timezone": tz})
        return entries
