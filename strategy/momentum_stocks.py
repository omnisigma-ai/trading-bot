"""
Momentum Stocks Strategy
------------------------
Scans for US stocks breaking out of consolidation with volume
confirmation. These setups offer asymmetric R:R because stocks
that break out can run 5-20% while the stop is 2-3% below entry.

Logic:
1. Screen universe for stocks at new 20-day highs with above-average volume
2. Entry: market buy at current price
3. SL: below recent swing low or 2x ATR below entry (whichever is tighter)
4. TP: none fixed — uses trailing stop (let winners run)
5. Exit: partial scale-out (50% at 1.5R) + chandelier trail on remainder
6. Risk: 0.5% per stock trade (configurable)
"""
from strategy.base import BaseStrategy, TradeIntent
from strategy.stock_screener import screen_universe


class MomentumStocksStrategy(BaseStrategy):
    """US stock momentum breakout strategy."""

    name = "momentum_stocks"

    def generate(
        self,
        config: dict,
        ib=None,
        account_balance: float = 0.0,
    ) -> list[TradeIntent]:
        if ib is None or not ib.isConnected():
            print("[MomentumStocks] No IB connection — skipping")
            return []

        strat_cfg = config.get("strategies", {}).get("momentum_stocks", {})
        if not strat_cfg.get("enabled", False):
            return []

        symbols = strat_cfg.get("symbols", [])
        if not symbols:
            print("[MomentumStocks] No symbols configured — skipping")
            return []

        risk_pct = strat_cfg.get("risk_per_trade", 0.005)
        max_positions = strat_cfg.get("max_positions", 3)
        min_volume_ratio = strat_cfg.get("min_volume_ratio", 1.5)
        breakout_lookback = strat_cfg.get("breakout_lookback_days", 20)
        exit_strategy = strat_cfg.get("exit_strategy", "partial_scale_out")
        trailing_config = strat_cfg.get("trailing_stop", {
            "type": "chandelier",
            "atr_period": 14,
            "atr_multiplier": 2.0,
        })
        partial_exits = strat_cfg.get("partial_exits", [
            {"pct": 50, "at_rr": 1.5, "action": "reallocate"},
            {"pct": 50, "at_rr": 999, "action": "close"},  # trails until stopped
        ])

        # Screen for candidates
        print(f"[MomentumStocks] Scanning {len(symbols)} stocks...")
        candidates = screen_universe(
            ib=ib,
            symbols=symbols,
            min_volume_ratio=min_volume_ratio,
            breakout_lookback=breakout_lookback,
        )

        if not candidates:
            print("[MomentumStocks] No breakout candidates found")
            return []

        print(f"[MomentumStocks] Found {len(candidates)} candidates")
        intents = []

        for candidate in candidates[:max_positions]:
            risk_usd = account_balance * risk_pct
            entry_price = candidate.current_price

            # SL: tighter of swing low or 2x ATR below entry
            atr_stop = entry_price - (2 * candidate.atr)
            sl = max(candidate.swing_low, atr_stop)  # use the tighter (higher) stop

            # Ensure SL is below entry
            if sl >= entry_price:
                continue

            risk_per_share = entry_price - sl
            shares = int(risk_usd / risk_per_share) if risk_per_share > 0 else 0
            if shares < 1:
                print(f"[MomentumStocks] {candidate.symbol}: Risk too small for 1 share")
                continue

            # No fixed TP — let trailing stop manage exit
            # Set initial TP at 3x risk for the opportunity scorer
            tp = entry_price + (risk_per_share * 3)

            intent = TradeIntent(
                strategy=self.name,
                instrument_type="stock",
                symbol=candidate.symbol,
                direction="BUY",
                entry_type="MARKET",
                entry_price=entry_price,
                stop_loss=round(sl, 2),
                take_profit=round(tp, 2),
                risk_pips=0,
                risk_dollars=risk_usd,
                quantity=shares,
                exit_strategy=exit_strategy,
                trailing_config=trailing_config,
                partial_exits=partial_exits,
                metadata={
                    **candidate.metadata,
                    "atr": candidate.atr,
                    "swing_low": candidate.swing_low,
                    "risk_per_share": risk_per_share,
                    "tp_pips": risk_per_share * 3 / risk_per_share,  # R:R for scorer
                },
            )
            intents.append(intent)
            print(
                f"[MomentumStocks] {candidate.symbol}: "
                f"${entry_price:.2f} | SL ${sl:.2f} | "
                f"Vol {candidate.volume_ratio:.1f}x | "
                f"Risk ${risk_per_share:.2f}/share × {shares} shares"
            )

        return intents

    def get_schedule(self, config: dict) -> list[dict]:
        strat_cfg = config.get("strategies", {}).get("momentum_stocks", {})
        schedule = strat_cfg.get("schedule", {})
        tz = schedule.get("timezone", "Australia/Sydney")
        run_times = schedule.get("run_times", ["23:30"])
        entries = []
        for t in run_times:
            hour, minute = map(int, t.split(":"))
            entries.append({"hour": hour, "minute": minute, "timezone": tz})
        return entries
