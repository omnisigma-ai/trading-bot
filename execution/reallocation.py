"""
Profit Reallocation Engine
--------------------------
Automatically earmarks 50% (configurable) of trading profits for
long-term investments:
  - 60% VGS (Vanguard Intl Shares, ASX)
  - 30% VAS (Vanguard AU Shares, ASX)
  - 10% EV-scored undervalued ASX stock (selected by ev_scorer)

Purchases are in AUD since all instruments are ASX-listed.
"""
import json
from dataclasses import asdict
from datetime import datetime

from ib_insync import IB

from execution.stock_trader import StockTrader
from logs.trade_logger import TradeLogger
from notifications import telegram_notifier as notify
from strategy.feature_tracker import FeatureDecision, log_decision


class ProfitReallocator:
    """Manages the profit-to-investment reallocation pipeline."""

    def __init__(
        self,
        ib: IB,
        logger: TradeLogger,
        config: dict,
        bot_token: str = "",
        chat_id: str = "",
    ):
        self.ib = ib
        self.logger = logger
        self.stock_trader = StockTrader(ib)
        self.bot_token = bot_token
        self.chat_id = chat_id

        realloc_cfg = config.get("reallocation", {})
        self.enabled = realloc_cfg.get("enabled", False)
        self.realloc_pct = realloc_cfg.get("pct", 0.50)
        self.min_purchase_aud = realloc_cfg.get("min_purchase_aud", 50.0)
        self.currency = realloc_cfg.get("currency", "AUD")
        self.etf_allocation = realloc_cfg.get("etf_allocation", {
            "VGS": 0.60,
            "VAS": 0.30,
        })
        self.value_stock_pct = realloc_cfg.get("value_stock_allocation", 0.0)
        self.value_stock_cfg = config.get("value_stock", {})
        self.deploy_strategy = realloc_cfg.get("deploy_strategy", "weekly")
        self.max_wait_days = realloc_cfg.get("max_wait_days", 30)
        self.dip_thresholds = realloc_cfg.get("dip_thresholds", {})
        self.session_id = ""  # set per run

    def on_trade_closed(
        self,
        trade_db_id: int,
        strategy: str,
        pnl_usd: float,
        usd_aud_rate: float = None,
    ) -> None:
        """
        Called when a profitable trade closes.
        Earmarks a percentage of profits for reallocation.
        Amounts stored in both USD and AUD (investments are ASX/AUD).
        """
        if not self.enabled:
            return
        if pnl_usd <= 0:
            return  # only reallocate profits

        earmarked_usd = round(pnl_usd * self.realloc_pct, 2)
        earmarked_aud = round(earmarked_usd * usd_aud_rate, 2) if usd_aud_rate else None

        self.logger.log_reallocation_pending(
            source_trade_id=trade_db_id,
            source_strategy=strategy,
            profit_usd=pnl_usd,
            earmarked_usd=earmarked_usd,
            earmarked_aud=earmarked_aud,
        )

        pending_total = self.logger.get_pending_reallocation_total()
        pending_aud = round(pending_total * usd_aud_rate, 2) if usd_aud_rate else pending_total
        print(f"[Realloc] Earmarked A${earmarked_aud:.2f} from trade #{trade_db_id} | Total pending: A${pending_aud:.2f}")

        notify._send(
            self.bot_token, self.chat_id,
            f"\U0001f4b0 *PROFIT EARMARKED* \u2014 A${earmarked_aud:.2f} for reallocation\n"
            f"Source: trade #{trade_db_id} ({strategy}) | +${pnl_usd:.2f} USD\n"
            f"Pending total: A${pending_aud:.2f} (threshold: A${self.min_purchase_aud:.0f})"
        )

    def execute_pending_purchases(self, usd_aud_rate: float = None) -> list[dict]:
        """
        Batch-purchase ASX ETFs + EV-scored value stock from accumulated profits.

        If deploy_strategy="dip": only deploy when macro dip detected or max_wait_days reached.
        If deploy_strategy="weekly": deploy every time this is called (original behavior).

        Always logs a macro snapshot for ML training data.
        """
        if not self.enabled:
            return []

        # pending_total is stored in USD — convert to AUD
        pending_usd = self.logger.get_pending_reallocation_total()
        if usd_aud_rate is None:
            usd_aud_rate = 1.54  # fallback
        pending_aud = pending_usd * usd_aud_rate

        if pending_aud < self.min_purchase_aud:
            print(f"[Realloc] Pending A${pending_aud:.2f} < threshold A${self.min_purchase_aud:.0f} \u2014 skipping")
            return []

        # ── Macro dip check (always runs — logs data for ML) ─────────────
        dip_signal = self._check_macro_conditions()
        deploying = True

        if self.deploy_strategy == "dip" and dip_signal is not None:
            from strategy.dip_detector import should_deploy

            last_deploy = self.logger.get_last_deploy_date()
            if last_deploy:
                from datetime import datetime as dt
                days_since = (dt.utcnow() - dt.fromisoformat(last_deploy)).days
            else:
                days_since = None

            deploying = should_deploy(dip_signal, days_since, self.max_wait_days)

            # Log macro snapshot (always, regardless of deploy decision)
            self.logger.log_macro_snapshot(
                snapshot_date=datetime.utcnow().strftime("%Y-%m-%d"),
                vix=dip_signal.macro_snapshot.get("vix"),
                us_10y_yield=dip_signal.macro_snapshot.get("us_10y_yield"),
                gold=dip_signal.macro_snapshot.get("gold"),
                oil_wti=dip_signal.macro_snapshot.get("oil_wti"),
                aud_usd=dip_signal.macro_snapshot.get("aud_usd"),
                dxy=dip_signal.macro_snapshot.get("dxy"),
                vix_5d_chg=dip_signal.changes.get("vix_5d_chg"),
                gold_5d_chg=dip_signal.changes.get("gold_5d_chg"),
                oil_5d_chg=dip_signal.changes.get("oil_wti_5d_chg"),
                aud_usd_5d_chg=dip_signal.changes.get("aud_usd_5d_chg"),
                dxy_5d_chg=dip_signal.changes.get("dxy_5d_chg"),
                is_dip=dip_signal.is_dip,
                dip_confidence=dip_signal.confidence,
                dip_triggers=", ".join(dip_signal.triggers),
                deployed=deploying,
            )

            notify.notify_dip_detected(
                self.bot_token, self.chat_id,
                signal=dip_signal,
                deploying=deploying,
                pending_aud=pending_aud,
            )

            # Log dip detector decision
            self._log_dip_decision(
                deploying=deploying,
                dip_signal=dip_signal,
                pending_aud=pending_aud,
                days_since=days_since,
            )

            if not deploying:
                wait_str = f"{days_since}d" if days_since is not None else "never"
                print(f"[Realloc] No dip — holding A${pending_aud:.2f} (last deploy: {wait_str})")
                return []

        pending_records = self.logger.get_pending_reallocations()
        realloc_ids = [r["id"] for r in pending_records]
        purchases = []

        print(f"[Realloc] Executing purchases: A${pending_aud:.2f}")

        # ── ETF purchases ────────────────────────────────────────────────
        for etf_symbol, weight in self.etf_allocation.items():
            amount_aud = pending_aud * weight
            if amount_aud < 10:
                continue

            try:
                price_aud = self.stock_trader.get_stock_price(etf_symbol, exchange="ASX", currency="AUD")
                shares = int(amount_aud / price_aud)
                if shares < 1:
                    print(f"[Realloc] {etf_symbol}: A${amount_aud:.2f} < 1 share (A${price_aud:.2f}) \u2014 accumulating")
                    continue

                actual_cost_aud = shares * price_aud
                actual_cost_usd = round(actual_cost_aud / usd_aud_rate, 2)
                order_id = self.stock_trader.place_etf_buy(etf_symbol, shares, exchange="ASX", currency="AUD")

                purchase_id = self.logger.log_etf_purchase(
                    symbol=etf_symbol,
                    shares=shares,
                    avg_cost_usd=round(price_aud / usd_aud_rate, 2),
                    total_invested_usd=actual_cost_usd,
                    avg_cost_aud=price_aud,
                    total_invested_aud=actual_cost_aud,
                    ib_order_id=order_id,
                    usd_aud_rate=usd_aud_rate,
                    notes=f"Auto-reallocation from {len(realloc_ids)} trades (ASX)",
                )

                purchases.append({
                    "symbol": etf_symbol,
                    "type": "etf",
                    "shares": shares,
                    "price_aud": price_aud,
                    "cost_aud": actual_cost_aud,
                    "cost_usd": actual_cost_usd,
                    "order_id": order_id,
                    "purchase_id": purchase_id,
                })

                notify._send(
                    self.bot_token, self.chat_id,
                    f"\U0001f4c8 *ETF PURCHASED* \u2014 {shares} \u00d7 {etf_symbol} @ A${price_aud:.2f}\n"
                    f"Cost: A${actual_cost_aud:.2f} ({weight * 100:.0f}% allocation)"
                )

            except Exception as e:
                print(f"[Realloc] Failed to buy {etf_symbol}: {e}")
                notify._send(
                    self.bot_token, self.chat_id,
                    f"\u26a0\ufe0f *ETF PURCHASE FAILED* \u2014 {etf_symbol}: {e}"
                )

        # ── Value stock purchase (EV-scored) ─────────────────────────────
        if self.value_stock_pct > 0 and self.value_stock_cfg.get("enabled", False):
            value_amount_aud = pending_aud * self.value_stock_pct
            if value_amount_aud >= 10:
                try:
                    value_purchase = self._execute_value_stock_purchase(
                        value_amount_aud, usd_aud_rate,
                    )
                    if value_purchase:
                        purchases.append(value_purchase)
                except Exception as e:
                    print(f"[Realloc] Value stock selection failed: {e}")
                    notify._send(
                        self.bot_token, self.chat_id,
                        f"\u26a0\ufe0f *VALUE STOCK SELECTION FAILED* \u2014 {e}"
                    )

        # ── Mark reallocations as purchased ──────────────────────────────
        if purchases:
            purchase_ids = [p["purchase_id"] for p in purchases if "purchase_id" in p]
            if purchase_ids:
                self.logger.mark_reallocations_purchased(realloc_ids, purchase_ids[0])

            # Summary notification
            total_aud = sum(p["cost_aud"] for p in purchases)
            lines = ["\U0001f4ca *REALLOCATION COMPLETE*"]
            for p in purchases:
                label = p.get("type", "etf").upper()
                lines.append(
                    f"  [{label}] {p['shares']}\u00d7 {p['symbol']} "
                    f"@ A${p['price_aud']:.2f} = A${p['cost_aud']:.2f}"
                )
            lines.append(f"\nTotal invested: A${total_aud:.2f}")
            notify._send(self.bot_token, self.chat_id, "\n".join(lines))

        return purchases

    def _check_macro_conditions(self):
        """Fetch macro indicators and run dip detection. Returns DipSignal or None."""
        try:
            from data.macro_indicators import fetch_macro_snapshot, fetch_macro_history, compute_changes
            from strategy.dip_detector import detect_dip

            snapshot = fetch_macro_snapshot()
            history = fetch_macro_history(days=10)
            changes = compute_changes(snapshot, history, lookback=5)

            signal = detect_dip(snapshot, changes, self.dip_thresholds)
            print(f"[Macro] {signal.summary()}")
            return signal

        except Exception as e:
            print(f"[Macro] Failed to check conditions: {e}")
            return None

    def _execute_value_stock_purchase(
        self, amount_aud: float, usd_aud_rate: float,
    ) -> dict | None:
        """
        Run EV scoring on the curated ASX universe, select the top stock,
        and place a buy order.
        """
        from data.asx_fundamentals import fetch_universe_fundamentals
        from strategy.ev_scorer import score_universe, select_best_stock

        cfg = self.value_stock_cfg
        universe_dict = cfg.get("universe", {})

        # Flatten sector-grouped universe into flat list + sector map
        symbols = []
        sector_map = {}
        for sector, tickers in universe_dict.items():
            for ticker in tickers:
                symbols.append(ticker)
                sector_map[ticker] = sector

        if not symbols:
            print("[ValueStock] No universe configured")
            return None

        # Fetch fundamentals (uses cache)
        fundamentals = fetch_universe_fundamentals(
            symbols=symbols,
            logger=self.logger,
            max_age_hours=cfg.get("cache_hours", 24),
        )

        if not fundamentals:
            print("[ValueStock] No fundamental data available")
            return None

        # Score universe
        weights = cfg.get("weights")
        scores = score_universe(fundamentals, sector_map, weights)

        # Log all scores
        for s in scores:
            print(f"[ValueStock] {s.summary()}")

        # Get current holdings for position limit check
        current_holdings = self.logger.get_value_stock_total_by_symbol()
        max_position = cfg.get("max_position_aud", 500.0)
        min_score = cfg.get("min_composite_score", 40.0)
        min_moat = cfg.get("min_moat", "NARROW")

        # Select best stock
        best = select_best_stock(scores, current_holdings, max_position, min_score, min_moat)

        # Log EV scorer decisions for all candidates
        self._log_ev_decisions(scores, best)

        if best is None:
            print("[ValueStock] No suitable stock found this cycle")
            return None

        # Place order
        price_aud = self.stock_trader.get_stock_price(best.symbol, exchange="ASX", currency="AUD")
        shares = int(amount_aud / price_aud)

        if shares < 1:
            print(f"[ValueStock] {best.symbol}: A${amount_aud:.2f} < 1 share (A${price_aud:.2f})")
            return None

        actual_cost_aud = shares * price_aud
        order_id = self.stock_trader.place_etf_buy(best.symbol, shares, exchange="ASX", currency="AUD")

        # Log to value_stock_holdings
        purchase_id = self.logger.log_value_stock_purchase(
            symbol=best.symbol,
            shares=shares,
            avg_cost_aud=price_aud,
            total_invested_aud=actual_cost_aud,
            composite_score=best.composite_score,
            moat_rating=best.moat_rating,
            ib_order_id=order_id,
            ev_score_json=json.dumps(asdict(best)),
            notes=f"EV score {best.composite_score:.1f}, moat={best.moat_rating}",
        )

        # Send notification with rationale
        runners_up = [s for s in scores if s.symbol != best.symbol and s.moat_rating != "NONE"][:3]
        notify.notify_value_stock_selection(
            self.bot_token, self.chat_id,
            selected=best,
            runners_up=runners_up,
            shares=shares,
            price_aud=price_aud,
            cost_aud=actual_cost_aud,
        )

        print(f"[ValueStock] Purchased {shares} x {best.symbol} @ A${price_aud:.2f} (score={best.composite_score:.1f})")

        return {
            "symbol": best.symbol,
            "type": "value_stock",
            "shares": shares,
            "price_aud": price_aud,
            "cost_aud": actual_cost_aud,
            "cost_usd": round(actual_cost_aud / usd_aud_rate, 2),
            "order_id": order_id,
            "purchase_id": purchase_id,
            "ev_score": best.composite_score,
            "moat": best.moat_rating,
        }

    def _log_dip_decision(
        self, deploying: bool, dip_signal, pending_aud: float,
        days_since: int | None,
    ) -> None:
        """Log a dip detector deploy/hold decision."""
        if not self.session_id:
            return
        try:
            if deploying and dip_signal.is_dip:
                decision, rule = "deploy", f"dip: {', '.join(dip_signal.triggers)}"
            elif deploying:
                rule = f"max_wait_reached (days={days_since})"
                decision = "deploy"
            else:
                decision, rule = "hold", "no_dip"

            context = {
                "pending_aud": round(pending_aud, 2),
                "is_dip": dip_signal.is_dip,
                "confidence": dip_signal.confidence,
                "triggers": dip_signal.triggers,
                "days_since_last_deploy": days_since,
            }
            context.update(dip_signal.macro_snapshot)

            log_decision(self.logger, FeatureDecision(
                feature="dip_detector",
                symbol="PORTFOLIO",
                strategy="reallocation",
                decision=decision,
                rule=rule,
                context=context,
                session_id=self.session_id,
            ))
        except Exception as e:
            print(f"[FeatureTracker] Failed to log dip decision: {e}")

    def _log_ev_decisions(self, scores: list, best) -> None:
        """Log EV scorer accept/reject decisions for all candidates."""
        if not self.session_id:
            return
        try:
            for s in scores:
                context = {
                    "composite_score": s.composite_score,
                    "valuation_score": s.valuation_score,
                    "quality_score": s.quality_score,
                    "safety_score": s.safety_score,
                    "moat_rating": s.moat_rating,
                    "ev_to_ebitda": s.ev_to_ebitda,
                    "trailing_pe": s.trailing_pe,
                    "roe": s.roe,
                }
                if s.selected:
                    decision = "accept"
                    rule = f"score={s.composite_score:.1f}, moat={s.moat_rating}"
                else:
                    decision = "reject"
                    rule = s.reject_reason or f"not_selected (score={s.composite_score:.1f})"

                log_decision(self.logger, FeatureDecision(
                    feature="ev_scorer",
                    symbol=s.symbol,
                    strategy="reallocation",
                    decision=decision,
                    rule=rule,
                    context=context,
                    session_id=self.session_id,
                ))
        except Exception as e:
            print(f"[FeatureTracker] Failed to log EV decisions: {e}")

    def get_portfolio_summary(self) -> dict:
        """Get current portfolio summary (ETFs + value stocks)."""
        holdings = self.logger.get_etf_holdings_summary()
        total_etf = self.logger.get_total_etf_invested()
        value_holdings = self.logger.get_value_stock_total_by_symbol()
        pending = self.logger.get_pending_reallocation_total()

        return {
            "etf_holdings": holdings,
            "total_etf_invested_usd": total_etf,
            "value_stock_holdings": value_holdings,
            "total_value_stocks_aud": sum(value_holdings.values()),
            "pending_reallocation_usd": pending,
        }
