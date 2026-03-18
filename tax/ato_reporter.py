"""
ATO Tax Reporter
-----------------
Generates tax reports for the Australian Tax Office (ATO).
Supports both classifications:
  - Investor: CGT events schedule (capital gains/losses per trade)
  - Trader: Business income/loss with deductible expenses

Australian Financial Year: July 1 → June 30.
All amounts converted to AUD using the broker's USD/AUD rate at trade time.
"""
import csv
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path

from logs.trade_logger import TradeLogger


class ATOReporter:
    def __init__(self, db_path: str = "logs/trades.db"):
        self.db_path = db_path
        self.logger = TradeLogger(db_path)

    def close(self):
        self.logger.close()

    @staticmethod
    def get_fy_bounds(fy_year: str) -> tuple[str, str]:
        """
        Convert FY string to date bounds.
        '2025-26' → ('2025-07-01', '2026-06-30')
        """
        parts = fy_year.split("-")
        start_year = int(parts[0])
        return f"{start_year}-07-01", f"{start_year + 1}-06-30"

    # ── Investor Report (CGT Schedule) ───────────────────────────────────────

    def generate_investor_report(self, fy_year: str) -> list[dict]:
        """
        Generate CGT schedule data for 'investor' classification.

        Each closed trade is a CGT event with:
        - Acquisition date (opened_at) and disposal date (closed_at)
        - Cost base in AUD (entry cost + entry commission)
        - Capital proceeds in AUD (exit proceeds - exit commission)
        - Net capital gain/loss
        - Holding period in days (always < 365 for bot trades, so no 50% discount)
        """
        fy_start, fy_end = self.get_fy_bounds(fy_year)
        trades = self.logger.get_fy_trades(fy_start, fy_end)

        events = []
        for t in trades:
            if t["result"] in ("NO_TRIGGER", "CANCELLED"):
                continue  # Not a CGT event — no disposal occurred

            usd_aud = t.get("usd_aud_rate") or 1.54  # fallback
            lot_size = t.get("lot_size") or 0
            entry_price = t.get("entry_price") or 0
            exit_price = t.get("exit_price") or 0
            comm_entry = (t.get("commission_entry") or 0) * usd_aud
            comm_exit = (t.get("commission_exit") or 0) * usd_aud
            pnl_usd = t.get("pnl_usd") or 0
            pnl_aud = t.get("pnl_aud") or round(pnl_usd * usd_aud, 2)

            # Calculate holding period
            opened = t.get("opened_at") or ""
            closed = t.get("closed_at") or ""
            holding_days = 0
            if opened and closed:
                try:
                    dt_open = datetime.fromisoformat(opened)
                    dt_close = datetime.fromisoformat(closed)
                    holding_days = (dt_close - dt_open).days
                except ValueError:
                    pass

            # Cost base and proceeds depend on direction
            # For BUY: cost base = what you paid to acquire, proceeds = what you received on disposal
            # For SELL (short): cost base = buy-back cost, proceeds = initial sell
            direction = t.get("direction", "BUY")

            # Gross notional = pnl in USD is already calculated correctly by the bot
            # For ATO: cost base = entry cost + commissions; proceeds = exit value
            # Simplified: use P&L directly since forex margin doesn't have a "cost base" in the traditional sense
            # ATO guidance: for forex CFDs/margin, report the net gain/loss per transaction
            total_commission_aud = round(comm_entry + comm_exit, 2)

            events.append({
                "trade_id": t["id"],
                "pair": t["pair"],
                "direction": direction,
                "acquisition_date": opened[:10] if opened else "",
                "disposal_date": closed[:10] if closed else "",
                "settlement_date": t.get("settlement_date") or "",
                "holding_days": holding_days,
                "cgt_discount_eligible": holding_days >= 365,  # always False for bot trades
                "pnl_usd": pnl_usd,
                "pnl_aud": pnl_aud,
                "commission_aud": total_commission_aud,
                "net_gain_loss_aud": round(pnl_aud - total_commission_aud, 2),
                "result": t["result"],
                "lot_size": lot_size,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "usd_aud_rate": usd_aud,
            })

        return events

    # ── Trader Report (Business Income) ──────────────────────────────────────

    def generate_trader_report(self, fy_year: str) -> dict:
        """
        Generate business income summary for 'trader' classification.

        All trading P&L is ordinary income/loss (not CGT).
        Trading expenses (VPS, data, software) are deductible.
        """
        fy_start, fy_end = self.get_fy_bounds(fy_year)
        trades = self.logger.get_fy_trades(fy_start, fy_end)
        expenses = self.logger.get_fy_expenses(fy_start, fy_end)

        # Filter to actual trades (not NO_TRIGGER/CANCELLED)
        actual = [t for t in trades if t["result"] in ("TP", "SL", "TIME_EXIT")]

        wins = [t for t in actual if (t.get("pnl_usd") or 0) > 0]
        losses = [t for t in actual if (t.get("pnl_usd") or 0) <= 0]

        gross_income_aud = sum(
            (t.get("pnl_aud") or round((t.get("pnl_usd") or 0) * (t.get("usd_aud_rate") or 1.54), 2))
            for t in wins
        )
        total_losses_aud = sum(
            abs(t.get("pnl_aud") or round((t.get("pnl_usd") or 0) * (t.get("usd_aud_rate") or 1.54), 2))
            for t in losses
        )
        total_commissions_aud = sum(
            (t.get("commission_usd") or 0) * (t.get("usd_aud_rate") or 1.54)
            for t in actual
        )

        # Expenses by category
        expense_by_cat = {}
        total_expenses = 0.0
        for e in expenses:
            cat = e["category"]
            amt = e["amount_aud"]
            expense_by_cat[cat] = expense_by_cat.get(cat, 0) + amt
            total_expenses += amt

        net_trading_pnl = round(gross_income_aud - total_losses_aud, 2)
        net_business_income = round(net_trading_pnl - total_commissions_aud - total_expenses, 2)

        avg_win = round(gross_income_aud / len(wins), 2) if wins else 0
        avg_loss = round(total_losses_aud / len(losses), 2) if losses else 0

        return {
            "fy_year": fy_year,
            "trade_count": len(actual),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(len(wins) / len(actual) * 100, 1) if actual else 0,
            "gross_trading_income_aud": round(gross_income_aud, 2),
            "total_trading_losses_aud": round(total_losses_aud, 2),
            "net_trading_pnl_aud": net_trading_pnl,
            "total_commissions_aud": round(total_commissions_aud, 2),
            "total_expenses_aud": round(total_expenses, 2),
            "deductible_expenses": [
                {"category": cat, "total_aud": round(amt, 2)}
                for cat, amt in sorted(expense_by_cat.items())
            ],
            "net_business_income_aud": net_business_income,
            "avg_win_aud": avg_win,
            "avg_loss_aud": avg_loss,
        }

    # ── Summary (both classifications) ───────────────────────────────────────

    def generate_summary(self, fy_year: str) -> dict:
        """High-level stats useful for either tax classification."""
        fy_start, fy_end = self.get_fy_bounds(fy_year)
        trades = self.logger.get_fy_trades(fy_start, fy_end)
        actual = [t for t in trades if t["result"] in ("TP", "SL", "TIME_EXIT")]

        total_pnl_usd = sum(t.get("pnl_usd") or 0 for t in actual)
        total_pnl_aud = sum(
            (t.get("pnl_aud") or round((t.get("pnl_usd") or 0) * (t.get("usd_aud_rate") or 1.54), 2))
            for t in actual
        )
        total_comm_usd = sum(t.get("commission_usd") or 0 for t in actual)
        total_comm_aud = sum(
            (t.get("commission_usd") or 0) * (t.get("usd_aud_rate") or 1.54)
            for t in actual
        )

        # Monthly breakdown
        monthly = {}
        for t in actual:
            closed = t.get("closed_at") or ""
            if len(closed) >= 7:
                month = closed[:7]  # YYYY-MM
                if month not in monthly:
                    monthly[month] = {"month": month, "trades": 0, "pnl_aud": 0}
                monthly[month]["trades"] += 1
                pnl = t.get("pnl_aud") or round((t.get("pnl_usd") or 0) * (t.get("usd_aud_rate") or 1.54), 2)
                monthly[month]["pnl_aud"] = round(monthly[month]["pnl_aud"] + pnl, 2)

        wins = [t for t in actual if (t.get("pnl_usd") or 0) > 0]

        return {
            "fy_year": fy_year,
            "total_trades": len(actual),
            "total_wins": len(wins),
            "total_losses": len(actual) - len(wins),
            "win_rate": round(len(wins) / len(actual) * 100, 1) if actual else 0,
            "gross_pnl_usd": round(total_pnl_usd, 2),
            "gross_pnl_aud": round(total_pnl_aud, 2),
            "total_commissions_usd": round(total_comm_usd, 2),
            "total_commissions_aud": round(total_comm_aud, 2),
            "net_pnl_aud": round(total_pnl_aud - total_comm_aud, 2),
            "monthly_breakdown": sorted(monthly.values(), key=lambda x: x["month"]),
        }

    # ── CSV Export ────────────────────────────────────────────────────────────

    def export_csv(self, report_type: str, fy_year: str, output_path: str) -> str:
        """
        Export tax report to CSV.

        Args:
            report_type: 'investor' or 'trader'
            fy_year: e.g. '2025-26'
            output_path: file path for CSV output

        Returns:
            Absolute path to the generated CSV
        """
        path = Path(output_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        if report_type == "investor":
            return self._export_investor_csv(fy_year, path)
        elif report_type == "trader":
            return self._export_trader_csv(fy_year, path)
        else:
            raise ValueError(f"Unknown report type: {report_type}. Use 'investor' or 'trader'.")

    def _export_investor_csv(self, fy_year: str, path: Path) -> str:
        """Export CGT events to CSV — one row per trade."""
        events = self.generate_investor_report(fy_year)

        fieldnames = [
            "trade_id", "pair", "direction", "acquisition_date", "disposal_date",
            "settlement_date", "holding_days", "cgt_discount_eligible",
            "entry_price", "exit_price", "lot_size",
            "pnl_usd", "pnl_aud", "commission_aud", "net_gain_loss_aud",
            "usd_aud_rate", "result",
        ]

        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(events)

        # Append summary row
        if events:
            total_net = sum(e["net_gain_loss_aud"] for e in events)
            total_comm = sum(e["commission_aud"] for e in events)
            with open(path, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([])
                writer.writerow(["SUMMARY", f"FY {fy_year}"])
                writer.writerow(["Total CGT events", len(events)])
                writer.writerow(["Net capital gain/loss (AUD)", f"{total_net:.2f}"])
                writer.writerow(["Total commissions (AUD)", f"{total_comm:.2f}"])
                gains = [e for e in events if e["net_gain_loss_aud"] > 0]
                losses = [e for e in events if e["net_gain_loss_aud"] <= 0]
                writer.writerow(["Capital gains count", len(gains)])
                writer.writerow(["Capital losses count", len(losses)])
                if gains:
                    writer.writerow(["Total capital gains (AUD)", f"{sum(e['net_gain_loss_aud'] for e in gains):.2f}"])
                if losses:
                    writer.writerow(["Total capital losses (AUD)", f"{sum(e['net_gain_loss_aud'] for e in losses):.2f}"])

        print(f"[Tax] Investor CGT report exported: {path} ({len(events)} events)")
        return str(path.resolve())

    def _export_trader_csv(self, fy_year: str, path: Path) -> str:
        """Export business income report to CSV — summary + trade details."""
        report = self.generate_trader_report(fy_year)
        fy_start, fy_end = self.get_fy_bounds(fy_year)
        trades = self.logger.get_fy_trades(fy_start, fy_end)
        actual = [t for t in trades if t["result"] in ("TP", "SL", "TIME_EXIT")]

        with open(path, "w", newline="") as f:
            writer = csv.writer(f)

            # Summary section
            writer.writerow([f"BUSINESS INCOME REPORT — FY {fy_year}"])
            writer.writerow([])
            writer.writerow(["Metric", "Value"])
            writer.writerow(["Total trades", report["trade_count"]])
            writer.writerow(["Win rate", f"{report['win_rate']}%"])
            writer.writerow(["Gross trading income (AUD)", f"{report['gross_trading_income_aud']:.2f}"])
            writer.writerow(["Total trading losses (AUD)", f"{report['total_trading_losses_aud']:.2f}"])
            writer.writerow(["Net trading P&L (AUD)", f"{report['net_trading_pnl_aud']:.2f}"])
            writer.writerow(["Total commissions (AUD)", f"{report['total_commissions_aud']:.2f}"])
            writer.writerow(["Total expenses (AUD)", f"{report['total_expenses_aud']:.2f}"])
            writer.writerow(["Net business income (AUD)", f"{report['net_business_income_aud']:.2f}"])
            writer.writerow([])

            # Deductible expenses breakdown
            writer.writerow(["DEDUCTIBLE EXPENSES"])
            writer.writerow(["Category", "Total (AUD)"])
            for exp in report["deductible_expenses"]:
                writer.writerow([exp["category"], f"{exp['total_aud']:.2f}"])
            writer.writerow([])

            # Trade details
            writer.writerow(["TRADE DETAILS"])
            detail_fields = [
                "id", "pair", "direction", "opened_at", "closed_at",
                "entry_price", "exit_price", "lot_size", "result",
                "pips", "pnl_usd", "pnl_aud", "commission_usd", "usd_aud_rate",
            ]
            writer.writerow(detail_fields)
            for t in actual:
                writer.writerow([t.get(f, "") for f in detail_fields])

        print(f"[Tax] Trader business income report exported: {path} ({report['trade_count']} trades)")
        return str(path.resolve())

    # ── Expense Management ───────────────────────────────────────────────────

    def add_expense(
        self,
        date: str,
        category: str,
        description: str,
        amount_aud: float,
        amount_usd: float = None,
        receipt_ref: str = None,
    ) -> int:
        """Add a deductible expense."""
        return self.logger.log_expense(
            expense_date=date,
            category=category,
            description=description,
            amount_aud=amount_aud,
            amount_usd=amount_usd,
            receipt_ref=receipt_ref,
        )
