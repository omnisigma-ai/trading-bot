"""
Tax Report CLI
--------------
Command-line interface for generating ATO tax reports and managing expenses.

Usage:
    python -m tax.cli --report investor --fy 2025-26
    python -m tax.cli --report trader --fy 2025-26
    python -m tax.cli --report investor --fy 2025-26 --output reports/cgt_2025-26.csv
    python -m tax.cli --summary --fy 2025-26
    python -m tax.cli --add-expense --date 2025-08-01 --category VPS --amount 15.00 --desc "DigitalOcean monthly"
    python -m tax.cli --list-expenses --fy 2025-26
"""
import argparse
import json
import sys

from tax.ato_reporter import ATOReporter


def main():
    parser = argparse.ArgumentParser(
        description="ATO Tax Report Generator for London Breakout Trading Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m tax.cli --summary --fy 2025-26
  python -m tax.cli --report investor --fy 2025-26 --output cgt_report.csv
  python -m tax.cli --report trader --fy 2025-26
  python -m tax.cli --add-expense --date 2025-08-01 --category VPS --amount 15.00 --desc "Vultr"
  python -m tax.cli --list-expenses --fy 2025-26

Expense categories: VPS, DATA_FEED, SOFTWARE, EDUCATION, OTHER
        """,
    )

    # Report generation
    parser.add_argument("--report", choices=["investor", "trader"],
                        help="Generate tax report (investor=CGT schedule, trader=business income)")
    parser.add_argument("--summary", action="store_true",
                        help="Show high-level FY summary")
    parser.add_argument("--fy", type=str,
                        help="Australian financial year (e.g. 2025-26)")
    parser.add_argument("--output", "-o", type=str,
                        help="Output CSV file path (default: reports/<type>_<fy>.csv)")

    # Expense management
    parser.add_argument("--add-expense", action="store_true",
                        help="Add a deductible expense")
    parser.add_argument("--list-expenses", action="store_true",
                        help="List expenses for a financial year")
    parser.add_argument("--date", type=str, help="Expense date (YYYY-MM-DD)")
    parser.add_argument("--category", type=str,
                        choices=["VPS", "DATA_FEED", "SOFTWARE", "EDUCATION", "OTHER"],
                        help="Expense category")
    parser.add_argument("--amount", type=float, help="Amount in AUD")
    parser.add_argument("--desc", type=str, help="Expense description")
    parser.add_argument("--receipt", type=str, help="Receipt reference (optional)")

    # Database
    parser.add_argument("--db", type=str, default="logs/trades.db",
                        help="Path to trades database (default: logs/trades.db)")

    args = parser.parse_args()

    reporter = ATOReporter(args.db)

    try:
        if args.summary:
            _handle_summary(reporter, args)
        elif args.report:
            _handle_report(reporter, args)
        elif args.add_expense:
            _handle_add_expense(reporter, args)
        elif args.list_expenses:
            _handle_list_expenses(reporter, args)
        else:
            parser.print_help()
    finally:
        reporter.close()


def _handle_summary(reporter: ATOReporter, args):
    if not args.fy:
        print("Error: --fy required (e.g. --fy 2025-26)")
        sys.exit(1)

    summary = reporter.generate_summary(args.fy)
    print(f"\n{'='*55}")
    print(f"  ATO TAX SUMMARY — FY {summary['fy_year']}")
    print(f"{'='*55}")
    print(f"  Total trades:          {summary['total_trades']}")
    print(f"  Wins / Losses:         {summary['total_wins']} / {summary['total_losses']}")
    print(f"  Win rate:              {summary['win_rate']}%")
    print(f"  Gross P&L (USD):       ${summary['gross_pnl_usd']:,.2f}")
    print(f"  Gross P&L (AUD):       A${summary['gross_pnl_aud']:,.2f}")
    print(f"  Total commissions:     A${summary['total_commissions_aud']:,.2f}")
    print(f"  Net P&L (AUD):         A${summary['net_pnl_aud']:,.2f}")

    if summary["monthly_breakdown"]:
        print(f"\n  Monthly Breakdown:")
        print(f"  {'Month':<10} {'Trades':>7} {'P&L (AUD)':>12}")
        print(f"  {'-'*10} {'-'*7} {'-'*12}")
        for m in summary["monthly_breakdown"]:
            print(f"  {m['month']:<10} {m['trades']:>7} A${m['pnl_aud']:>10,.2f}")

    print(f"{'='*55}\n")

    if summary["total_trades"] == 0:
        print("  No trades found for this financial year.")
        print("  Start paper trading to collect data.\n")


def _handle_report(reporter: ATOReporter, args):
    if not args.fy:
        print("Error: --fy required (e.g. --fy 2025-26)")
        sys.exit(1)

    output = args.output or f"reports/{args.report}_{args.fy}.csv"
    path = reporter.export_csv(args.report, args.fy, output)
    print(f"\nReport saved to: {path}")

    # Also print summary to console
    if args.report == "investor":
        events = reporter.generate_investor_report(args.fy)
        if events:
            total = sum(e["net_gain_loss_aud"] for e in events)
            print(f"\nCGT Events: {len(events)}")
            print(f"Net capital {'gain' if total >= 0 else 'loss'} (AUD): A${total:,.2f}")
            print(f"\nNote: Bot trades are held < 12 months — no 50% CGT discount applies.")
        else:
            print("\nNo CGT events for this period.")

    elif args.report == "trader":
        report = reporter.generate_trader_report(args.fy)
        print(f"\nNet business income (AUD): A${report['net_business_income_aud']:,.2f}")
        if report["deductible_expenses"]:
            print(f"Deductible expenses:")
            for exp in report["deductible_expenses"]:
                print(f"  {exp['category']}: A${exp['total_aud']:,.2f}")


def _handle_add_expense(reporter: ATOReporter, args):
    if not all([args.date, args.category, args.amount]):
        print("Error: --date, --category, and --amount are required")
        print("Example: --add-expense --date 2025-08-01 --category VPS --amount 15.00 --desc 'DigitalOcean'")
        sys.exit(1)

    eid = reporter.add_expense(
        date=args.date,
        category=args.category,
        description=args.desc or "",
        amount_aud=args.amount,
        receipt_ref=args.receipt,
    )
    print(f"Expense #{eid} added: {args.date} | {args.category} | A${args.amount:.2f} | {args.desc or ''}")


def _handle_list_expenses(reporter: ATOReporter, args):
    if not args.fy:
        print("Error: --fy required (e.g. --fy 2025-26)")
        sys.exit(1)

    fy_start, fy_end = ATOReporter.get_fy_bounds(args.fy)
    expenses = reporter.logger.get_fy_expenses(fy_start, fy_end)

    if not expenses:
        print(f"No expenses recorded for FY {args.fy}")
        return

    print(f"\nExpenses for FY {args.fy}:")
    print(f"{'ID':>4} {'Date':<12} {'Category':<12} {'Amount (AUD)':>13} {'Description'}")
    print(f"{'-'*4} {'-'*12} {'-'*12} {'-'*13} {'-'*30}")
    total = 0
    for e in expenses:
        print(f"{e['id']:>4} {e['expense_date']:<12} {e['category']:<12} A${e['amount_aud']:>10,.2f} {e.get('description', '')}")
        total += e["amount_aud"]
    print(f"\nTotal: A${total:,.2f}")


if __name__ == "__main__":
    main()
