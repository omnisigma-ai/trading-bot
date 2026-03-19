"""
Trade Logger
------------
SQLite journal for all trades, signals, execution events, account snapshots,
and deductible expenses. Supports schema migration for safe upgrades.
"""
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path


CREATE_TRADES = """
CREATE TABLE IF NOT EXISTS trades (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pair            TEXT NOT NULL,
    direction       TEXT NOT NULL,         -- BUY | SELL
    entry_price     REAL,
    exit_price      REAL,
    stop_loss       REAL,
    take_profit     REAL,
    lot_size        REAL,
    sl_pips         REAL,
    tp_pips         REAL,
    result          TEXT,                  -- TP | SL | TIME_EXIT | CANCELLED | NO_TRIGGER
    pips            REAL,
    pnl_usd         REAL,
    opened_at       TEXT,
    closed_at       TEXT,
    ib_order_id     INTEGER,
    notes           TEXT
)
"""

CREATE_SIGNALS = """
CREATE TABLE IF NOT EXISTS signals (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    pair            TEXT NOT NULL,
    signal_date     TEXT NOT NULL,
    signal_time_utc TEXT NOT NULL,
    range_high      REAL,
    range_low       REAL,
    range_size_pips REAL,
    buy_entry       REAL,
    buy_sl          REAL,
    buy_tp          REAL,
    sell_entry      REAL,
    sell_sl         REAL,
    sell_tp         REAL,
    sl_pips         REAL,
    tp_pips         REAL,
    traded          INTEGER DEFAULT 0,
    skip_reason     TEXT,
    trade_id        INTEGER,
    FOREIGN KEY (trade_id) REFERENCES trades(id)
)
"""

CREATE_EXECUTION_EVENTS = """
CREATE TABLE IF NOT EXISTS execution_events (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id        INTEGER,
    ib_order_id     INTEGER,
    event_type      TEXT NOT NULL,         -- PLACED | FILLED | CANCELLED | EXPIRED | ERROR
    event_time      TEXT NOT NULL,
    order_type      TEXT,                  -- ENTRY | TP | SL
    price           REAL,
    quantity         REAL,
    commission      REAL DEFAULT 0,
    commission_currency TEXT DEFAULT 'USD',
    ib_exec_id      TEXT,
    notes           TEXT,
    FOREIGN KEY (trade_id) REFERENCES trades(id)
)
"""

CREATE_ACCOUNT_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS account_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   TEXT NOT NULL,
    snapshot_time   TEXT NOT NULL,
    net_liquidation REAL NOT NULL,
    net_liquidation_aud REAL,
    usd_aud_rate    REAL,
    realised_pnl_today REAL DEFAULT 0,
    open_positions  INTEGER DEFAULT 0,
    notes           TEXT
)
"""

CREATE_EXPENSES = """
CREATE TABLE IF NOT EXISTS expenses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    expense_date    TEXT NOT NULL,
    category        TEXT NOT NULL,         -- VPS | DATA_FEED | SOFTWARE | EDUCATION | OTHER
    description     TEXT,
    amount_aud      REAL NOT NULL,
    amount_usd      REAL,
    receipt_ref     TEXT,
    fy_year         TEXT
)
"""

# Columns to add to the trades table via migration (name, type + default)
TRADES_MIGRATIONS = [
    ("fill_price", "REAL"),
    ("slippage_pips", "REAL"),
    ("spread_at_entry", "REAL"),
    ("entry_fill_time", "TEXT"),
    ("exit_fill_time", "TEXT"),
    ("commission_entry", "REAL DEFAULT 0"),
    ("commission_exit", "REAL DEFAULT 0"),
    ("commission_usd", "REAL DEFAULT 0"),
    ("pnl_aud", "REAL"),
    ("usd_aud_rate", "REAL"),
    ("settlement_date", "TEXT"),
    ("strategy", "TEXT DEFAULT 'london_breakout'"),
    ("instrument_type", "TEXT DEFAULT 'forex'"),
]

# Columns to add to signals table via migration
SIGNALS_MIGRATIONS = [
    ("strategy", "TEXT DEFAULT 'london_breakout'"),
]

# ── New tables for wealth builder ──────────────────────────────────────────

CREATE_REALLOCATIONS = """
CREATE TABLE IF NOT EXISTS reallocations (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    source_trade_id INTEGER NOT NULL,
    source_strategy TEXT NOT NULL,
    profit_usd      REAL NOT NULL,
    earmarked_usd   REAL NOT NULL,
    earmarked_aud   REAL,
    status          TEXT DEFAULT 'pending',
    etf_purchase_id INTEGER,
    created_at      TEXT NOT NULL,
    purchased_at    TEXT,
    FOREIGN KEY (source_trade_id) REFERENCES trades(id)
)
"""

CREATE_ETF_HOLDINGS = """
CREATE TABLE IF NOT EXISTS etf_holdings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    shares          REAL NOT NULL,
    avg_cost_usd    REAL NOT NULL,
    avg_cost_aud    REAL,
    total_invested_usd REAL NOT NULL,
    total_invested_aud REAL,
    ib_order_id     INTEGER,
    purchased_at    TEXT NOT NULL,
    usd_aud_rate    REAL,
    notes           TEXT
)
"""

CREATE_PORTFOLIO_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   TEXT NOT NULL,
    trading_balance_usd REAL,
    etf_value_usd   REAL,
    total_wealth_usd REAL,
    total_wealth_aud REAL,
    usd_aud_rate    REAL,
    pending_reallocation_usd REAL,
    notes           TEXT
)
"""

CREATE_FUNDAMENTAL_CACHE = """
CREATE TABLE IF NOT EXISTS fundamental_cache (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    trailing_pe     REAL,
    forward_pe      REAL,
    price_to_book   REAL,
    ev_to_ebitda    REAL,
    ev_to_revenue   REAL,
    fcf_yield       REAL,
    roe             REAL,
    operating_margin REAL,
    gross_margin    REAL,
    roic            REAL,
    debt_to_equity  REAL,
    interest_coverage REAL,
    market_cap      REAL,
    dividend_yield  REAL,
    fetched_at      TEXT NOT NULL
)
"""

CREATE_VALUE_STOCK_HOLDINGS = """
CREATE TABLE IF NOT EXISTS value_stock_holdings (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    shares          REAL NOT NULL,
    avg_cost_aud    REAL NOT NULL,
    total_invested_aud REAL NOT NULL,
    composite_score REAL,
    moat_rating     TEXT,
    ib_order_id     INTEGER,
    purchased_at    TEXT NOT NULL,
    ev_score_json   TEXT,
    notes           TEXT
)
"""

CREATE_FEATURE_DECISIONS = """
CREATE TABLE IF NOT EXISTS feature_decisions (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp       TEXT NOT NULL,
    session_id      TEXT NOT NULL,
    feature         TEXT NOT NULL,
    symbol          TEXT,
    strategy        TEXT,
    decision        TEXT NOT NULL,
    rule            TEXT,
    context_json    TEXT,
    outcome         TEXT,
    outcome_pnl     REAL,
    counterfactual  TEXT,
    counterfactual_pnl REAL,
    backfilled      INTEGER DEFAULT 0
)
"""

CREATE_MACRO_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS macro_snapshots (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_date   TEXT NOT NULL,
    vix             REAL,
    us_10y_yield    REAL,
    gold            REAL,
    oil_wti         REAL,
    aud_usd         REAL,
    dxy             REAL,
    vix_5d_chg      REAL,
    gold_5d_chg     REAL,
    oil_5d_chg      REAL,
    aud_usd_5d_chg  REAL,
    dxy_5d_chg      REAL,
    is_dip          INTEGER DEFAULT 0,
    dip_confidence  REAL DEFAULT 0,
    dip_triggers    TEXT,
    deployed        INTEGER DEFAULT 0,
    notes           TEXT
)
"""


class TradeLogger:
    def __init__(self, db_path: str = "logs/trades.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._create_tables()
        self._migrate()

    def _create_tables(self):
        """Create all tables if they don't exist."""
        for ddl in [CREATE_TRADES, CREATE_SIGNALS, CREATE_EXECUTION_EVENTS,
                     CREATE_ACCOUNT_SNAPSHOTS, CREATE_EXPENSES,
                     CREATE_REALLOCATIONS, CREATE_ETF_HOLDINGS,
                     CREATE_PORTFOLIO_SNAPSHOTS,
                     CREATE_FUNDAMENTAL_CACHE, CREATE_VALUE_STOCK_HOLDINGS,
                     CREATE_MACRO_SNAPSHOTS, CREATE_FEATURE_DECISIONS]:
            self.conn.execute(ddl)
        self.conn.commit()

    def _migrate(self):
        """Add new columns to existing tables if missing (safe for existing data)."""
        # Trades table migrations
        existing = {row[1] for row in self.conn.execute("PRAGMA table_info(trades)").fetchall()}
        for col_name, col_type in TRADES_MIGRATIONS:
            if col_name not in existing:
                self.conn.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")

        # Signals table migrations
        existing_sig = {row[1] for row in self.conn.execute("PRAGMA table_info(signals)").fetchall()}
        for col_name, col_type in SIGNALS_MIGRATIONS:
            if col_name not in existing_sig:
                self.conn.execute(f"ALTER TABLE signals ADD COLUMN {col_name} {col_type}")

        self.conn.commit()

    # ── trades ───────────────────────────────────────────────────────────────

    def log_trade_opened(
        self,
        pair: str,
        direction: str,
        entry_price: float,
        stop_loss: float,
        take_profit: float,
        lot_size: float,
        sl_pips: float,
        tp_pips: float,
        ib_order_id: int = None,
    ) -> int:
        """Insert a new trade record, returns row id."""
        cur = self.conn.execute(
            """INSERT INTO trades
               (pair, direction, entry_price, stop_loss, take_profit,
                lot_size, sl_pips, tp_pips, opened_at, ib_order_id)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (pair, direction, entry_price, stop_loss, take_profit,
             lot_size, sl_pips, tp_pips,
             datetime.utcnow().isoformat(), ib_order_id),
        )
        self.conn.commit()
        return cur.lastrowid

    def update_entry_fill(
        self,
        trade_id: int,
        fill_price: float,
        slippage_pips: float,
        commission_entry: float = 0,
        entry_fill_time: str = None,
        spread_at_entry: float = None,
    ) -> None:
        """Update trade with actual IB fill data on entry."""
        self.conn.execute(
            """UPDATE trades
               SET fill_price=?, slippage_pips=?, commission_entry=?,
                   entry_fill_time=?, spread_at_entry=?
               WHERE id=?""",
            (fill_price, slippage_pips, commission_entry,
             entry_fill_time, spread_at_entry, trade_id),
        )
        self.conn.commit()

    def log_trade_closed(
        self,
        trade_id: int,
        exit_price: float,
        result: str,
        pips: float,
        pnl_usd: float,
        commission_exit: float = 0,
        exit_fill_time: str = None,
        pnl_aud: float = None,
        usd_aud_rate: float = None,
        settlement_date: str = None,
    ) -> None:
        """Close a trade with exit details, commission, and AUD conversion."""
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """UPDATE trades
               SET exit_price=?, result=?, pips=?, pnl_usd=?, closed_at=?,
                   commission_exit=?,
                   commission_usd = COALESCE(commission_entry, 0) + ?,
                   exit_fill_time=?, pnl_aud=?, usd_aud_rate=?,
                   settlement_date=?
               WHERE id=?""",
            (exit_price, result, pips, pnl_usd, now,
             commission_exit, commission_exit,
             exit_fill_time, pnl_aud, usd_aud_rate,
             settlement_date, trade_id),
        )
        self.conn.commit()

    def update_commission(self, ib_order_id: int, commission: float, currency: str = "USD") -> None:
        """Update commission from a late-arriving IB commission report."""
        # Find which trade this order belongs to and which leg (entry or exit)
        row = self.conn.execute(
            "SELECT id, ib_order_id FROM trades WHERE ib_order_id=?", (ib_order_id,)
        ).fetchone()
        if row:
            self.conn.execute(
                """UPDATE trades SET commission_entry=?,
                   commission_usd = ? + COALESCE(commission_exit, 0)
                   WHERE id=?""",
                (commission, commission, row[0]),
            )
            self.conn.commit()

    # ── signals ──────────────────────────────────────────────────────────────

    def log_signal(
        self,
        pair: str,
        signal_date: str,
        signal_time_utc: str,
        range_high: float = None,
        range_low: float = None,
        range_size_pips: float = None,
        buy_entry: float = None,
        buy_sl: float = None,
        buy_tp: float = None,
        sell_entry: float = None,
        sell_sl: float = None,
        sell_tp: float = None,
        sl_pips: float = None,
        tp_pips: float = None,
        traded: bool = False,
        skip_reason: str = None,
        trade_id: int = None,
    ) -> int:
        """Log a signal evaluation (traded or skipped)."""
        cur = self.conn.execute(
            """INSERT INTO signals
               (pair, signal_date, signal_time_utc,
                range_high, range_low, range_size_pips,
                buy_entry, buy_sl, buy_tp, sell_entry, sell_sl, sell_tp,
                sl_pips, tp_pips, traded, skip_reason, trade_id)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (pair, signal_date, signal_time_utc,
             range_high, range_low, range_size_pips,
             buy_entry, buy_sl, buy_tp, sell_entry, sell_sl, sell_tp,
             sl_pips, tp_pips, 1 if traded else 0, skip_reason, trade_id),
        )
        self.conn.commit()
        return cur.lastrowid

    # ── execution events ─────────────────────────────────────────────────────

    def log_execution_event(
        self,
        trade_id: int,
        ib_order_id: int,
        event_type: str,
        event_time: str,
        order_type: str = None,
        price: float = None,
        quantity: float = None,
        commission: float = 0,
        commission_currency: str = "USD",
        ib_exec_id: str = None,
        notes: str = None,
    ) -> int:
        """Log an immutable execution event (fill, cancel, etc.)."""
        cur = self.conn.execute(
            """INSERT INTO execution_events
               (trade_id, ib_order_id, event_type, event_time,
                order_type, price, quantity, commission,
                commission_currency, ib_exec_id, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (trade_id, ib_order_id, event_type, event_time,
             order_type, price, quantity, commission,
             commission_currency, ib_exec_id, notes),
        )
        self.conn.commit()
        return cur.lastrowid

    # ── account snapshots ────────────────────────────────────────────────────

    def log_account_snapshot(
        self,
        net_liquidation: float,
        net_liquidation_aud: float = None,
        usd_aud_rate: float = None,
        realised_pnl_today: float = 0,
        open_positions: int = 0,
        notes: str = None,
    ) -> None:
        """Record daily account balance snapshot."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        now = datetime.utcnow().isoformat()
        self.conn.execute(
            """INSERT INTO account_snapshots
               (snapshot_date, snapshot_time, net_liquidation,
                net_liquidation_aud, usd_aud_rate,
                realised_pnl_today, open_positions, notes)
               VALUES (?,?,?,?,?,?,?,?)""",
            (today, now, net_liquidation, net_liquidation_aud,
             usd_aud_rate, realised_pnl_today, open_positions, notes),
        )
        self.conn.commit()

    def get_account_history(self, days: int = 90) -> list[dict]:
        """Returns daily balance snapshots for charting."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        cur = self.conn.execute(
            """SELECT snapshot_date, net_liquidation, net_liquidation_aud, usd_aud_rate
               FROM account_snapshots
               WHERE snapshot_date >= ?
               ORDER BY snapshot_date""",
            (cutoff,),
        )
        return [
            {"date": r[0], "balance_usd": r[1], "balance_aud": r[2], "usd_aud_rate": r[3]}
            for r in cur.fetchall()
        ]

    # ── expenses ─────────────────────────────────────────────────────────────

    def log_expense(
        self,
        expense_date: str,
        category: str,
        description: str,
        amount_aud: float,
        amount_usd: float = None,
        receipt_ref: str = None,
    ) -> int:
        """Log a deductible expense for ATO trader classification."""
        # Auto-calculate FY year from date
        from datetime import date as dt_date
        d = dt_date.fromisoformat(expense_date)
        fy_year = f"{d.year}-{str(d.year + 1)[2:]}" if d.month >= 7 else f"{d.year - 1}-{str(d.year)[2:]}"

        cur = self.conn.execute(
            """INSERT INTO expenses
               (expense_date, category, description, amount_aud,
                amount_usd, receipt_ref, fy_year)
               VALUES (?,?,?,?,?,?,?)""",
            (expense_date, category, description, amount_aud,
             amount_usd, receipt_ref, fy_year),
        )
        self.conn.commit()
        return cur.lastrowid

    # ── queries ──────────────────────────────────────────────────────────────

    def get_today_pnl(self) -> float:
        """Returns total realised P&L in USD for today (UTC)."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        cur = self.conn.execute(
            "SELECT COALESCE(SUM(pnl_usd), 0) FROM trades WHERE opened_at LIKE ? AND pnl_usd IS NOT NULL",
            (f"{today}%",),
        )
        return float(cur.fetchone()[0])

    def get_weekly_pnl(self) -> float:
        """Returns total realised P&L in USD for the last 7 days (UTC)."""
        week_ago = (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d")
        cur = self.conn.execute(
            "SELECT COALESCE(SUM(pnl_usd), 0) FROM trades WHERE opened_at >= ? AND pnl_usd IS NOT NULL",
            (week_ago,),
        )
        return float(cur.fetchone()[0])

    def get_consecutive_losses(self) -> int:
        """Returns the number of consecutive SL results at the end of trade history."""
        cur = self.conn.execute(
            "SELECT result FROM trades WHERE result IS NOT NULL ORDER BY closed_at DESC LIMIT 10"
        )
        rows = cur.fetchall()
        count = 0
        for (result,) in rows:
            if result == "SL":
                count += 1
            else:
                break
        return count

    def get_daily_summary(self, date: str = None) -> list[dict]:
        """Fetch all trades for a given date (YYYY-MM-DD). Defaults to today."""
        if date is None:
            date = datetime.utcnow().strftime("%Y-%m-%d")

        cur = self.conn.execute(
            "SELECT pair, direction, result, pips, pnl_usd FROM trades WHERE opened_at LIKE ?",
            (f"{date}%",),
        )
        rows = cur.fetchall()
        return [
            {"pair": r[0], "direction": r[1], "result": r[2], "pips": r[3], "pnl_usd": r[4]}
            for r in rows
        ]

    def get_fy_trades(self, fy_start: str, fy_end: str) -> list[dict]:
        """Fetch all closed trades within an Australian FY date range."""
        cur = self.conn.execute(
            """SELECT id, pair, direction, entry_price, exit_price, fill_price,
                      stop_loss, take_profit, lot_size, sl_pips, tp_pips,
                      result, pips, pnl_usd, pnl_aud, usd_aud_rate,
                      commission_entry, commission_exit, commission_usd,
                      opened_at, closed_at, settlement_date, ib_order_id
               FROM trades
               WHERE closed_at IS NOT NULL
                 AND closed_at >= ? AND closed_at < ?
               ORDER BY closed_at""",
            (fy_start, fy_end + "T23:59:59"),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_fy_expenses(self, fy_start: str, fy_end: str) -> list[dict]:
        """Fetch all expenses within a date range."""
        cur = self.conn.execute(
            """SELECT id, expense_date, category, description,
                      amount_aud, amount_usd, receipt_ref, fy_year
               FROM expenses
               WHERE expense_date >= ? AND expense_date <= ?
               ORDER BY expense_date""",
            (fy_start, fy_end),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── reallocations ─────────────────────────────────────────────────────

    def log_reallocation_pending(
        self,
        source_trade_id: int,
        source_strategy: str,
        profit_usd: float,
        earmarked_usd: float,
        earmarked_aud: float = None,
    ) -> int:
        """Earmark a portion of trade profit for ETF reallocation."""
        cur = self.conn.execute(
            """INSERT INTO reallocations
               (source_trade_id, source_strategy, profit_usd,
                earmarked_usd, earmarked_aud, status, created_at)
               VALUES (?,?,?,?,?,?,?)""",
            (source_trade_id, source_strategy, profit_usd,
             earmarked_usd, earmarked_aud, "pending",
             datetime.utcnow().isoformat()),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_pending_reallocation_total(self) -> float:
        """Returns total USD pending for ETF purchase."""
        cur = self.conn.execute(
            "SELECT COALESCE(SUM(earmarked_usd), 0) FROM reallocations WHERE status='pending'"
        )
        return float(cur.fetchone()[0])

    def get_pending_reallocations(self) -> list[dict]:
        """Returns all pending reallocation records."""
        cur = self.conn.execute(
            """SELECT id, source_trade_id, source_strategy, profit_usd,
                      earmarked_usd, created_at
               FROM reallocations WHERE status='pending'
               ORDER BY created_at"""
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def mark_reallocations_purchased(self, realloc_ids: list[int], etf_purchase_id: int) -> None:
        """Mark pending reallocations as purchased after ETF buy."""
        now = datetime.utcnow().isoformat()
        for rid in realloc_ids:
            self.conn.execute(
                """UPDATE reallocations
                   SET status='purchased', etf_purchase_id=?, purchased_at=?
                   WHERE id=?""",
                (etf_purchase_id, now, rid),
            )
        self.conn.commit()

    # ── ETF holdings ──────────────────────────────────────────────────────

    def log_etf_purchase(
        self,
        symbol: str,
        shares: float,
        avg_cost_usd: float,
        total_invested_usd: float,
        purchased_at: str = None,
        avg_cost_aud: float = None,
        total_invested_aud: float = None,
        ib_order_id: int = None,
        usd_aud_rate: float = None,
        notes: str = None,
    ) -> int:
        """Log an ETF purchase for the long-term portfolio."""
        if purchased_at is None:
            purchased_at = datetime.utcnow().isoformat()
        cur = self.conn.execute(
            """INSERT INTO etf_holdings
               (symbol, shares, avg_cost_usd, avg_cost_aud,
                total_invested_usd, total_invested_aud,
                ib_order_id, purchased_at, usd_aud_rate, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (symbol, shares, avg_cost_usd, avg_cost_aud,
             total_invested_usd, total_invested_aud,
             ib_order_id, purchased_at, usd_aud_rate, notes),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_etf_holdings_summary(self) -> list[dict]:
        """Returns aggregate ETF holdings (total shares + avg cost per symbol)."""
        cur = self.conn.execute(
            """SELECT symbol,
                      SUM(shares) as total_shares,
                      SUM(total_invested_usd) as total_invested_usd,
                      SUM(total_invested_usd) / SUM(shares) as avg_cost_usd
               FROM etf_holdings
               GROUP BY symbol
               ORDER BY symbol"""
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_total_etf_invested(self) -> float:
        """Total USD invested in ETFs."""
        cur = self.conn.execute(
            "SELECT COALESCE(SUM(total_invested_usd), 0) FROM etf_holdings"
        )
        return float(cur.fetchone()[0])

    # ── portfolio snapshots ───────────────────────────────────────────────

    def log_portfolio_snapshot(
        self,
        trading_balance_usd: float,
        etf_value_usd: float = 0.0,
        total_wealth_usd: float = None,
        total_wealth_aud: float = None,
        usd_aud_rate: float = None,
        pending_reallocation_usd: float = 0.0,
        notes: str = None,
    ) -> None:
        """Record combined portfolio snapshot."""
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if total_wealth_usd is None:
            total_wealth_usd = trading_balance_usd + etf_value_usd
        self.conn.execute(
            """INSERT INTO portfolio_snapshots
               (snapshot_date, trading_balance_usd, etf_value_usd,
                total_wealth_usd, total_wealth_aud, usd_aud_rate,
                pending_reallocation_usd, notes)
               VALUES (?,?,?,?,?,?,?,?)""",
            (today, trading_balance_usd, etf_value_usd,
             total_wealth_usd, total_wealth_aud, usd_aud_rate,
             pending_reallocation_usd, notes),
        )
        self.conn.commit()

    def get_portfolio_history(self, days: int = 90) -> list[dict]:
        """Returns portfolio snapshots for wealth tracking."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        cur = self.conn.execute(
            """SELECT snapshot_date, trading_balance_usd, etf_value_usd,
                      total_wealth_usd, total_wealth_aud
               FROM portfolio_snapshots
               WHERE snapshot_date >= ?
               ORDER BY snapshot_date""",
            (cutoff,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── fundamental cache ─────────────────────────────────────────────────

    def cache_fundamentals(self, symbol: str, metrics: dict) -> None:
        """Store yfinance fundamental data for an ASX stock."""
        self.conn.execute(
            """INSERT INTO fundamental_cache
               (symbol, trailing_pe, forward_pe, price_to_book, ev_to_ebitda,
                ev_to_revenue, fcf_yield, roe, operating_margin, gross_margin,
                roic, debt_to_equity, interest_coverage, market_cap,
                dividend_yield, fetched_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (symbol,
             metrics.get("trailing_pe"),
             metrics.get("forward_pe"),
             metrics.get("price_to_book"),
             metrics.get("ev_to_ebitda"),
             metrics.get("ev_to_revenue"),
             metrics.get("fcf_yield"),
             metrics.get("roe"),
             metrics.get("operating_margin"),
             metrics.get("gross_margin"),
             metrics.get("roic"),
             metrics.get("debt_to_equity"),
             metrics.get("interest_coverage"),
             metrics.get("market_cap"),
             metrics.get("dividend_yield"),
             datetime.utcnow().isoformat()),
        )
        self.conn.commit()

    def get_cached_fundamentals(
        self, symbols: list[str], max_age_hours: int = 24,
    ) -> dict[str, dict]:
        """
        Return cached fundamentals for symbols if fresh enough.
        Returns {symbol: {metric_name: value}} for symbols with valid cache.
        """
        cutoff = (datetime.utcnow() - timedelta(hours=max_age_hours)).isoformat()
        placeholders = ",".join("?" for _ in symbols)
        cur = self.conn.execute(
            f"""SELECT symbol, trailing_pe, forward_pe, price_to_book,
                       ev_to_ebitda, ev_to_revenue, fcf_yield, roe,
                       operating_margin, gross_margin, roic, debt_to_equity,
                       interest_coverage, market_cap, dividend_yield, fetched_at
                FROM fundamental_cache
                WHERE symbol IN ({placeholders}) AND fetched_at >= ?
                ORDER BY fetched_at DESC""",
            (*symbols, cutoff),
        )
        cols = [desc[0] for desc in cur.description]
        result = {}
        for row in cur.fetchall():
            d = dict(zip(cols, row))
            sym = d.pop("symbol")
            if sym not in result:  # keep most recent per symbol
                result[sym] = d
        return result

    # ── value stock holdings ───────────────────────────────────────────────

    def log_value_stock_purchase(
        self,
        symbol: str,
        shares: float,
        avg_cost_aud: float,
        total_invested_aud: float,
        composite_score: float = None,
        moat_rating: str = None,
        ib_order_id: int = None,
        ev_score_json: str = None,
        notes: str = None,
    ) -> int:
        """Log a value stock purchase selected by EV scoring."""
        cur = self.conn.execute(
            """INSERT INTO value_stock_holdings
               (symbol, shares, avg_cost_aud, total_invested_aud,
                composite_score, moat_rating, ib_order_id,
                purchased_at, ev_score_json, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            (symbol, shares, avg_cost_aud, total_invested_aud,
             composite_score, moat_rating, ib_order_id,
             datetime.utcnow().isoformat(), ev_score_json, notes),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_value_stock_total_by_symbol(self) -> dict[str, float]:
        """Returns {symbol: total_invested_aud} for position limit checks."""
        cur = self.conn.execute(
            """SELECT symbol, SUM(total_invested_aud)
               FROM value_stock_holdings
               GROUP BY symbol"""
        )
        return {row[0]: float(row[1]) for row in cur.fetchall()}

    # ── macro snapshots ────────────────────────────────────────────────────

    def log_macro_snapshot(
        self,
        snapshot_date: str,
        vix: float = None,
        us_10y_yield: float = None,
        gold: float = None,
        oil_wti: float = None,
        aud_usd: float = None,
        dxy: float = None,
        vix_5d_chg: float = None,
        gold_5d_chg: float = None,
        oil_5d_chg: float = None,
        aud_usd_5d_chg: float = None,
        dxy_5d_chg: float = None,
        is_dip: bool = False,
        dip_confidence: float = 0.0,
        dip_triggers: str = "",
        deployed: bool = False,
        notes: str = None,
    ) -> int:
        """Log daily macro indicator snapshot for dip detection + ML training."""
        cur = self.conn.execute(
            """INSERT INTO macro_snapshots
               (snapshot_date, vix, us_10y_yield, gold, oil_wti, aud_usd, dxy,
                vix_5d_chg, gold_5d_chg, oil_5d_chg, aud_usd_5d_chg, dxy_5d_chg,
                is_dip, dip_confidence, dip_triggers, deployed, notes)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (snapshot_date, vix, us_10y_yield, gold, oil_wti, aud_usd, dxy,
             vix_5d_chg, gold_5d_chg, oil_5d_chg, aud_usd_5d_chg, dxy_5d_chg,
             1 if is_dip else 0, dip_confidence, dip_triggers,
             1 if deployed else 0, notes),
        )
        self.conn.commit()
        return cur.lastrowid

    def get_last_deploy_date(self) -> str | None:
        """Returns the most recent date capital was deployed, or None."""
        cur = self.conn.execute(
            "SELECT snapshot_date FROM macro_snapshots WHERE deployed=1 ORDER BY snapshot_date DESC LIMIT 1"
        )
        row = cur.fetchone()
        return row[0] if row else None

    def get_macro_history(self, days: int = 90) -> list[dict]:
        """Returns macro snapshots for analysis / ML training."""
        cutoff = (datetime.utcnow() - timedelta(days=days)).strftime("%Y-%m-%d")
        cur = self.conn.execute(
            """SELECT snapshot_date, vix, us_10y_yield, gold, oil_wti, aud_usd, dxy,
                      vix_5d_chg, gold_5d_chg, oil_5d_chg, aud_usd_5d_chg, dxy_5d_chg,
                      is_dip, dip_confidence, dip_triggers, deployed
               FROM macro_snapshots
               WHERE snapshot_date >= ?
               ORDER BY snapshot_date""",
            (cutoff,),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    # ── feature decisions ─────────────────────────────────────────────────

    def log_feature_decision(
        self,
        feature: str,
        symbol: str,
        strategy: str,
        decision: str,
        rule: str,
        context_json: str,
        session_id: str,
    ) -> int:
        """Log a feature decision (accept/reject/deploy/hold/modify)."""
        cur = self.conn.execute(
            """INSERT INTO feature_decisions
               (timestamp, session_id, feature, symbol, strategy,
                decision, rule, context_json)
               VALUES (?,?,?,?,?,?,?,?)""",
            (datetime.utcnow().isoformat(), session_id, feature, symbol,
             strategy, decision, rule, context_json),
        )
        self.conn.commit()
        return cur.lastrowid

    def update_decision_outcome(
        self, decision_id: int, outcome: str, outcome_pnl: float = None,
    ) -> None:
        """Update a feature decision with the actual outcome."""
        self.conn.execute(
            "UPDATE feature_decisions SET outcome=?, outcome_pnl=? WHERE id=?",
            (outcome, outcome_pnl, decision_id),
        )
        self.conn.commit()

    def update_counterfactual(
        self, decision_id: int, counterfactual: str, counterfactual_pnl: float = None,
    ) -> None:
        """Update a rejected decision with what would have happened."""
        self.conn.execute(
            """UPDATE feature_decisions
               SET counterfactual=?, counterfactual_pnl=?, backfilled=1
               WHERE id=?""",
            (counterfactual, counterfactual_pnl, decision_id),
        )
        self.conn.commit()

    def get_feature_decisions(
        self, feature: str = None, lookback_days: int = 90,
    ) -> list[dict]:
        """Query feature decisions, optionally filtered by feature name."""
        cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()
        if feature:
            cur = self.conn.execute(
                """SELECT id, timestamp, session_id, feature, symbol, strategy,
                          decision, rule, context_json, outcome, outcome_pnl,
                          counterfactual, counterfactual_pnl, backfilled
                   FROM feature_decisions
                   WHERE feature=? AND timestamp >= ?
                   ORDER BY timestamp DESC""",
                (feature, cutoff),
            )
        else:
            cur = self.conn.execute(
                """SELECT id, timestamp, session_id, feature, symbol, strategy,
                          decision, rule, context_json, outcome, outcome_pnl,
                          counterfactual, counterfactual_pnl, backfilled
                   FROM feature_decisions
                   WHERE timestamp >= ?
                   ORDER BY timestamp DESC""",
                (cutoff,),
            )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def get_feature_value_scores(self, lookback_days: int = 90) -> dict[str, dict]:
        """
        Compute value scores per feature over the lookback period.
        Returns {feature: {total, correct, score, pending}}.

        A decision is 'correct' if:
          - accept + outcome=profit
          - reject + counterfactual=would_loss
          - deploy + outcome=profit
          - hold + counterfactual=would_loss
        """
        cutoff = (datetime.utcnow() - timedelta(days=lookback_days)).isoformat()
        cur = self.conn.execute(
            """SELECT feature, decision, outcome, counterfactual
               FROM feature_decisions
               WHERE timestamp >= ?""",
            (cutoff,),
        )

        stats: dict[str, dict] = {}
        for feature, decision, outcome, counterfactual in cur.fetchall():
            if feature not in stats:
                stats[feature] = {"total": 0, "correct": 0, "pending": 0}

            s = stats[feature]
            s["total"] += 1

            if decision in ("accept", "deploy"):
                if outcome == "profit":
                    s["correct"] += 1
                elif outcome is None:
                    s["pending"] += 1
            elif decision in ("reject", "hold"):
                if counterfactual == "would_loss":
                    s["correct"] += 1
                elif counterfactual is None and outcome is None:
                    s["pending"] += 1

        # Compute score percentage
        for feature, s in stats.items():
            evaluated = s["total"] - s["pending"]
            s["score"] = round((s["correct"] / evaluated) * 100, 1) if evaluated > 0 else None

        return stats

    def get_unbackfilled_rejects(self, max_age_days: int = 7) -> list[dict]:
        """Get rejected decisions that need counterfactual backfill."""
        cutoff = (datetime.utcnow() - timedelta(days=max_age_days)).isoformat()
        min_age = (datetime.utcnow() - timedelta(hours=6)).isoformat()
        cur = self.conn.execute(
            """SELECT id, timestamp, feature, symbol, strategy, decision,
                      rule, context_json
               FROM feature_decisions
               WHERE decision IN ('reject', 'hold')
                 AND backfilled = 0
                 AND timestamp >= ?
                 AND timestamp <= ?
               ORDER BY timestamp""",
            (cutoff, min_age),
        )
        cols = [desc[0] for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self):
        self.conn.close()
