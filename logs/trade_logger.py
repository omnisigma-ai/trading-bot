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
]


class TradeLogger:
    def __init__(self, db_path: str = "logs/trades.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._create_tables()
        self._migrate()

    def _create_tables(self):
        """Create all tables if they don't exist."""
        for ddl in [CREATE_TRADES, CREATE_SIGNALS, CREATE_EXECUTION_EVENTS,
                     CREATE_ACCOUNT_SNAPSHOTS, CREATE_EXPENSES]:
            self.conn.execute(ddl)
        self.conn.commit()

    def _migrate(self):
        """Add new columns to trades table if missing (safe for existing data)."""
        existing = {row[1] for row in self.conn.execute("PRAGMA table_info(trades)").fetchall()}
        for col_name, col_type in TRADES_MIGRATIONS:
            if col_name not in existing:
                self.conn.execute(f"ALTER TABLE trades ADD COLUMN {col_name} {col_type}")
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

    def close(self):
        self.conn.close()
