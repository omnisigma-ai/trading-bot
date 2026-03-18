"""
Trade Logger
------------
SQLite journal for all trades. One row per trade.
"""
import sqlite3
from datetime import datetime
from pathlib import Path


CREATE_TABLE = """
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
    result          TEXT,                  -- TP | SL | TIME_EXIT | CANCELLED
    pips            REAL,
    pnl_usd         REAL,
    opened_at       TEXT,
    closed_at       TEXT,
    ib_order_id     INTEGER,
    notes           TEXT
)
"""


class TradeLogger:
    def __init__(self, db_path: str = "logs/trades.db"):
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute(CREATE_TABLE)
        self.conn.commit()

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

    def log_trade_closed(
        self,
        trade_id: int,
        exit_price: float,
        result: str,
        pips: float,
        pnl_usd: float,
    ) -> None:
        self.conn.execute(
            """UPDATE trades
               SET exit_price=?, result=?, pips=?, pnl_usd=?, closed_at=?
               WHERE id=?""",
            (exit_price, result, pips, pnl_usd,
             datetime.utcnow().isoformat(), trade_id),
        )
        self.conn.commit()

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
        from datetime import timedelta
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

    def close(self):
        self.conn.close()
