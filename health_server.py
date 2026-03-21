"""
Health Server for Trading Bot
-----------------------------
HTTP health endpoint on port 8082 for orchestrator monitoring.
Runs in a daemon thread alongside the main APScheduler.

Endpoints:
    GET /health  → JSON status (healthy/degraded/down)
"""
import json
import os
import time
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime

from config.loader import load_config
from logs.trade_logger import TradeLogger


class _HealthState:
    """Shared mutable state updated by the main bot, read by the health server."""

    def __init__(self):
        self.start_time = time.time()
        self.scheduler_running = False
        self.last_strategy_run: float | None = None
        self.last_strategy_error: str | None = None
        self.strategy_run_count = 0
        self.error_count = 0
        self.ib_connected = False
        self.ib_reconnect_count = 0
        self.continuous_monitoring = True

    def record_strategy_run(self):
        self.last_strategy_run = time.time()
        self.strategy_run_count += 1

    def record_error(self, error: str):
        self.last_strategy_error = error
        self.error_count += 1


# Singleton shared with main.py
state = _HealthState()


class _HealthHandler(BaseHTTPRequestHandler):
    """Handles GET /health requests."""

    def _check_auth(self) -> bool:
        """Validate Bearer token for sensitive endpoints (timing-safe)."""
        import hmac
        token = os.getenv("HEALTH_AUTH_TOKEN", "")
        if not token:
            return True
        auth = self.headers.get("Authorization", "")
        return hmac.compare_digest(auth, f"Bearer {token}")

    def do_GET(self):
        if self.path == "/health":
            self._handle_health()
        elif self.path == "/financials":
            if not self._check_auth():
                self.send_error(401, "Unauthorized")
                return
            self._handle_financials()
        else:
            self.send_error(404)

    def _handle_health(self):
        uptime = time.time() - state.start_time
        healthy = state.scheduler_running

        config = load_config()
        db_path = config.get("db_path", "logs/trades.db")

        today_pnl = 0.0
        open_trades = 0
        try:
            tl = TradeLogger(db_path)
            try:
                today_pnl = tl.get_today_pnl()
                daily = tl.get_daily_summary()
                open_trades = sum(1 for t in daily if t.get("result") is None)
            finally:
                tl.close()
        except Exception:
            pass

        body = {
            "service": "trading-bot",
            "status": "healthy" if healthy else "degraded",
            "uptime_hours": round(uptime / 3600, 1),
            "scheduler_running": state.scheduler_running,
            "strategy_runs": state.strategy_run_count,
            "errors_total": state.error_count,
            "last_run_ago_sec": (
                round(time.time() - state.last_strategy_run, 1)
                if state.last_strategy_run
                else None
            ),
            "last_error": state.last_strategy_error,
            "today_pnl_usd": round(today_pnl, 2),
            "open_trades": open_trades,
            "ib_connected": state.ib_connected,
            "ib_reconnects": state.ib_reconnect_count,
            "continuous_monitoring": state.continuous_monitoring,
            "mode": config.get("mode", "unknown"),
            "timestamp": datetime.utcnow().isoformat() + "Z",
        }

        payload = json.dumps(body).encode()
        status_code = 200 if healthy else 503
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _handle_financials(self):
        config = load_config()
        db_path = config.get("db_path", "logs/trades.db")

        try:
            tl = TradeLogger(db_path)
            try:
                now = datetime.utcnow()

                current = tl.get_monthly_pnl(now.year, now.month)

                prev_m = now.month - 1 if now.month > 1 else 12
                prev_y = now.year if now.month > 1 else now.year - 1
                previous = tl.get_monthly_pnl(prev_y, prev_m)

                trailing = []
                y, m = prev_y, prev_m
                for _ in range(3):
                    m -= 1
                    if m < 1:
                        m = 12
                        y -= 1
                    t = tl.get_monthly_pnl(y, m)
                    trailing.append({"period": t["period"], "realised_pnl": t["realised_pnl_usd"]})

                body = {
                    "service": "trading-bot",
                    "currency": "USD",
                    "current_month": {
                        "period": current["period"],
                        "realised_pnl": current["realised_pnl_usd"],
                        "realised_pnl_aud": current["realised_pnl_aud"],
                        "trade_count": current["trade_count"],
                        "win_count": current["win_count"],
                        "loss_count": current["loss_count"],
                        "commissions": current["commissions_usd"],
                        "expenses": current["expenses_usd"],
                    },
                    "previous_month": {
                        "period": previous["period"],
                        "realised_pnl": previous["realised_pnl_usd"],
                        "trade_count": previous["trade_count"],
                    },
                    "trailing_3_months": trailing,
                }
            finally:
                tl.close()
        except Exception as e:
            import logging as _logging
            _logging.getLogger("trading-bot.health").error(f"Financials error: {type(e).__name__}: {e}")
            body = {"error": "Internal server error"}

        payload = json.dumps(body).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format, *args):
        # Suppress default stderr logging to avoid noise
        pass


def start_health_server(port: int = 8082):
    """Start the health HTTP server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", port), _HealthHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"[Health] Server on :{port} (Tailscale access only)")
    return server
