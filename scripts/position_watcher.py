"""
Position Watcher
----------------
Lightweight script that monitors open positions and sends Telegram
alerts on P&L changes (every 5 pips) and when TP/SL fills.

Run alongside main.py or standalone when positions are open.
"""
import time
from datetime import datetime

from ib_insync import IB, Forex

from config.loader import load_config
from notifications import telegram_notifier as notify
from strategy.london_breakout import PIP_SIZE
from risk.position_sizer import pip_value_per_lot


def watch_positions():
    config = load_config()
    tg = config.get("telegram", {})
    bot_token = tg.get("bot_token", "")
    chat_id = str(tg.get("chat_id", ""))

    ib = IB()
    ib_cfg = config["ib"]
    port = ib_cfg["paper_port"] if config["mode"] == "paper" else ib_cfg["live_port"]

    try:
        ib.connect(ib_cfg["host"], port, clientId=50)
    except Exception as e:
        print(f"[Watcher] Could not connect to IB: {e}")
        return

    notify._send(bot_token, chat_id, "\U0001f440 *POSITION WATCHER* started — monitoring P&L")
    print("[Watcher] Connected. Monitoring positions...")

    # Track last alert level per position
    last_alert: dict[str, float] = {}  # symbol → last_pips_alerted
    alert_threshold = 5.0  # pips
    update_interval = 30  # seconds between checks

    try:
        while True:
            positions = ib.positions()
            open_pos = [p for p in positions if p.position != 0]

            if not open_pos:
                # Check if we had positions before (= they just closed)
                if last_alert:
                    notify._send(bot_token, chat_id,
                                 "\u2705 *ALL POSITIONS CLOSED* — no open trades")
                    print("[Watcher] All positions closed.")
                    last_alert.clear()
                ib.sleep(update_interval)
                continue

            for pos in open_pos:
                sym = pos.contract.localSymbol  # e.g. "AUD.USD"
                pair = sym.replace(".", "")       # e.g. "AUDUSD"
                qty = pos.position
                entry = pos.avgCost
                side = "SHORT" if qty < 0 else "LONG"

                # Get current price
                contract = Forex(pair=pair)
                try:
                    ib.qualifyContracts(contract)
                    ticker = ib.reqMktData(contract)
                    ib.sleep(2)
                    mid = ticker.midpoint()
                    ib.cancelMktData(contract)
                except Exception:
                    continue

                if not mid or mid != mid:  # NaN check
                    continue

                pip = PIP_SIZE.get(pair.upper(), 0.0001)
                if side == "SHORT":
                    pips = (entry - mid) / pip
                else:
                    pips = (mid - entry) / pip

                quote_rate = 1.0 if pair.upper().endswith("USD") else 150.0
                pv = pip_value_per_lot(pair, quote_rate)
                lots = abs(qty) / 100_000
                pnl_usd = pips * pv * lots

                # Check if we should alert
                prev = last_alert.get(sym, 0.0)
                if abs(pips - prev) >= alert_threshold:
                    sign = "+" if pips >= 0 else ""
                    arrow = "\U0001f7e2" if pips >= 0 else "\U0001f534"
                    msg = (
                        f"{arrow} *P&L UPDATE* — {pair} `{side}`\n"
                        f"Entry: `{entry:.5f}` | Now: `{mid:.5f}`\n"
                        f"{sign}{pips:.1f} pips | {sign}${pnl_usd:.2f}"
                    )
                    notify._send(bot_token, chat_id, msg)
                    last_alert[sym] = pips
                    print(f"[Watcher] {pair} {side}: {sign}{pips:.1f} pips ({sign}${pnl_usd:.2f})")

            ib.sleep(update_interval)

    except KeyboardInterrupt:
        print("\n[Watcher] Stopped.")
    except Exception as e:
        notify._send(bot_token, chat_id, f"\u26a0\ufe0f *WATCHER ERROR* — {e}")
        print(f"[Watcher] Error: {e}")
    finally:
        notify._send(bot_token, chat_id, "\U0001f6d1 *POSITION WATCHER* stopped")
        ib.disconnect()


if __name__ == "__main__":
    watch_positions()
