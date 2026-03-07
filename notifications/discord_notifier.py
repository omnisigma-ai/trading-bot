"""
Discord Notifier
----------------
Sends trade alerts to a Discord channel via webhook.
No bot token required — just paste a webhook URL in settings.yaml.
"""
import requests
from datetime import datetime


def _post(webhook_url: str, content: str) -> None:
    if not webhook_url or webhook_url == "YOUR_DISCORD_WEBHOOK_URL_HERE":
        print(f"[Discord] (no webhook set) {content}")
        return

    resp = requests.post(webhook_url, json={"content": content}, timeout=10)
    if resp.status_code not in (200, 204):
        print(f"[Discord] Failed to send alert: {resp.status_code} {resp.text}")


def notify_order_placed(webhook_url: str, pair: str, direction: str,
                        entry: float, sl: float, tp: float,
                        sl_pips: float, tp_pips: float,
                        lot_size: float, risk_usd: float) -> None:
    arrow = "🟢" if direction == "BUY" else "🔴"
    msg = (
        f"{arrow} **ORDER PLACED** — {pair}\n"
        f"Direction: `{direction} STOP`\n"
        f"Entry:  `{entry}`\n"
        f"SL:     `{sl}` (-{sl_pips:.1f} pips)\n"
        f"TP:     `{tp}` (+{tp_pips:.1f} pips)\n"
        f"Size:   `{lot_size} lots` | Risk: `${risk_usd:.2f} (1%)`"
    )
    _post(webhook_url, msg)


def notify_order_filled(webhook_url: str, pair: str, direction: str, fill_price: float) -> None:
    msg = f"🔵 **ORDER FILLED** — {pair} `{direction}` @ `{fill_price}`"
    _post(webhook_url, msg)


def notify_tp_hit(webhook_url: str, pair: str, direction: str,
                  pips: float, pnl_usd: float) -> None:
    msg = (
        f"⚡ **TP HIT** — {pair} `{direction}`\n"
        f"+{pips:.1f} pips | **+${pnl_usd:.2f}**"
    )
    _post(webhook_url, msg)


def notify_sl_hit(webhook_url: str, pair: str, direction: str,
                  pips: float, pnl_usd: float) -> None:
    msg = (
        f"🔴 **SL HIT** — {pair} `{direction}`\n"
        f"-{pips:.1f} pips | **-${abs(pnl_usd):.2f}**"
    )
    _post(webhook_url, msg)


def notify_no_signal(webhook_url: str, pair: str, reason: str = "range too tight") -> None:
    msg = f"⚪ **NO SIGNAL** — {pair} ({reason})"
    _post(webhook_url, msg)


def notify_error(webhook_url: str, message: str, fatal: bool = False) -> None:
    prefix = "🚨 **FATAL ERROR**" if fatal else "⚠️ **ERROR**"
    _post(webhook_url, f"{prefix} — {message}")


def notify_daily_summary(webhook_url: str, results: list[dict]) -> None:
    """
    Args:
        results: list of dicts with keys: pair, result, pips, pnl_usd
                 result: 'TP' | 'SL' | 'NO_SIGNAL' | 'OPEN'
    """
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    lines = [f"📊 **DAILY SUMMARY** — {date_str}"]

    total_pnl = 0.0
    for r in results:
        pair = r.get("pair", "?")
        result = r.get("result", "?")
        pips = r.get("pips", 0.0)
        pnl = r.get("pnl_usd", 0.0)
        total_pnl += pnl

        if result == "TP":
            lines.append(f"  ⚡ {pair}: TP hit +{pips:.1f} pips | +${pnl:.2f}")
        elif result == "SL":
            lines.append(f"  🔴 {pair}: SL hit -{abs(pips):.1f} pips | -${abs(pnl):.2f}")
        elif result == "NO_SIGNAL":
            lines.append(f"  ⚪ {pair}: No signal")
        else:
            lines.append(f"  🕐 {pair}: {result}")

    sign = "+" if total_pnl >= 0 else ""
    lines.append(f"\n**Net P&L: {sign}${total_pnl:.2f}**")

    _post(webhook_url, "\n".join(lines))
