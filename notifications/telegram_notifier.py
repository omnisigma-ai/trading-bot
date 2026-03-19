"""
Telegram Notifier
-----------------
Sends trade alerts to Telegram via Bot API.
Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in settings.yaml.
Uses Markdown parse mode for formatting.
"""
import requests
from datetime import datetime

# Only this chat ID is allowed to receive messages
AUTHORIZED_CHAT_ID = "7169122227"


def _send(bot_token: str, chat_id: str, text: str) -> None:
    if not bot_token or not chat_id:
        print(f"[Telegram] (not configured) {text}")
        return

    # Only allow sending to the authorized chat ID
    if str(chat_id) != AUTHORIZED_CHAT_ID:
        print(f"[Telegram] Blocked — chat_id {chat_id} is not authorized")
        return

    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

    # Telegram max is 4096 chars — truncate if needed
    if len(text) > 4000:
        text = text[:4000] + "\n... (truncated)"

    try:
        resp = requests.post(url, json={
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=10)
        if resp.status_code != 200:
            print(f"[Telegram] Failed to send: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[Telegram] Error sending alert: {e}")


def notify_order_placed(bot_token: str, chat_id: str, pair: str, direction: str,
                        entry: float, sl: float, tp: float,
                        sl_pips: float, tp_pips: float,
                        lot_size: float, risk_usd: float) -> None:
    arrow = "\U0001f7e2" if direction == "BUY" else "\U0001f534"
    msg = (
        f"{arrow} *ORDER PLACED* — {pair}\n"
        f"Direction: `{direction} STOP`\n"
        f"Entry:  `{entry}`\n"
        f"SL:     `{sl}` (-{sl_pips:.1f} pips)\n"
        f"TP:     `{tp}` (+{tp_pips:.1f} pips)\n"
        f"Size:   `{lot_size} lots` | Risk: `${risk_usd:.2f} (1%)`"
    )
    _send(bot_token, chat_id, msg)


def notify_order_filled(bot_token: str, chat_id: str, pair: str, direction: str,
                        fill_price: float) -> None:
    msg = f"\U0001f535 *ORDER FILLED* — {pair} `{direction}` @ `{fill_price}`"
    _send(bot_token, chat_id, msg)


def notify_tp_hit(bot_token: str, chat_id: str, pair: str, direction: str,
                  pips: float, pnl_usd: float) -> None:
    msg = (
        f"\u26a1 *TP HIT* — {pair} `{direction}`\n"
        f"+{pips:.1f} pips | *+${pnl_usd:.2f}*"
    )
    _send(bot_token, chat_id, msg)


def notify_sl_hit(bot_token: str, chat_id: str, pair: str, direction: str,
                  pips: float, pnl_usd: float) -> None:
    msg = (
        f"\U0001f534 *SL HIT* — {pair} `{direction}`\n"
        f"-{pips:.1f} pips | *-${abs(pnl_usd):.2f}*"
    )
    _send(bot_token, chat_id, msg)


def notify_no_signal(bot_token: str, chat_id: str, pair: str,
                     reason: str = "range too tight") -> None:
    msg = f"\u26aa *NO SIGNAL* — {pair} ({reason})"
    _send(bot_token, chat_id, msg)


def notify_error(bot_token: str, chat_id: str, message: str,
                 fatal: bool = False) -> None:
    prefix = "\U0001f6a8 *FATAL ERROR*" if fatal else "\u26a0\ufe0f *ERROR*"
    _send(bot_token, chat_id, f"{prefix} — {message}")


def notify_daily_summary(bot_token: str, chat_id: str,
                         results: list[dict]) -> None:
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    lines = [f"\U0001f4ca *DAILY SUMMARY* — {date_str}"]

    total_pnl = 0.0
    for r in results:
        pair = r.get("pair", "?")
        result = r.get("result", "?")
        pips = r.get("pips", 0.0)
        pnl = r.get("pnl_usd", 0.0)
        total_pnl += pnl

        if result == "TP":
            lines.append(f"  \u26a1 {pair}: TP hit +{pips:.1f} pips | +${pnl:.2f}")
        elif result == "SL":
            lines.append(f"  \U0001f534 {pair}: SL hit -{abs(pips):.1f} pips | -${abs(pnl):.2f}")
        elif result == "NO_SIGNAL":
            lines.append(f"  \u26aa {pair}: No signal")
        else:
            lines.append(f"  \U0001f552 {pair}: {result}")

    sign = "+" if total_pnl >= 0 else ""
    lines.append(f"\n*Net P&L: {sign}${total_pnl:.2f}*")

    _send(bot_token, chat_id, "\n".join(lines))


def notify_position_update(bot_token: str, chat_id: str,
                           positions: list[dict], unrealised_pnl: float,
                           account_balance: float) -> None:
    lines = ["\U0001f4c8 *POSITION UPDATE*"]
    if not positions:
        lines.append("  No open positions — waiting for breakout")
    else:
        for p in positions:
            pair = p.get("pair", "?")
            side = p.get("side", "?")
            entry = p.get("entry", 0)
            current = p.get("current", 0)
            pnl = p.get("pnl", 0)
            sign = "+" if pnl >= 0 else ""
            lines.append(f"  {pair} `{side}` @ `{entry}` | now `{current}` | {sign}${pnl:.2f}")
    lines.append(f"\nBalance: `${account_balance:,.2f}` | Unrealised: `${unrealised_pnl:,.2f}`")
    _send(bot_token, chat_id, "\n".join(lines))


def notify_bot_shutdown(bot_token: str, chat_id: str, reason: str = "") -> None:
    msg = "\U0001f6d1 *BOT STOPPED*"
    if reason:
        msg += f" — {reason}"
    _send(bot_token, chat_id, msg)
