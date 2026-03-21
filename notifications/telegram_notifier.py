"""
Telegram Notifier
-----------------
Sends trade alerts to Telegram via Bot API.
Requires TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in settings.yaml.
Uses Markdown parse mode for formatting.
"""
import os
import requests
from datetime import datetime

# Only this chat ID is allowed to receive messages
AUTHORIZED_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")


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
        lines.append("  No open positions")
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


def notify_health_check(
    bot_token: str, chat_id: str,
    account_balance: float,
    unrealised_pnl: float,
    positions: list[dict],
    daily_pnl: float,
    weekly_pnl: float,
    all_time: dict,
    top_wins: list[dict],
    top_losses: list[dict],
    open_trade_count: int,
    pending_order_count: int = 0,
    currency: str = "USD",
) -> None:
    """15-minute health check with full portfolio snapshot."""
    now_str = datetime.utcnow().strftime("%H:%M UTC")
    sym = "A$" if currency == "AUD" else "$"

    # Header
    lines = [f"\U0001f3e5 *HEALTH CHECK* \u2014 {now_str}"]
    lines.append("")

    # Account
    total_equity = account_balance + unrealised_pnl
    lines.append("*Account*")
    lines.append(f"  Balance:     `{sym}{account_balance:,.2f}`")
    lines.append(f"  Unrealised:  `{sym}{unrealised_pnl:+,.2f}`")
    lines.append(f"  Equity:      `{sym}{total_equity:,.2f}`")
    lines.append("")

    # P&L Summary (realised P&L tracked in USD)
    lines.append("*Realised P&L (USD)*")
    lines.append(f"  Today:    `${daily_pnl:+,.2f}`")
    lines.append(f"  Week:     `${weekly_pnl:+,.2f}`")
    at_pnl = all_time.get("pnl_usd", 0)
    at_trades = all_time.get("total_trades", 0)
    at_wr = all_time.get("win_rate", 0)
    at_wins = all_time.get("wins", 0)
    at_losses = all_time.get("losses", 0)
    lines.append(f"  All-time: `${at_pnl:+,.2f}` ({at_trades} trades, {at_wr:.0f}% WR, {at_wins}W/{at_losses}L)")
    lines.append("")

    # Filled Positions
    filled_count = len(positions)
    lines.append(f"*Filled Positions ({filled_count})*")
    if not positions:
        lines.append("  None")
    else:
        for p in positions:
            pair = p.get("pair", "?")
            side = p.get("side", "?")
            qty = p.get("qty", 0)
            pnl = p.get("unrealised_pnl", 0)
            icon = "\U0001f7e2" if pnl >= 0 else "\U0001f534"
            lines.append(f"  {icon} {pair} `{side}` x`{qty}` | `${pnl:+,.2f}`")

    # Pending orders
    if pending_order_count > 0:
        lines.append(f"  \u23f3 {pending_order_count} pending orders awaiting trigger")

    # Top Wins
    if top_wins:
        lines.append("")
        lines.append("*Top 5 Wins*")
        for i, t in enumerate(top_wins[:5], 1):
            sym_t = t.get("pair", "?")
            pnl = t.get("pnl_usd", 0)
            pips = t.get("pips", 0)
            strat = t.get("strategy") or ""
            strat_str = f" [{strat}]" if strat else ""
            lines.append(f"  {i}. {sym_t} `+${pnl:,.2f}` (+{pips:.0f}p){strat_str}")

    # Top Losses
    if top_losses:
        lines.append("")
        lines.append("*Top 5 Losses*")
        for i, t in enumerate(top_losses[:5], 1):
            sym_t = t.get("pair", "?")
            pnl = t.get("pnl_usd", 0)
            pips = t.get("pips", 0)
            strat = t.get("strategy") or ""
            strat_str = f" [{strat}]" if strat else ""
            lines.append(f"  {i}. {sym_t} `-${abs(pnl):,.2f}` ({pips:.0f}p){strat_str}")

    # No trades yet
    if not top_wins and not top_losses:
        lines.append("")
        lines.append("_No completed trades yet_")

    _send(bot_token, chat_id, "\n".join(lines))


def notify_bot_shutdown(bot_token: str, chat_id: str, reason: str = "") -> None:
    msg = "\U0001f6d1 *BOT STOPPED*"
    if reason:
        msg += f" — {reason}"
    _send(bot_token, chat_id, msg)


def notify_stock_order(bot_token: str, chat_id: str, symbol: str,
                       direction: str, shares: int, entry: float,
                       sl: float, tp: float, risk_usd: float,
                       strategy: str = "momentum_stocks") -> None:
    arrow = "\U0001f7e2" if direction == "BUY" else "\U0001f534"
    msg = (
        f"{arrow} *STOCK ORDER* — {symbol} [{strategy}]\n"
        f"Direction: `{direction}` | Shares: `{shares}`\n"
        f"Entry:  `${entry:.2f}`\n"
        f"SL:     `${sl:.2f}` | TP: `${tp:.2f}`\n"
        f"Risk:   `${risk_usd:.2f}`"
    )
    _send(bot_token, chat_id, msg)


def notify_futures_order(bot_token: str, chat_id: str, symbol: str,
                         direction: str, contracts: int, entry: float,
                         sl: float, tp: float, risk_usd: float,
                         strategy: str = "futures_breakout") -> None:
    arrow = "\U0001f7e2" if direction == "BUY" else "\U0001f534"
    msg = (
        f"{arrow} *FUTURES ORDER* \u2014 {symbol} [{strategy}]\n"
        f"Direction: `{direction}` | Contracts: `{contracts}`\n"
        f"Entry:  `{entry:.2f}`\n"
        f"SL:     `{sl:.2f}` | TP: `{tp:.2f}`\n"
        f"Risk:   `${risk_usd:.2f}`"
    )
    _send(bot_token, chat_id, msg)


def notify_score_report(bot_token: str, chat_id: str,
                        scores: list) -> None:
    """Send opportunity scoring summary."""
    if not scores:
        return
    lines = ["\U0001f3af *OPPORTUNITY SCORES*"]
    for s in scores:
        icon = "\u2705" if s.accepted else "\u274c"
        lines.append(
            f"  {icon} {s.symbol} {s.direction} | "
            f"R:R `{s.risk_reward_ratio:.1f}` | "
            f"Win `{s.win_probability*100:.0f}%` | "
            f"EV `{s.expected_value:+.2f}` | "
            f"Score `{s.asymmetry_score:.2f}`"
        )
    _send(bot_token, chat_id, "\n".join(lines))


def notify_portfolio_summary(bot_token: str, chat_id: str,
                             trading_balance: float,
                             etf_value: float,
                             pending_realloc: float,
                             etf_holdings: list[dict] = None) -> None:
    """Weekly wealth summary."""
    total = trading_balance + etf_value
    lines = ["\U0001f4b0 *PORTFOLIO SUMMARY*"]
    lines.append(f"  Trading balance: `${trading_balance:,.2f}`")
    lines.append(f"  ETF portfolio:   `${etf_value:,.2f}`")
    lines.append(f"  *Total wealth:   ${total:,.2f}*")
    if pending_realloc > 0:
        lines.append(f"  Pending realloc: `${pending_realloc:.2f}`")
    if etf_holdings:
        lines.append("\n  *ETF Holdings:*")
        for h in etf_holdings:
            lines.append(
                f"    {h['symbol']}: {h['total_shares']:.0f} shares "
                f"(${h['total_invested_usd']:,.2f})"
            )
    _send(bot_token, chat_id, "\n".join(lines))


def notify_exit_action(bot_token: str, chat_id: str,
                       symbol: str, action: str, details: str) -> None:
    """Notify about exit strategy actions (trailing stop moves, partial exits)."""
    icons = {
        "modify_stop": "\U0001f504",     # arrows
        "partial_close": "\u2702\ufe0f", # scissors
        "breakeven": "\U0001f6e1\ufe0f", # shield
    }
    icon = icons.get(action, "\u2699\ufe0f")
    msg = f"{icon} *EXIT ACTION* — {symbol}\n{details}"
    _send(bot_token, chat_id, msg)


def notify_value_stock_selection(bot_token: str, chat_id: str,
                                 selected, runners_up: list,
                                 shares: int, price_aud: float,
                                 cost_aud: float) -> None:
    """Notify about EV-scored value stock purchase with selection rationale."""
    s = selected
    lines = [
        f"\U0001f4a1 *VALUE STOCK PURCHASED* \u2014 {shares} x {s.symbol} @ A${price_aud:.2f}",
        f"Cost: `A${cost_aud:.2f}` | Moat: `{s.moat_rating}`",
        "",
        f"*EV Score: {s.composite_score:.1f}/100*",
        f"  Valuation: `{s.valuation_score:.0f}` | Quality: `{s.quality_score:.0f}` | Safety: `{s.safety_score:.0f}`",
    ]

    # Snowflake score (SWS-style 30-check analysis)
    if s.snowflake_attempted > 0:
        lines.append(
            f"  *Snowflake: {s.snowflake_total}/{s.snowflake_attempted}* "
            f"(V:{s.snowflake_value} F:{s.snowflake_future} P:{s.snowflake_past} "
            f"H:{s.snowflake_health} D:{s.snowflake_dividends})"
        )
    if s.dcf_margin_of_safety is not None:
        mos_str = f"+{s.dcf_margin_of_safety:.0%}" if s.dcf_margin_of_safety >= 0 else f"{s.dcf_margin_of_safety:.0%}"
        lines.append(f"  DCF margin of safety: `{mos_str}`")

    metrics = []
    if s.ev_to_ebitda is not None:
        metrics.append(f"EV/EBITDA `{s.ev_to_ebitda:.1f}`")
    if s.trailing_pe is not None:
        metrics.append(f"P/E `{s.trailing_pe:.1f}`")
    if s.roe is not None:
        metrics.append(f"ROE `{s.roe * 100:.1f}%`")
    if s.debt_to_equity is not None:
        metrics.append(f"D/E `{s.debt_to_equity:.0f}`")
    if s.dividend_yield is not None:
        metrics.append(f"Div `{s.dividend_yield * 100:.1f}%`")
    if metrics:
        lines.append("  " + " | ".join(metrics))

    if runners_up:
        lines.append("")
        lines.append("*Runners-up:*")
        for r in runners_up[:3]:
            lines.append(
                f"  #{r.rank} {r.symbol} \u2014 Score `{r.composite_score:.1f}` "
                f"({r.moat_rating})"
            )

    _send(bot_token, chat_id, "\n".join(lines))


def notify_dip_detected(bot_token: str, chat_id: str,
                        signal, deploying: bool,
                        pending_aud: float = 0) -> None:
    """Notify about macro dip detection and deployment decision."""
    if signal.is_dip:
        icon = "\U0001f4c9"  # chart down
        status = "DEPLOYING" if deploying else "DIP DETECTED (insufficient funds)"
    else:
        if deploying:
            icon = "\u23f0"  # alarm clock
            status = "MAX WAIT REACHED \u2014 deploying"
        else:
            icon = "\U0001f4ca"  # chart
            status = "NO DIP \u2014 holding"

    lines = [f"{icon} *MACRO CHECK* \u2014 {status}"]

    if signal.triggers:
        lines.append("*Triggers:*")
        for t in signal.triggers:
            lines.append(f"  \u26a0\ufe0f {t}")

    snap = signal.macro_snapshot
    lines.append("")
    readings = []
    if snap.get("vix") is not None:
        readings.append(f"VIX `{snap['vix']:.1f}`")
    if snap.get("gold") is not None:
        readings.append(f"Gold `${snap['gold']:,.0f}`")
    if snap.get("oil_wti") is not None:
        readings.append(f"Oil `${snap['oil_wti']:.1f}`")
    if snap.get("aud_usd") is not None:
        readings.append(f"AUD `{snap['aud_usd']:.4f}`")
    if readings:
        lines.append(" | ".join(readings))

    lines.append(f"Confidence: `{signal.confidence:.0%}` | Pending: `A${pending_aud:,.2f}`")

    _send(bot_token, chat_id, "\n".join(lines))


def notify_feature_health(bot_token: str, chat_id: str,
                          scores: list, diagnostics: list) -> None:
    """Weekly feature health report with value scores and diagnostics."""
    if not scores:
        return

    week_str = datetime.utcnow().strftime("%Y-%m-%d")
    lines = [f"\U0001f4ca *FEATURE HEALTH REPORT* \u2014 {week_str}"]

    for s in scores:
        if s.score is None:
            icon = "\u2754"  # question mark
            score_str = "pending"
        elif s.score >= 70:
            icon = "\u2705"  # green check
            score_str = f"{s.score:.0f}%"
        elif s.score >= 50:
            icon = "\U0001f7e1"  # yellow circle
            score_str = f"{s.score:.0f}%"
        else:
            icon = "\u26a0\ufe0f"  # warning
            score_str = f"{s.score:.0f}%"

        lines.append(
            f"  {icon} `{s.feature}`: {score_str} "
            f"({s.correct}/{s.evaluated} correct)"
        )

    if diagnostics:
        lines.append("")
        lines.append("*Underperformers:*")
        for d in diagnostics:
            lines.append(f"  \u26a0\ufe0f *{d.feature}* ({d.value_score:.0f}%)")
            if d.failure_clusters:
                top = d.failure_clusters[0]
                lines.append(
                    f"    Top failure: `{top['rule']}` "
                    f"({top['count']}x, {top['pct']:.0f}%)"
                )
            for fix in d.suggested_fixes[:2]:
                lines.append(f"    \U0001f527 {fix}")

    _send(bot_token, chat_id, "\n".join(lines))
