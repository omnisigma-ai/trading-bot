"""
Daily & Weekly Loss Limits
--------------------------
Protects the account by stopping the bot from trading when:
  1. Today's losses exceed the daily loss limit (default 2% of account)
  2. This week's losses exceed the weekly loss limit (default 5% of account)
  3. The last N consecutive trades were all SL hits (default 3 in a row)

Called in main.py before placing any orders each session.
Raises LimitBreached if any limit is hit — the caller skips the session.
"""


class LimitBreached(Exception):
    pass


def check_limits(
    logger,
    account_balance: float,
    daily_loss_limit: float = 0.02,
    weekly_loss_limit: float = 0.05,
    max_consecutive_losses: int = 3,
) -> None:
    """
    Check all risk limits before trading. Raises LimitBreached if any is hit.

    Args:
        logger: TradeLogger instance with DB access
        account_balance: Current account balance in USD
        daily_loss_limit: Max daily loss as fraction of account (e.g. 0.02 = 2%)
        weekly_loss_limit: Max weekly loss as fraction of account (e.g. 0.05 = 5%)
        max_consecutive_losses: Pause after this many SL hits in a row
    """
    today_pnl = logger.get_today_pnl()
    weekly_pnl = logger.get_weekly_pnl()
    consec_losses = logger.get_consecutive_losses()

    daily_limit_usd = account_balance * daily_loss_limit
    weekly_limit_usd = account_balance * weekly_loss_limit

    if today_pnl <= -daily_limit_usd:
        raise LimitBreached(
            f"Daily loss limit reached: ${abs(today_pnl):.2f} lost today "
            f"(limit: ${daily_limit_usd:.2f} / {daily_loss_limit*100:.0f}%). "
            f"No trading today."
        )

    if weekly_pnl <= -weekly_limit_usd:
        raise LimitBreached(
            f"Weekly loss limit reached: ${abs(weekly_pnl):.2f} lost this week "
            f"(limit: ${weekly_limit_usd:.2f} / {weekly_loss_limit*100:.0f}%). "
            f"No trading until next week."
        )

    if consec_losses >= max_consecutive_losses:
        raise LimitBreached(
            f"{consec_losses} consecutive losses detected. "
            f"Pausing bot — please review strategy before resuming."
        )
