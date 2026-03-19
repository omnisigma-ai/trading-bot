"""
Portfolio-Level Risk Management
-------------------------------
Cross-strategy risk checks to prevent overexposure.
Called before executing any new trade intent.
"""
from logs.trade_logger import TradeLogger


class PortfolioRiskError(Exception):
    pass


def check_portfolio_risk(
    logger: TradeLogger,
    account_balance: float,
    new_risk_usd: float,
    instrument_type: str,
    config: dict,
    open_positions: list[dict] = None,
) -> None:
    """
    Check portfolio-level risk before accepting a new trade.
    Raises PortfolioRiskError if any limit is breached.

    Args:
        logger: TradeLogger for DB queries
        account_balance: Current account balance
        new_risk_usd: Dollar risk of the proposed trade
        instrument_type: "forex", "stock", or "etf"
        config: Portfolio limits config section
        open_positions: List of current open positions
    """
    limits = config.get("portfolio_limits", {})
    if not limits:
        return  # no limits configured

    if open_positions is None:
        open_positions = []

    # Max total risk across all strategies
    max_total_risk_pct = limits.get("max_total_risk_pct", 0.05)
    existing_risk = _estimate_open_risk(logger)
    total_risk = existing_risk + new_risk_usd
    max_risk_usd = account_balance * max_total_risk_pct

    if total_risk > max_risk_usd:
        raise PortfolioRiskError(
            f"Total risk ${total_risk:.2f} would exceed "
            f"{max_total_risk_pct*100:.0f}% limit (${max_risk_usd:.2f})"
        )

    # Max positions per instrument type
    max_forex = limits.get("max_forex_positions", 2)
    max_stock = limits.get("max_stock_positions", 3)

    forex_count = sum(1 for p in open_positions if _is_forex_position(p))
    stock_count = sum(1 for p in open_positions if _is_stock_position(p))

    if instrument_type == "forex" and forex_count >= max_forex:
        raise PortfolioRiskError(
            f"Max forex positions reached ({forex_count}/{max_forex})"
        )

    if instrument_type == "stock" and stock_count >= max_stock:
        raise PortfolioRiskError(
            f"Max stock positions reached ({stock_count}/{max_stock})"
        )


def _estimate_open_risk(logger: TradeLogger) -> float:
    """Estimate total USD at risk from open trades."""
    # Sum risk_usd from trades that are open (no result yet)
    try:
        cur = logger.conn.execute(
            """SELECT COALESCE(SUM(lot_size * sl_pips * 10), 0)
               FROM trades
               WHERE result IS NULL AND lot_size IS NOT NULL AND sl_pips IS NOT NULL"""
        )
        return float(cur.fetchone()[0])
    except Exception:
        return 0.0


def _is_forex_position(pos: dict) -> bool:
    """Check if a position dict represents a forex position."""
    sym = pos.get("symbol", "")
    # Forex positions in IB have 3-letter base currency symbols
    return len(sym) == 3 and sym.isalpha()


def _is_stock_position(pos: dict) -> bool:
    """Check if a position dict represents a stock position."""
    sym = pos.get("symbol", "")
    return len(sym) > 3 or not sym.isalpha()
