"""
Position Sizer
--------------
Calculates lot size to risk exactly 1% (or configured %) of account balance
given the stop loss distance in pips.

Pip values (approximate, per standard lot = 100,000 units):
  GBP/JPY: ~1,000 JPY per pip ≈ USD value depends on USD/JPY rate
  AUD/JPY: ~1,000 JPY per pip

For simplicity, we calculate in the quote currency (JPY for JPY pairs)
and express lot size in standard lots.

Formula:
  risk_amount  = account_balance × risk_pct
  pip_value    = lot_size × pip_size × contract_size   (in quote currency)
  lot_size     = risk_amount / (sl_pips × pip_value_per_lot)

For JPY pairs: pip_value_per_lot = 1,000 JPY (1 pip × 100,000 units × 0.01)
  lot_size = risk_jpy / (sl_pips × 1000)
  where risk_jpy = risk_usd × usd_jpy_rate (approximated or passed in)
"""

# Contract size for a standard lot
CONTRACT_SIZE = 100_000

# Pip sizes
PIP_SIZE = {
    "GBPJPY": 0.01,
    "AUDJPY": 0.01,
}


def pip_value_per_lot(pair: str, quote_per_usd: float = 1.0) -> float:
    """
    Returns the value of 1 pip per standard lot in USD.

    For JPY-quoted pairs (e.g. GBP/JPY):
      1 pip = 0.01 JPY × 100,000 = 1,000 JPY
      USD value = 1,000 / USD_JPY_rate

    Args:
        pair: e.g. 'GBPJPY'
        quote_per_usd: Exchange rate of quote currency per 1 USD
                       e.g. if USD/JPY = 150, pass 150.0

    Returns:
        Pip value in USD per standard lot
    """
    pip = PIP_SIZE.get(pair.upper())
    if pip is None:
        raise ValueError(f"Unknown pair: {pair}")

    pip_val_in_quote = pip * CONTRACT_SIZE       # e.g. 0.01 × 100,000 = 1,000 JPY
    pip_val_in_usd = pip_val_in_quote / quote_per_usd
    return pip_val_in_usd


def calculate_lot_size(
    pair: str,
    account_balance: float,
    risk_pct: float,
    sl_pips: float,
    quote_per_usd: float = 150.0,
    min_lot: float = 0.01,
    max_lot: float = 10.0,
    lot_step: float = 0.01,
) -> float:
    """
    Calculate the lot size to risk exactly `risk_pct` of account balance.

    Args:
        pair: e.g. 'GBPJPY'
        account_balance: Account balance in USD
        risk_pct: Fraction to risk e.g. 0.01 for 1%
        sl_pips: Stop loss distance in pips
        quote_per_usd: Quote currency / USD rate (e.g. USD/JPY rate)
        min_lot: Minimum lot size allowed by broker
        max_lot: Maximum lot size cap
        lot_step: Lot size increment (IB typically 0.01)

    Returns:
        Lot size rounded to nearest lot_step, clamped to [min_lot, max_lot]
    """
    if sl_pips <= 0:
        raise ValueError("SL pips must be positive.")

    risk_amount = account_balance * risk_pct
    pv = pip_value_per_lot(pair, quote_per_usd)

    import math
    raw_lots = risk_amount / (sl_pips * pv)

    # Round down to nearest lot_step (math.floor avoids IEEE 754 float precision bugs)
    lots = math.floor(raw_lots / lot_step) * lot_step
    lots = max(min_lot, min(lots, max_lot))

    return round(lots, 2)


def estimate_commission(lot_size: float, commission_per_lot: float = 2.0) -> float:
    """
    Estimate round-trip IBKR forex commission in USD.

    Args:
        lot_size: Position size in standard lots
        commission_per_lot: USD per lot per side (IBKR ~$2/lot for < $25K trades)

    Returns:
        Estimated total round-trip commission in USD (entry + exit)
    """
    return lot_size * commission_per_lot * 2


def check_commission_viability(
    estimated_commission: float,
    risk_amount: float,
    max_commission_pct: float = 0.10,
) -> tuple[bool, float]:
    """
    Check if commission makes the trade unviable for small accounts.

    Args:
        estimated_commission: Expected round-trip commission in USD
        risk_amount: Dollar amount at risk (account_balance * risk_pct)
        max_commission_pct: Skip trade if commission exceeds this fraction of risk

    Returns:
        (is_viable, commission_as_fraction_of_risk)
    """
    if risk_amount <= 0:
        return False, 1.0
    commission_pct = estimated_commission / risk_amount
    return commission_pct <= max_commission_pct, round(commission_pct, 4)
