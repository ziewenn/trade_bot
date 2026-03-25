from decimal import Decimal


def calculate_taker_fee(
    price: float,
    fee_rate: float = 0.25,
    exponent: float = 2.0,
) -> float:
    """Calculate taker fee for a given price.

    Formula: C * p * feeRate * (p * (1 - p))^exponent
    where C = complement = min(price, 1 - price)

    Args:
        price: Order price (0.0 to 1.0).
        fee_rate: Fee rate parameter (default 0.25).
        exponent: Exponent parameter (default 2.0).

    Returns:
        Fee as a fraction of the notional.
    """
    if price <= 0.0 or price >= 1.0:
        return 0.0

    complement = min(price, 1.0 - price)
    fee = complement * price * fee_rate * (price * (1.0 - price)) ** exponent
    return fee


def calculate_taker_fee_decimal(
    price: Decimal,
    size: Decimal,
    fee_rate: float = 0.25,
    exponent: float = 2.0,
) -> Decimal:
    """Calculate taker fee in USDC for a given order."""
    fee_pct = calculate_taker_fee(float(price), fee_rate, exponent)
    return Decimal(str(fee_pct)) * size


def calculate_maker_fee(price: Decimal, size: Decimal) -> Decimal:
    """Maker fee is always zero on 5-minute crypto markets."""
    return Decimal("0")


def estimate_maker_rebate(
    maker_volume: Decimal,
    total_taker_fees_collected: Decimal,
    total_maker_volume: Decimal,
) -> Decimal:
    """Estimate daily maker rebate.

    Rebate = 20% of collected taker fees, distributed proportionally by maker volume.
    """
    if total_maker_volume <= 0:
        return Decimal("0")

    rebate_pool = total_taker_fees_collected * Decimal("0.20")
    share = maker_volume / total_maker_volume
    return rebate_pool * share


def breakeven_edge_taker(price: float) -> float:
    """Minimum edge needed for a profitable taker trade."""
    return calculate_taker_fee(price)


def breakeven_edge_maker() -> float:
    """Minimum edge needed for a profitable maker trade (zero fees)."""
    return 0.0
