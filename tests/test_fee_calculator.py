import pytest
from decimal import Decimal

from core.fee_calculator import (
    calculate_taker_fee,
    calculate_taker_fee_decimal,
    calculate_maker_fee,
    breakeven_edge_taker,
    breakeven_edge_maker,
)


class TestTakerFee:

    def test_fee_at_50_pct(self):
        """Fee at 0.50 is highest (formula: C*p*feeRate*(p*(1-p))^exp)."""
        fee = calculate_taker_fee(0.50)
        # C=0.50, p=0.50, 0.25*0.0625=0.00390625
        assert abs(fee - 0.00390625) < 0.0001

    def test_fee_at_30_pct(self):
        """Fee at 0.30."""
        fee = calculate_taker_fee(0.30)
        # C=0.30, p=0.30, p*(1-p)=0.21, 0.30*0.30*0.25*0.21^2
        expected = 0.30 * 0.30 * 0.25 * (0.30 * 0.70) ** 2
        assert abs(fee - expected) < 0.0001

    def test_fee_at_10_pct(self):
        """At price 0.10, fee should be near zero."""
        fee = calculate_taker_fee(0.10)
        assert fee < 0.001

    def test_fee_at_90_pct(self):
        """At price 0.90, fee should be near zero."""
        fee = calculate_taker_fee(0.90)
        assert fee < 0.001

    def test_fee_at_extremes(self):
        """At 0 and 1, fee should be 0."""
        assert calculate_taker_fee(0.0) == 0.0
        assert calculate_taker_fee(1.0) == 0.0

    def test_fee_higher_near_50(self):
        """Fee should be higher near 50% than at extremes."""
        fee_50 = calculate_taker_fee(0.50)
        fee_10 = calculate_taker_fee(0.10)
        fee_90 = calculate_taker_fee(0.90)
        assert fee_50 > fee_10
        assert fee_50 > fee_90

    def test_max_fee_at_50(self):
        """Fee should be maximized near p=0.50."""
        fee_50 = calculate_taker_fee(0.50)
        fee_40 = calculate_taker_fee(0.40)
        fee_60 = calculate_taker_fee(0.60)
        assert fee_50 > fee_40
        assert fee_50 > fee_60


class TestTakerFeeDecimal:

    def test_fee_in_usdc(self):
        fee = calculate_taker_fee_decimal(
            price=Decimal("0.50"),
            size=Decimal("100"),
        )
        # Formula gives ~0.39% at 0.50, * 100 shares
        assert float(fee) == pytest.approx(0.39, abs=0.05)


class TestMakerFee:

    def test_always_zero(self):
        assert calculate_maker_fee(Decimal("0.50"), Decimal("100")) == Decimal("0")
        assert calculate_maker_fee(Decimal("0.10"), Decimal("1000")) == Decimal("0")
        assert calculate_maker_fee(Decimal("0.99"), Decimal("1")) == Decimal("0")


class TestBreakevenEdge:

    def test_maker_breakeven_is_zero(self):
        assert breakeven_edge_maker() == 0.0

    def test_taker_breakeven_positive(self):
        edge = breakeven_edge_taker(0.50)
        assert edge > 0.0  # Some edge needed
