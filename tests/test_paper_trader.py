import pytest
import pytest_asyncio
from decimal import Decimal

from execution.paper_trader import PaperTrader
from data.models import OrderStatus, Side


class TestPaperTrader:

    @pytest_asyncio.fixture
    async def trader(self, database):
        return PaperTrader(initial_bankroll=1000.0, database=database)

    @pytest.mark.asyncio
    async def test_place_order(self, trader):
        order = await trader.place_order(
            token_id="up_token",
            side="BUY",
            price=Decimal("0.50"),
            size=Decimal("20"),
            market_condition_id="cond123",
        )
        assert order.status == OrderStatus.LIVE
        assert order.price == Decimal("0.50")
        assert order.size == Decimal("20")

    @pytest.mark.asyncio
    async def test_cancel_order(self, trader):
        order = await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )
        result = await trader.cancel_order(order.order_id)
        assert result is True

        # Cancelling again should fail
        result = await trader.cancel_order(order.order_id)
        assert result is False

    @pytest.mark.asyncio
    async def test_cancel_all(self, trader):
        await trader.place_order("up_token", "BUY", Decimal("0.50"), Decimal("20"), "c1")
        await trader.place_order("down_token", "BUY", Decimal("0.48"), Decimal("10"), "c1")
        count = await trader.cancel_all_orders()
        assert count == 2

        orders = await trader.get_open_orders()
        assert len(orders) == 0

    @pytest.mark.asyncio
    async def test_fill_on_trade_event(self, trader):
        """Paper buy order fills when trade at or below order price."""
        order = await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )

        # Trade at 0.49 should fill our 0.50 buy
        await trader.on_trade_event("up_token", Decimal("0.49"), Decimal("100"), 1000)

        orders = await trader.get_open_orders()
        assert len(orders) == 0  # Should be filled

        # Check position was created
        assert "up_token" in trader.positions

    @pytest.mark.asyncio
    async def test_no_fill_on_wrong_price(self, trader):
        """Paper buy order does NOT fill when trade above order price."""
        await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )

        # Trade at 0.55 should NOT fill our 0.50 buy
        await trader.on_trade_event("up_token", Decimal("0.55"), Decimal("100"), 1000)

        orders = await trader.get_open_orders()
        assert len(orders) == 1  # Still live

    @pytest.mark.asyncio
    async def test_no_fill_on_wrong_token(self, trader):
        """Paper buy order does NOT fill on a different token's trade."""
        await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )

        await trader.on_trade_event("down_token", Decimal("0.40"), Decimal("100"), 1000)

        orders = await trader.get_open_orders()
        assert len(orders) == 1

    @pytest.mark.asyncio
    async def test_slippage_applied(self, trader):
        """Fill price should include 0.5 cent slippage."""
        await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )

        await trader.on_trade_event("up_token", Decimal("0.49"), Decimal("100"), 1000)

        pos = trader.positions.get("up_token")
        assert pos is not None
        # Fill price should be trade_price + slippage = 0.49 + 0.005 = 0.495
        assert pos.avg_entry_price == Decimal("0.495")

    @pytest.mark.asyncio
    async def test_bankroll_decreases_on_buy(self, trader):
        initial = trader.bankroll
        await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )
        await trader.on_trade_event("up_token", Decimal("0.49"), Decimal("100"), 1000)

        # Cost = fill_price * size = 0.495 * 20 = 9.90
        expected = initial - Decimal("0.495") * Decimal("20")
        assert trader.bankroll == expected

    @pytest.mark.asyncio
    async def test_market_resolution_win(self, trader):
        # Buy up tokens
        await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )
        await trader.on_trade_event("up_token", Decimal("0.49"), Decimal("100"), 1000)

        bankroll_before = trader.bankroll

        # Market resolves Up wins
        await trader.resolve_market("up_token", "down_token")

        # Should get $1 per share = 20 shares = $20
        payout = Decimal("20") * Decimal("1.0")
        assert trader.bankroll == bankroll_before + payout
        assert "up_token" not in trader.positions

    @pytest.mark.asyncio
    async def test_market_resolution_loss(self, trader):
        await trader.place_order(
            "up_token", "BUY", Decimal("0.50"), Decimal("20"), "cond123"
        )
        await trader.on_trade_event("up_token", Decimal("0.49"), Decimal("100"), 1000)

        bankroll_before = trader.bankroll

        # Market resolves Down wins — our up tokens are worthless
        await trader.resolve_market("down_token", "up_token")

        assert trader.bankroll == bankroll_before  # No payout
        assert "up_token" not in trader.positions
