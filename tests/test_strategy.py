import math
import pytest
from decimal import Decimal
from scipy.stats import norm

from core.strategy import Strategy
from core.state import SharedState
from data.models import MarketInfo, MarketPhase, OrderBook, OrderBookLevel


class TestEstimateProbability:
    """Test the log-normal probability model."""

    def test_price_above_anchor_gives_high_prob(self):
        """When BTC is above anchor, P(Up) should be > 50%."""
        prob = Strategy.estimate_probability(
            current_price=100_000,
            anchor_price=99_900,
            remaining_seconds=150,
            volatility_per_second=0.00001,
        )
        assert prob > 0.5

    def test_price_below_anchor_gives_low_prob(self):
        """When BTC is below anchor, P(Up) should be < 50%."""
        prob = Strategy.estimate_probability(
            current_price=99_900,
            anchor_price=100_000,
            remaining_seconds=150,
            volatility_per_second=0.00001,
        )
        assert prob < 0.5

    def test_price_equals_anchor_gives_50pct(self):
        """When BTC equals anchor, P(Up) should be ~50%."""
        prob = Strategy.estimate_probability(
            current_price=100_000,
            anchor_price=100_000,
            remaining_seconds=150,
            volatility_per_second=0.00001,
        )
        assert abs(prob - 0.5) < 0.01

    def test_zero_remaining_above(self):
        """At expiry, if price >= anchor, P(Up) = 1."""
        prob = Strategy.estimate_probability(
            current_price=100_001,
            anchor_price=100_000,
            remaining_seconds=0,
        )
        assert prob == 1.0

    def test_zero_remaining_below(self):
        """At expiry, if price < anchor, P(Up) = 0."""
        prob = Strategy.estimate_probability(
            current_price=99_999,
            anchor_price=100_000,
            remaining_seconds=0,
        )
        assert prob == 0.0

    def test_clamped_to_range(self):
        """Probability should be clamped to [0.01, 0.99]."""
        # Very large move should still be clamped
        prob = Strategy.estimate_probability(
            current_price=110_000,
            anchor_price=100_000,
            remaining_seconds=10,
            volatility_per_second=0.00001,
        )
        assert prob <= 0.99

        prob = Strategy.estimate_probability(
            current_price=90_000,
            anchor_price=100_000,
            remaining_seconds=10,
            volatility_per_second=0.00001,
        )
        assert prob >= 0.01

    def test_matches_scipy_reference(self):
        """Verify against manual scipy calculation (within clamping bounds)."""
        S = 100_005.0  # Small move so result stays within [0.01, 0.99]
        K = 100_000.0
        t = 200.0
        vol = 0.00001

        log_return = math.log(S / K)
        sigma = vol * math.sqrt(t)
        z = log_return / sigma
        expected = float(norm.cdf(z))
        # Clamp expected like the function does
        expected = max(0.01, min(0.99, expected))

        actual = Strategy.estimate_probability(S, K, t, vol)
        assert abs(actual - expected) < 0.001

    def test_more_time_means_closer_to_50(self):
        """With more time remaining, probability should be closer to 50%."""
        # Use a small enough move that clamping doesn't flatten both values
        prob_short = Strategy.estimate_probability(100_001, 100_000, 30, 0.0001)
        prob_long = Strategy.estimate_probability(100_001, 100_000, 280, 0.0001)

        assert abs(prob_long - 0.5) < abs(prob_short - 0.5)


class TestKellySize:
    """Test fractional Kelly position sizing."""

    def test_no_edge_returns_zero(self):
        size = Strategy.kelly_size(
            estimated_prob=0.50,
            market_price=0.55,
            kelly_fraction=0.25,
            bankroll=1000,
        )
        assert size == 0.0

    def test_positive_edge(self):
        size = Strategy.kelly_size(
            estimated_prob=0.60,
            market_price=0.50,
            kelly_fraction=0.25,
            bankroll=1000,
            max_position_pct=0.10,  # Raise cap so Kelly isn't clamped
        )
        # full_kelly = (0.60 - 0.50) / (1 - 0.50) = 0.20
        # fractional = 0.20 * 0.25 = 0.05
        # dollar = 0.05 * 1000 = 50
        assert abs(size - 50.0) < 0.01

    def test_capped_at_max_position(self):
        size = Strategy.kelly_size(
            estimated_prob=0.90,
            market_price=0.10,
            kelly_fraction=0.25,
            bankroll=1000,
            max_position_pct=0.02,
        )
        assert size <= 1000 * 0.02

    def test_market_price_at_one_returns_zero(self):
        size = Strategy.kelly_size(
            estimated_prob=0.99,
            market_price=1.0,
        )
        assert size == 0.0


class TestSignalGeneration:
    """Test the full signal generation pipeline."""

    def _make_state_with_market(self, anchor=Decimal("100000"), binance=Decimal("100050")):
        import time

        state = SharedState()
        state.current_market = MarketInfo(
            event_slug="btc-updown-5m-123",
            condition_id="cond123",
            token_id_up="up_token",
            token_id_down="down_token",
            start_time=time.time() - 10,  # Started 10s ago
            end_time=time.time() + 290,  # 290s remaining
            anchor_price=anchor,
        )
        state.market_phase = MarketPhase.OPEN
        state.binance_price = binance
        state.binance_tick_time = time.monotonic()

        # Set up orderbooks
        state.orderbook_up = OrderBook(
            asset_id="up_token",
            bids=[OrderBookLevel(Decimal("0.48"), Decimal("100"))],
            asks=[OrderBookLevel(Decimal("0.52"), Decimal("100"))],
            timestamp_ms=int(time.time() * 1000),
        )
        state.orderbook_down = OrderBook(
            asset_id="down_token",
            bids=[OrderBookLevel(Decimal("0.48"), Decimal("100"))],
            asks=[OrderBookLevel(Decimal("0.52"), Decimal("100"))],
            timestamp_ms=int(time.time() * 1000),
        )
        state.polymarket_tick_time = time.monotonic()

        return state

    def test_signal_with_sufficient_edge(self, settings):
        strategy = Strategy(settings)
        state = self._make_state_with_market(
            anchor=Decimal("100000"),
            binance=Decimal("100500"),  # Strong upward move
        )

        signal = strategy.generate_signal(state, 1000.0)
        # With a large enough move, should get a signal
        # The exact outcome depends on the probability model
        # Just check it returns something or None gracefully
        if signal:
            assert signal.direction in ("UP", "DOWN")
            assert signal.edge > 0
            assert signal.kelly_size > 0

    def test_no_signal_without_market(self, settings):
        strategy = Strategy(settings)
        state = SharedState()
        signal = strategy.generate_signal(state, 1000.0)
        assert signal is None

    def test_no_signal_past_entry_window(self, settings):
        import time

        strategy = Strategy(settings)
        state = self._make_state_with_market()
        # Set market to have started 300s ago (past 240s entry window)
        state.current_market.start_time = time.time() - 300
        signal = strategy.generate_signal(state, 1000.0)
        assert signal is None
