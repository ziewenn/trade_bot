import math
import time
from decimal import Decimal
from typing import Optional

import numpy as np
from scipy.stats import norm

from config.settings import Settings
from core.state import SharedState
from data.models import Signal, MarketPhase
from monitoring.logger import get_logger

logger = get_logger("strategy")


class Strategy:
    """Core latency arbitrage decision engine.

    Uses Binance real-time price as a leading indicator against the Chainlink
    anchor price to estimate true P(Up) and find edge against Polymarket orderbook.
    """

    def __init__(self, settings: Settings):
        self._trading_mode = settings.trading_mode
        self.min_edge = settings.min_edge_pct
        self.kelly_fraction = settings.kelly_fraction
        self.max_position_pct = settings.max_position_pct
        self.max_concurrent_exposure_pct = settings.max_concurrent_exposure_pct
        self.entry_window_sec = settings.market_entry_window_sec
        self.exit_buffer_sec = settings.market_exit_buffer_sec
        # Note: order_offset_cents in settings is no longer used — dynamic offset is calculated per-signal
        self.volatility_per_second = settings.volatility_per_second
        self._entry_min_elapsed_sec = settings.market_entry_min_elapsed_sec
        self._min_price_gap = settings.min_price_gap_usd
        self._binance_weight = settings.binance_prediction_weight

        # Volatility calibration state
        self._vol_window: list[tuple[int, float]] = []  # (ts_ms, log_return)
        self._last_vol_price: Optional[float] = None
        self._calibrated_vol: float = settings.volatility_per_second

    def generate_signal(
        self, state: SharedState, bankroll: float
    ) -> Optional[Signal]:
        """Generate a trading signal from current state.

        Returns Signal if there's actionable edge, None otherwise.
        """
        market = state.current_market
        if not market or not market.anchor_price:
            logger.info("signal_skip", reason="no_market_or_anchor")
            return None

        # Block trading if anchor is not authoritative (on-chain/state sources
        # can be $10+ wrong, causing guaranteed wrong-direction bets)
        if not state.anchor_is_authoritative:
            logger.info(
                "signal_skip",
                reason="anchor_not_authoritative",
                source=state.anchor_source,
            )
            return None

        # Cooldown: wait 2 seconds after anchor is set for price feeds to stabilize.
        # Right after cycle transition, Chainlink/Binance prices may be stale or
        # jumpy, causing wrong-direction bets.
        import time
        anchor_age = time.monotonic() - state.anchor_set_mono
        if state.anchor_set_mono > 0 and anchor_age < 2.0:
            logger.info(
                "signal_skip",
                reason="anchor_cooldown",
                age_s=round(anchor_age, 2),
            )
            return None

        # Check market phase
        remaining = state.get_remaining_seconds()
        elapsed = state.get_elapsed_seconds()

        if remaining <= 0:
            return None

        # Only enter within entry window of market open
        if elapsed > self.entry_window_sec:
            return None

        # Don't enter too early — probability estimates are unreliable with
        # lots of remaining time because small gaps easily revert
        if elapsed < self._entry_min_elapsed_sec:
            logger.info("signal_skip", reason="too_early", elapsed=round(elapsed, 1))
            return None

        # Don't enter if too close to resolution
        if remaining <= self.exit_buffer_sec:
            return None

        # Don't trade if Chainlink is stale — Polymarket resolves using Chainlink,
        # so trading without fresh Chainlink data risks guaranteed losses
        if state.chainlink_tick_time > 0:
            chainlink_age = time.monotonic() - state.chainlink_tick_time
            if chainlink_age > 10:
                logger.info(
                    "signal_skip",
                    reason="chainlink_stale",
                    seconds_since_last=round(chainlink_age, 1),
                    source=state.chainlink_source,
                )
                return None

        # Need both Binance price and orderbook data
        if state.binance_price <= 0:
            logger.info("signal_skip", reason="no_binance_price")
            return None

        anchor = float(market.anchor_price)
        current = float(state.chainlink_price)  # Chainlink = resolution oracle
        binance = float(state.binance_price)     # Binance = leading indicator

        if anchor <= 0 or current <= 0:
            return None

        # Always compute and display edge/direction for dashboard,
        # even when trade filters below will block execution.
        w = self._binance_weight
        predicted_price = (1 - w) * current + w * binance
        chainlink_says_up = current > anchor
        binance_says_up = binance > anchor

        true_prob_up = self.estimate_probability(
            current_price=predicted_price,
            anchor_price=anchor,
            remaining_seconds=remaining,
            volatility_per_second=self._calibrated_vol,
        )

        market_prob_up = self._get_market_prob_up(state)
        if market_prob_up is not None:
            if chainlink_says_up:
                edge = true_prob_up - market_prob_up
            else:
                edge = (1.0 - true_prob_up) - (1.0 - market_prob_up)
            state.current_true_prob = true_prob_up
            state.current_edge = edge
            state.current_signal_direction = "UP" if chainlink_says_up else "DOWN"

        # --- Trade filters (block execution, not dashboard) ---

        chainlink_gap = abs(current - anchor)
        binance_gap = abs(binance - anchor)
        price_gap = max(chainlink_gap, binance_gap)
        if price_gap < self._min_price_gap:
            logger.info(
                "signal_skip",
                reason="price_gap_too_small",
                gap_usd=round(price_gap, 2),
                chainlink_gap=round(chainlink_gap, 2),
                binance_gap=round(binance_gap, 2),
                min_required=self._min_price_gap,
            )
            return None

        min_chainlink_gap = self._min_price_gap * 0.25
        if chainlink_gap < min_chainlink_gap:
            logger.info(
                "signal_skip",
                reason="chainlink_gap_too_small",
                chainlink_gap=round(chainlink_gap, 2),
                binance_gap=round(binance_gap, 2),
                min_chainlink_gap=round(min_chainlink_gap, 2),
            )
            return None

        if chainlink_says_up != binance_says_up:
            logger.info(
                "signal_note",
                reason="binance_chainlink_disagree",
                chainlink=round(current, 2),
                binance=round(binance, 2),
                anchor=round(anchor, 2),
                trading_direction="UP" if chainlink_says_up else "DOWN",
            )

        # Verify Binance price is not wildly spiking (check tick-to-tick stability).
        now_ms = int(time.time() * 1000)
        recent_cutoff = now_ms - 5000
        recent_ticks = [(ts, float(p)) for ts, p in state.price_history if ts >= recent_cutoff]

        if len(recent_ticks) >= 3:
            prices = [p for _, p in recent_ticks]
            avg_price = sum(prices) / len(prices)
            max_deviation = max(abs(p - avg_price) for p in prices)
            if max_deviation > 100:
                logger.info(
                    "signal_skip",
                    reason="binance_price_spiking",
                    max_deviation=round(max_deviation, 2),
                    ticks_checked=len(recent_ticks),
                )
                return None

        if market_prob_up is None:
            logger.info(
                "signal_skip",
                reason="no_market_prob",
                best_ask_up=str(state.best_ask_up),
                best_bid_down=str(state.best_bid_down),
                has_book_up=state.orderbook_up is not None,
                has_book_down=state.orderbook_down is not None,
            )
            return None

        if chainlink_says_up:
            direction = "UP"
            edge = true_prob_up - market_prob_up
            token_id = market.token_id_up
            best_bid = state.best_bid_up
            market_price = market_prob_up
        else:
            direction = "DOWN"
            true_prob_down = 1.0 - true_prob_up
            market_prob_down = 1.0 - market_prob_up
            edge = true_prob_down - market_prob_down
            token_id = market.token_id_down
            best_bid = state.best_bid_down
            market_price = market_prob_down

        if best_bid is None:
            return None

        # Dynamic order offset: 20% of edge, capped at 2¢
        # For $50 bankroll, this prevents overpaying on small edges
        dynamic_offset = min(edge * 0.2, 0.02)
        order_price = best_bid + Decimal(str(round(max(dynamic_offset, 0.005), 4)))

        # Edge filter: skip if edge is below minimum threshold
        if edge < self.min_edge:
            logger.info(
                "signal_skip",
                reason="edge_too_low",
                edge=round(edge, 4),
                min_edge=self.min_edge,
            )
            return None

        # Calculate position size — use Kelly when edge is positive,
        # otherwise use max_position_pct as fixed size
        size = self.kelly_size(
            estimated_prob=true_prob_up if direction == "UP" else (1.0 - true_prob_up),
            market_price=market_price,
            kelly_fraction=self.kelly_fraction,
            bankroll=bankroll,
            max_position_pct=self.max_position_pct,
        )

        if size <= 0:
            size = bankroll * self.max_position_pct

        # Convert dollar amount to share count for CLOB API
        # CLOB API expects size = number of shares, price = cost per share
        # Dollar cost = shares × price, so shares = dollars / price
        if float(order_price) > 0:
            shares = size / float(order_price)
        else:
            shares = 0

        if shares <= 0:
            return None

        # Check concurrent exposure (in dollar terms)
        current_exposure = sum(
            float(p.size * p.avg_entry_price)
            for p in state.open_positions.values()
        )
        max_exposure = bankroll * self.max_concurrent_exposure_pct
        dollar_cost = shares * float(order_price)
        if current_exposure + dollar_cost > max_exposure:
            # Reduce shares to fit within exposure limit
            available_dollars = max(0, max_exposure - current_exposure)
            shares = available_dollars / float(order_price) if float(order_price) > 0 else 0
            if shares <= 0:
                return None

        logger.info(
            "signal_generated",
            direction=direction,
            true_prob=round(true_prob_up, 4),
            market_prob=round(market_price, 4),
            edge=round(edge, 4),
            shares=round(shares, 2),
            dollar_cost=round(shares * float(order_price), 2),
            price=float(order_price),
            predicted_price=round(predicted_price, 2),
            chainlink=round(current, 2),
            binance=round(binance, 2),
        )

        return Signal(
            direction=direction,
            true_prob=true_prob_up if direction == "UP" else (1.0 - true_prob_up),
            market_prob=market_price,
            edge=edge,
            kelly_size=Decimal(str(round(shares, 2))),
            token_id=token_id,
            price=order_price,
            timestamp_ms=int(time.time() * 1000),
        )

    def should_exit(self, state: SharedState) -> bool:
        """Check if we should exit positions near market close.

        Uses CHAINLINK price (not Binance) to decide, because Chainlink
        is what Polymarket uses for resolution. Only exits if Chainlink
        says we're LOSING — if we're winning, hold for the $1.00 payout.

        Logic:
        - We hold UP tokens + Chainlink > anchor → WINNING → don't exit
        - We hold UP tokens + Chainlink < anchor → LOSING → exit early
        - We hold DOWN tokens + Chainlink < anchor → WINNING → don't exit
        - We hold DOWN tokens + Chainlink > anchor → LOSING → exit early
        """
        remaining = state.get_remaining_seconds()
        if remaining <= 0 or remaining > self.exit_buffer_sec:
            return False

        # Need Chainlink price and anchor to decide
        market = state.current_market
        if not market or not market.anchor_price or state.chainlink_price <= 0:
            return False

        # Check if we have any positions
        if not state.open_positions:
            return False

        # Determine our position direction
        chainlink = float(state.chainlink_price)
        anchor = float(market.anchor_price)
        chainlink_says_up = chainlink > anchor

        # Check ALL positions — exit only if NONE are winning
        any_winning = False
        for pos in state.open_positions.values():
            is_up_position = pos.token_id == market.token_id_up
            is_down_position = pos.token_id == market.token_id_down

            if is_up_position and chainlink_says_up:
                any_winning = True
            if is_down_position and not chainlink_says_up:
                any_winning = True

        # Exit only if ALL positions are losing
        return not any_winning

    @staticmethod
    def estimate_probability(
        current_price: float,
        anchor_price: float,
        remaining_seconds: float,
        volatility_per_second: float = 0.00001,
    ) -> float:
        """Estimate P(BTC >= anchor_price at market close) given current price.

        Uses a simple log-normal model.
        """
        if remaining_seconds <= 0:
            return 1.0 if current_price >= anchor_price else 0.0

        if anchor_price <= 0 or current_price <= 0:
            return 0.5

        # Log return from anchor to current
        log_return = math.log(current_price / anchor_price)

        # Expected drift is approximately zero for 5-min windows
        drift = 0.0

        # Standard deviation of remaining price movement
        sigma = volatility_per_second * math.sqrt(remaining_seconds)

        # Dynamic floor: at least 30 seconds worth of volatility
        # This prevents overconfident signals at very short remaining times
        # while staying proportional to actual market conditions.
        # With default vol=0.0005: floor = 0.0005 * sqrt(30) ≈ 0.0027
        sigma_floor = volatility_per_second * math.sqrt(30)
        sigma = max(sigma, sigma_floor)

        if sigma == 0:
            return 1.0 if current_price >= anchor_price else 0.0

        # P(final_price >= anchor) using log-normal model
        z = (log_return + drift) / sigma
        prob_up = float(norm.cdf(z))

        # Clamp to [0.01, 0.99] to avoid extreme bets
        return max(0.01, min(0.99, prob_up))

    @staticmethod
    def kelly_size(
        estimated_prob: float,
        market_price: float,
        kelly_fraction: float = 0.25,
        bankroll: float = 1000.0,
        max_position_pct: float = 0.02,
    ) -> float:
        """Calculate position size using fractional Kelly criterion.

        For binary markets: f* = (q - p) / (1 - p)
        where q = estimated true probability, p = market price.
        """
        if estimated_prob <= market_price:
            return 0.0

        if market_price >= 1.0:
            return 0.0

        full_kelly = (estimated_prob - market_price) / (1.0 - market_price)
        fractional = full_kelly * kelly_fraction
        dollar_amount = fractional * bankroll

        # Hard cap
        max_dollars = bankroll * max_position_pct
        return min(dollar_amount, max_dollars)

    def _get_market_prob_up(self, state: SharedState) -> Optional[float]:
        """Get market-implied P(Up) from the orderbook.

        Tries multiple fallbacks since thin markets may only have
        partial orderbook data on one side.
        """
        # Best ask on Up token = cost to buy Up = market's P(Up)
        if state.best_ask_up is not None:
            return float(state.best_ask_up)

        # Fallback: 1 - best_bid_down
        if state.best_bid_down is not None:
            return 1.0 - float(state.best_bid_down)

        # Fallback: best_bid_up (slightly less accurate — inside the spread)
        if state.best_bid_up is not None:
            return float(state.best_bid_up)

        # Fallback: 1 - best_ask_down
        if state.best_ask_down is not None:
            return 1.0 - float(state.best_ask_down)

        return None

    def update_volatility(self, price: float, timestamp_ms: int):
        """Update realized volatility estimate from Binance ticks."""
        if self._last_vol_price is not None and self._last_vol_price > 0:
            log_ret = math.log(price / self._last_vol_price)
            self._vol_window.append((timestamp_ms, log_ret))

            # Keep last 60 seconds
            cutoff = timestamp_ms - 60_000
            self._vol_window = [
                (ts, r) for ts, r in self._vol_window if ts >= cutoff
            ]

            # Recalibrate: realized vol = std of returns per tick, scaled
            if len(self._vol_window) >= 10:
                returns = [r for _, r in self._vol_window]
                realized_std = float(np.std(returns))

                # Average time between ticks
                timestamps = [ts for ts, _ in self._vol_window]
                if len(timestamps) >= 2:
                    avg_dt_ms = (timestamps[-1] - timestamps[0]) / (len(timestamps) - 1)
                    avg_dt_s = avg_dt_ms / 1000.0
                    if avg_dt_s > 0:
                        # Floor: prevent overconfident estimates in quiet markets
                        self._calibrated_vol = max(
                            realized_std / math.sqrt(avg_dt_s),
                            0.0002,  # Never below 0.02%/sec
                        )

        self._last_vol_price = price
