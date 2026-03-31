import asyncio
import time
from decimal import Decimal
from typing import Optional

from config.settings import Settings
from core.state import SharedState
from core.strategy import Strategy
from data.models import Order, OrderStatus, Signal
from execution.trader_interface import TraderInterface
from monitoring.logger import get_logger

logger = get_logger("order_manager")


class RateLimiter:
    """Token bucket rate limiter for Polymarket CLOB API."""

    def __init__(self, max_tokens: int, refill_interval_s: float = 10.0):
        self.max_tokens = max_tokens
        self.tokens = max_tokens
        self.refill_interval = refill_interval_s
        self._last_refill = time.monotonic()

    async def acquire(self):
        now = time.monotonic()
        elapsed = now - self._last_refill
        if elapsed >= self.refill_interval:
            self.tokens = self.max_tokens
            self._last_refill = now

        if self.tokens <= 0:
            wait = self.refill_interval - elapsed
            if wait > 0:
                await asyncio.sleep(wait)
            self.tokens = self.max_tokens
            self._last_refill = time.monotonic()

        self.tokens -= 1


class OrderManager:
    """Manages the cancel/replace loop and bridges strategy signals to execution."""

    def __init__(
        self,
        trader: TraderInterface,
        strategy: Strategy,
        state: SharedState,
        settings: Settings,
        risk_manager=None,
    ):
        self.trader = trader
        self.strategy = strategy
        self.state = state
        self.cancel_replace_interval = settings.cancel_replace_interval_ms / 1000.0
        self.bankroll = settings.initial_bankroll
        self._risk_manager = risk_manager

        # Active orders tracking
        self._active_orders: dict[str, Order] = {}  # order_id -> Order
        self._current_signal: Optional[Signal] = None

        # Rate limiters
        self._post_limiter = RateLimiter(3500, 10.0)
        self._cancel_limiter = RateLimiter(3000, 10.0)

        # Metrics
        self._cancel_replace_latencies: list[float] = []

    async def run_cancel_replace_loop(self):
        """Main loop: re-evaluate signal and update orders every interval."""
        logger.info(
            "cancel_replace_loop_started",
            interval_ms=self.cancel_replace_interval * 1000,
        )

        iteration_count = 0
        last_heartbeat = time.monotonic()

        while True:
            start = time.monotonic()
            iteration_count += 1
            try:
                # Periodic heartbeat every 30s to confirm loop is alive
                if start - last_heartbeat >= 30.0:
                    avg_lat = self.avg_cancel_replace_latency_ms
                    logger.info(
                        "cancel_replace_heartbeat",
                        iterations=iteration_count,
                        avg_latency_ms=round(avg_lat, 1),
                        active_orders=len(self._active_orders),
                    )
                    last_heartbeat = start
                active_tokens = []
                if self.state.current_market:
                    active_tokens = [
                        self.state.current_market.token_id_up,
                        self.state.current_market.token_id_down,
                    ]
                await self.trader.sync_state(active_tokens)
                self.bankroll = float(self.trader.current_bankroll)

                self.state.open_positions = self.trader.current_positions
                self.state.paper_bankroll = self.trader.current_bankroll
                self.state.session_pnl = self.trader.current_pnl

                if self._risk_manager:
                    total_exposure = sum(
                        float(p.size * p.avg_entry_price)
                        for p in self.trader.current_positions.values()
                    )
                    equity = self.bankroll + total_exposure
                    self._risk_manager.update_equity(equity)
                    self._risk_manager.update_exposure(total_exposure)

                if self.state.trading_paused:
                    if self._active_orders:
                        await self.cancel_all()
                    self._current_signal = None
                else:
                    signal = self.strategy.generate_signal(self.state, self.bankroll)

                    # Check if we should exit positions
                    if self.strategy.should_exit(self.state):
                        await self._exit_all_positions()
                        self._current_signal = None
                    elif signal:
                        await self._update_orders(signal)
                        self._current_signal = signal
                    else:
                        # No signal — cancel stale orders
                        if self._active_orders:
                            await self.cancel_all()
                        self._current_signal = None

            except Exception as e:
                logger.error(
                    "cancel_replace_error",
                    error=str(e),
                    error_type=type(e).__name__,
                )
            except BaseException as e:
                logger.error(
                    "cancel_replace_fatal",
                    error=str(e),
                    error_type=type(e).__name__,
                )
                raise
            finally:
                # Always record latency — even on error, so 0ms doesn't mislead
                elapsed_ms = (time.monotonic() - start) * 1000
                self._cancel_replace_latencies.append(elapsed_ms)
                if len(self._cancel_replace_latencies) > 100:
                    self._cancel_replace_latencies = self._cancel_replace_latencies[-100:]

            await asyncio.sleep(self.cancel_replace_interval)

    async def _update_orders(self, signal: Signal):
        """Cancel stale orders and place updated ones based on new signal."""
        # Check if existing orders are still well-positioned
        orders_to_cancel = []
        has_matching_order = False

        for order_id, order in self._active_orders.items():
            if order.token_id != signal.token_id:
                # Wrong token (market may have changed)
                orders_to_cancel.append(order_id)
            elif abs(order.price - signal.price) > Decimal("0.01"):
                # Price has drifted — cancel and replace
                orders_to_cancel.append(order_id)
            else:
                has_matching_order = True

        # Cancel stale orders
        for order_id in orders_to_cancel:
            await self._cancel_order(order_id)

        # Place new order if needed
        if not has_matching_order:
            await self._place_order(signal)

    async def _place_order(self, signal: Signal):
        """Place a new maker order based on signal."""
        await self._post_limiter.acquire()

        try:
            if self._risk_manager:
                total_exposure = sum(
                    float(p.size * p.avg_entry_price)
                    for p in self.trader.current_positions.values()
                )
                current_equity = self.bankroll + total_exposure
                is_allowed = await self._risk_manager.check_pre_trade(signal, current_equity)
                if not is_allowed:
                    logger.debug("pre_trade_rejected")
                    return

            market = self.state.current_market
            condition_id = market.condition_id if market else ""

            order = await self.trader.place_order(
                token_id=signal.token_id,
                side="BUY",
                price=signal.price,
                size=signal.kelly_size,
                market_condition_id=condition_id,
            )

            if order.status == OrderStatus.LIVE:
                self._active_orders[order.order_id] = order
                self.state.open_orders[order.order_id] = order

                if hasattr(self.trader, "on_immediate_fill_check"):
                    is_up_token = (
                        market
                        and signal.token_id == market.token_id_up
                    )
                    best_ask = (
                        self.state.best_ask_up if is_up_token
                        else self.state.best_ask_down
                    )
                    best_bid = (
                        self.state.best_bid_up if is_up_token
                        else self.state.best_bid_down
                    )
                    await self.trader.on_immediate_fill_check(
                        order, best_ask, best_bid
                    )
                    if order.status != OrderStatus.LIVE:
                        self._active_orders.pop(order.order_id, None)
                        self.state.open_orders.pop(order.order_id, None)

        except Exception as e:
            logger.error("place_order_failed", error=str(e), error_type=type(e).__name__)

    async def _cancel_order(self, order_id: str):
        """Cancel a single order."""
        await self._cancel_limiter.acquire()

        try:
            success = await self.trader.cancel_order(order_id)
            if success:
                self._active_orders.pop(order_id, None)
                self.state.open_orders.pop(order_id, None)
        except Exception as e:
            logger.error("cancel_order_failed", order_id=order_id[:8], error=str(e), error_type=type(e).__name__)

    async def cancel_all(self) -> int:
        """Emergency cancel all orders."""
        count = await self.trader.cancel_all_orders()
        self._active_orders.clear()
        self.state.open_orders.clear()
        if count:
            logger.info("all_orders_cancelled", count=count)
        return count

    async def _exit_all_positions(self):
        """Exit all positions before market resolution by placing sell orders."""
        # First cancel all existing buy orders
        await self.cancel_all()

        # Place sell orders for all open positions
        for token_id, position in list(self.state.open_positions.items()):
            if position.size <= 0:
                continue

            # Sell at current best bid (aggressive exit)
            if token_id == getattr(self.state.current_market, "token_id_up", ""):
                best_bid = self.state.best_bid_up
            else:
                best_bid = self.state.best_bid_down

            if best_bid and best_bid > 0:
                try:
                    order = await self.trader.place_order(
                        token_id=token_id,
                        side="SELL",
                        price=best_bid,
                        size=position.size,
                        market_condition_id=position.market_condition_id,
                    )
                    if order.status == OrderStatus.LIVE:
                        self._active_orders[order.order_id] = order

                        if hasattr(self.trader, "on_immediate_fill_check"):
                            is_up = token_id == getattr(
                                self.state.current_market, "token_id_up", ""
                            )
                            await self.trader.on_immediate_fill_check(
                                order,
                                self.state.best_ask_up if is_up else self.state.best_ask_down,
                                self.state.best_bid_up if is_up else self.state.best_bid_down,
                            )
                            if order.status != OrderStatus.LIVE:
                                self._active_orders.pop(order.order_id, None)

                    logger.info(
                        "exit_order_placed",
                        token_id=token_id[:8],
                        price=float(best_bid),
                        size=float(position.size),
                    )
                except Exception as e:
                    logger.error("exit_order_failed", error=str(e), error_type=type(e).__name__)

    def update_bankroll(self, new_bankroll: float):
        self.bankroll = new_bankroll

    @property
    def avg_cancel_replace_latency_ms(self) -> float:
        if not self._cancel_replace_latencies:
            return 0.0
        return sum(self._cancel_replace_latencies) / len(self._cancel_replace_latencies)

    async def on_market_transition(self):
        """Handle transition to a new 5-minute market."""
        await self.cancel_all()
        self._current_signal = None

        # Reset strategy state displayed on dashboard so stale values don't persist
        self.state.current_true_prob = 0.5
        self.state.current_edge = 0.0
        self.state.current_signal_direction = None
        self.state.orderbook_up = None
        self.state.orderbook_down = None

        logger.info("market_transition_complete")
