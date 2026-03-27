"""Simulated live trader — paper trading with realistic execution imperfections.

Inherits from PaperTrader and adds:
- Network latency (100-300ms for orders, 30-80ms for cancels)
- Random order rejections (5% default)
- Reduced fill rate (70% default — simulates queue position)
- Higher slippage (1.5¢ default vs 0.5¢ paper)
"""

import asyncio
import random
import time
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

from data.models import Order, OrderStatus, Side
from data.database import Database
from execution.paper_trader import PaperTrader
from monitoring.logger import get_logger

if TYPE_CHECKING:
    from config.settings import Settings

logger = get_logger("sim_live_trader")


class SimulatedLiveTrader(PaperTrader):
    """Paper trader with realistic execution delays and imperfections."""

    def __init__(
        self,
        initial_bankroll: float,
        database: Database,
        settings: "Settings",
    ):
        super().__init__(initial_bankroll=initial_bankroll, database=database)

        # Latency settings (ms)
        self._order_latency = (
            settings.sim_order_latency_min_ms,
            settings.sim_order_latency_max_ms,
        )
        self._cancel_latency = (
            settings.sim_cancel_latency_min_ms,
            settings.sim_cancel_latency_max_ms,
        )
        self._fill_latency = (
            settings.sim_fill_latency_min_ms,
            settings.sim_fill_latency_max_ms,
        )

        # Execution imperfections
        self._rejection_rate = settings.sim_rejection_rate
        self._fill_rate = settings.sim_fill_rate
        self._slippage = Decimal(str(settings.sim_slippage_cents))

        # Stats
        self._sim_rejections = 0
        self._sim_missed_fills = 0
        self._total_order_latency_ms = 0.0
        self._order_count = 0

    async def place_order(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        market_condition_id: str,
    ) -> Order:
        """Place order with simulated network latency and random rejections."""
        start = time.monotonic()

        # Simulate API round-trip latency
        latency_ms = random.uniform(*self._order_latency)
        await asyncio.sleep(latency_ms / 1000.0)

        # Random rejection (simulates API errors, rate limits, etc.)
        if random.random() < self._rejection_rate:
            self._sim_rejections += 1
            order = Order(
                order_id=Order.generate_id(),
                token_id=token_id,
                side=Side(side),
                price=price,
                size=size,
                status=OrderStatus.CANCELLED,  # Rejected
                market_condition_id=market_condition_id,
            )
            actual_latency = (time.monotonic() - start) * 1000
            logger.info(
                "sim_order_rejected",
                order_id=order.order_id[:8],
                latency_ms=round(actual_latency, 1),
                total_rejections=self._sim_rejections,
            )
            return order

        # Order accepted — delegate to parent
        order = await super().place_order(
            token_id=token_id,
            side=side,
            price=price,
            size=size,
            market_condition_id=market_condition_id,
        )

        actual_latency = (time.monotonic() - start) * 1000
        self._total_order_latency_ms += actual_latency
        self._order_count += 1

        logger.info(
            "sim_order_placed",
            order_id=order.order_id[:8],
            latency_ms=round(actual_latency, 1),
            avg_latency_ms=round(self.avg_order_latency_ms, 1),
        )
        return order

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel order with simulated network latency."""
        latency_ms = random.uniform(*self._cancel_latency)
        await asyncio.sleep(latency_ms / 1000.0)
        return await super().cancel_order(order_id)

    async def _simulate_fill(
        self,
        order_id: str,
        trade_price: Decimal,
        available_size: Decimal,
        timestamp_ms: int,
    ):
        """Fill with higher slippage, reduced fill rate, and latency."""
        # Simulate fill detection delay
        latency_ms = random.uniform(*self._fill_latency)
        await asyncio.sleep(latency_ms / 1000.0)

        # Random fill-rate reduction (simulates queue position / thin liquidity)
        if random.random() > self._fill_rate:
            self._sim_missed_fills += 1
            logger.debug(
                "sim_fill_missed",
                order_id=order_id[:8],
                total_missed=self._sim_missed_fills,
            )
            return

        # Apply sim-live slippage (1.5¢ default) WITHOUT delegating to parent,
        # which would add another 0.5¢ on top. We handle the full fill here.
        order = self._orders.get(order_id)
        if not order or order.status != OrderStatus.LIVE:
            return

        if order.side == Side.BUY:
            fill_price = min(order.price, trade_price + self._slippage)
        else:
            fill_price = max(order.price, trade_price - self._slippage)

        # Determine fill size
        remaining = order.size - order.filled_size
        fill_size = min(remaining, available_size)

        if fill_size <= 0:
            return

        # Update order state
        order.filled_size += fill_size
        order.avg_fill_price = (
            (order.avg_fill_price * (order.filled_size - fill_size) + fill_price * fill_size)
            / order.filled_size
        )

        if order.filled_size >= order.size:
            order.status = OrderStatus.FILLED
            del self._orders[order_id]
        else:
            order.status = OrderStatus.PARTIALLY_FILLED

        # Update position
        await self._update_position(order, fill_price, fill_size, timestamp_ms)

        # Record trade (maker fee = 0)
        from data.models import Trade
        trade = Trade(
            trade_id=Trade.generate_id(),
            order_id=order.order_id,
            token_id=order.token_id,
            side=order.side,
            price=fill_price,
            size=fill_size,
            fee=Decimal("0"),
            pnl=Decimal("0"),
            timestamp=timestamp_ms / 1000.0,
            is_paper=self.is_paper,
            market_condition_id=order.market_condition_id,
        )

        await self.db.insert_trade(
            trade_id=trade.trade_id,
            order_id=trade.order_id,
            market_condition_id=trade.market_condition_id,
            token_id=trade.token_id,
            side=trade.side.value,
            outcome="",
            price=trade.price,
            size=trade.size,
            fee=trade.fee,
            realized_pnl=trade.pnl,
            is_paper=trade.is_paper,
            timestamp=trade.timestamp,
        )

        self._total_fills += 1

        logger.info(
            "sim_fill",
            order_id=order.order_id[:8],
            side=order.side.value,
            fill_price=float(fill_price),
            fill_size=float(fill_size),
            slippage_cents=float(self._slippage),
            status=order.status.value,
            bankroll=float(self.bankroll),
        )

        self._sync_to_state()

    @property
    def avg_order_latency_ms(self) -> float:
        if self._order_count == 0:
            return 0.0
        return self._total_order_latency_ms / self._order_count

    @property
    def sim_stats(self) -> dict:
        """Return simulation statistics for dashboard/logging."""
        return {
            "rejections": self._sim_rejections,
            "missed_fills": self._sim_missed_fills,
            "avg_order_latency_ms": round(self.avg_order_latency_ms, 1),
            "orders_placed": self._order_count,
        }
