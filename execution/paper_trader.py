import time
from decimal import Decimal
from typing import Optional, TYPE_CHECKING

from data.models import Order, OrderStatus, Side, Position, Trade
from data.database import Database
from execution.trader_interface import TraderInterface
from monitoring.logger import get_logger

if TYPE_CHECKING:
    from core.state import SharedState

logger = get_logger("paper_trader")

# Simulated slippage per fill
SLIPPAGE_CENTS = Decimal("0.005")


class PaperTrader(TraderInterface):
    """Simulated order execution engine using real market data."""

    def __init__(self, initial_bankroll: float, database: Database, is_paper: bool = True):
        self.bankroll = Decimal(str(initial_bankroll))
        self.db = database
        self.is_paper = is_paper
        self._shared_state: Optional["SharedState"] = None

        # In-memory state
        self._orders: dict[str, Order] = {}  # order_id -> Order
        self._positions: dict[str, Position] = {}  # token_id -> Position
        self._realized_pnl = Decimal("0")
        self._total_fills: int = 0        # All fills (entries + exits) for logging
        self._winning_fills: int = 0      # Legacy — kept for compat
        self._total_trades: int = 0       # Counted only at position CLOSE
        self._winning_trades: int = 0     # Counted only when close PnL > 0

    def set_shared_state(self, state: "SharedState"):
        """Connect to SharedState for real-time dashboard sync."""
        self._shared_state = state
        self._sync_to_state()

    def _sync_to_state(self):
        """Push current paper trader state to SharedState for dashboard display."""
        if self._shared_state is None:
            return
        self._shared_state.open_positions = self._positions.copy()
        self._shared_state.paper_bankroll = self.bankroll
        self._shared_state.session_pnl = self._realized_pnl
        self._shared_state.total_trades = self._total_trades
        self._shared_state.winning_trades = self._winning_trades

    def _record_recent_trade(self, token_id: str, pnl: Decimal, cost: float, side: str = ""):
        """Record a completed trade for the dashboard's recent trades display."""
        if self._shared_state is None:
            return
        if not side:
            market = self._shared_state.current_market
            if market:
                if token_id == market.token_id_up:
                    side = "UP"
                elif token_id == market.token_id_down:
                    side = "DOWN"
                else:
                    side = "?"
            else:
                side = "?"
        self._shared_state.recent_trades.append({
            "side": side,
            "pnl": float(pnl),
            "cost": cost,
            "won": float(pnl) > 0,
            "time": time.time(),
        })
        if len(self._shared_state.recent_trades) > 5:
            self._shared_state.recent_trades = self._shared_state.recent_trades[-5:]

    async def place_order(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        market_condition_id: str,
    ) -> Order:
        order = Order(
            order_id=Order.generate_id(),
            token_id=token_id,
            side=Side(side),
            price=price,
            size=size,
            status=OrderStatus.LIVE,
            market_condition_id=market_condition_id,
        )
        self._orders[order.order_id] = order

        logger.info(
            "paper_order_placed",
            order_id=order.order_id[:8],
            side=side,
            price=float(price),
            size=float(size),
            token_id=token_id[:8],
        )
        return order

    async def cancel_order(self, order_id: str) -> bool:
        order = self._orders.get(order_id)
        if order and order.status == OrderStatus.LIVE:
            order.status = OrderStatus.CANCELLED
            del self._orders[order_id]
            logger.debug("paper_order_cancelled", order_id=order_id[:8])
            return True
        return False

    async def cancel_all_orders(self) -> int:
        count = 0
        to_cancel = [
            oid for oid, o in self._orders.items()
            if o.status == OrderStatus.LIVE
        ]
        for oid in to_cancel:
            self._orders[oid].status = OrderStatus.CANCELLED
            del self._orders[oid]
            count += 1

        if count > 0:
            logger.info("paper_cancelled_all", count=count)
        return count

    async def get_open_orders(self) -> list[Order]:
        return [o for o in self._orders.values() if o.status == OrderStatus.LIVE]

    async def get_balances(self) -> dict[str, Decimal]:
        positions = {}
        for token_id, pos in self._positions.items():
            positions[token_id] = pos.size
        return {
            "usdc": self.bankroll,
            "positions": positions,
        }

    async def on_trade_event(
        self,
        asset_id: str,
        trade_price: Decimal,
        trade_size: Decimal,
        timestamp_ms: int,
    ):
        """Called when a real trade occurs on Polymarket — check if any paper orders fill."""
        to_fill: list[str] = []

        for order_id, order in self._orders.items():
            if order.status != OrderStatus.LIVE:
                continue
            if order.token_id != asset_id:
                continue

            # Check if this trade would fill our order
            if order.side == Side.BUY and trade_price <= order.price:
                to_fill.append(order_id)
            elif order.side == Side.SELL and trade_price >= order.price:
                to_fill.append(order_id)

        for order_id in to_fill:
            await self._simulate_fill(order_id, trade_price, trade_size, timestamp_ms)

    async def on_immediate_fill_check(
        self,
        order: Order,
        best_ask: Optional[Decimal],
        best_bid: Optional[Decimal],
    ):
        """Check if a newly placed order would immediately fill against real orderbook."""
        if order.side == Side.BUY and best_ask is not None:
            if order.price >= best_ask:
                await self._simulate_fill(
                    order.order_id,
                    best_ask,
                    order.size,
                    int(time.time() * 1000),
                )
        elif order.side == Side.SELL and best_bid is not None:
            if order.price <= best_bid:
                await self._simulate_fill(
                    order.order_id,
                    best_bid,
                    order.size,
                    int(time.time() * 1000),
                )

    async def _simulate_fill(
        self,
        order_id: str,
        trade_price: Decimal,
        available_size: Decimal,
        timestamp_ms: int,
    ):
        order = self._orders.get(order_id)
        if not order or order.status != OrderStatus.LIVE:
            return

        # Apply slippage — 0.5 cents worse than limit
        if order.side == Side.BUY:
            fill_price = min(order.price, trade_price + SLIPPAGE_CENTS)
        else:
            fill_price = max(order.price, trade_price - SLIPPAGE_CENTS)

        # Determine fill size
        remaining = order.size - order.filled_size
        fill_size = min(remaining, available_size)

        if fill_size <= 0:
            return

        # Update order
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

        # Maker fee is zero
        fee = Decimal("0")

        # Record trade
        trade = Trade(
            trade_id=Trade.generate_id(),
            order_id=order.order_id,
            token_id=order.token_id,
            side=order.side,
            price=fill_price,
            size=fill_size,
            fee=fee,
            pnl=Decimal("0"),  # Realized PnL calculated on close
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
            "paper_fill",
            order_id=order.order_id[:8],
            side=order.side.value,
            fill_price=float(fill_price),
            fill_size=float(fill_size),
            status=order.status.value,
            bankroll=float(self.bankroll),
        )

        # Sync to SharedState so dashboard updates in real time
        self._sync_to_state()

    async def _update_position(
        self,
        order: Order,
        fill_price: Decimal,
        fill_size: Decimal,
        timestamp_ms: int,
    ):
        """Update or create position based on fill."""
        token_id = order.token_id
        existing = self._positions.get(token_id)

        if order.side == Side.BUY:
            if existing:
                total_size = existing.size + fill_size
                existing.avg_entry_price = (
                    (existing.avg_entry_price * existing.size + fill_price * fill_size)
                    / total_size
                )
                existing.size = total_size
                await self.db.update_position_size(
                    token_id, existing.size, existing.avg_entry_price,
                )
            else:
                pos = Position(
                    token_id=token_id,
                    outcome="",
                    size=fill_size,
                    avg_entry_price=fill_price,
                    market_condition_id=order.market_condition_id,
                )
                self._positions[token_id] = pos

                await self.db.insert_position(
                    token_id=token_id,
                    market_condition_id=order.market_condition_id,
                    outcome="",
                    size=fill_size,
                    avg_entry_price=fill_price,
                    is_paper=self.is_paper,
                    opened_at=timestamp_ms / 1000.0,
                )

            self.bankroll -= fill_price * fill_size

        elif order.side == Side.SELL:
            if existing and existing.size > 0:
                # Close or reduce position
                close_size = min(existing.size, fill_size)
                pnl = (fill_price - existing.avg_entry_price) * close_size
                self._realized_pnl += pnl
                self.bankroll += fill_price * close_size

                existing.size -= close_size
                if existing.size <= 0:
                    self._total_trades += 1
                    if pnl > 0:
                        self._winning_trades += 1
                    self._record_recent_trade(token_id, pnl, float(existing.avg_entry_price * close_size))
                    del self._positions[token_id]

                    await self.db.close_position(
                        token_id=token_id,
                        close_price=fill_price,
                        pnl=pnl,
                        closed_at=timestamp_ms / 1000.0,
                    )

    async def resolve_market(
        self,
        winning_token_id: str,
        losing_token_id: str,
        outcome: str = "",
    ):
        """Handle market resolution — winning shares pay $1, losing pay $0.

        Uses deferred DB writes (no commit per position) to avoid blocking
        the event loop at cycle boundaries. The periodic flush loop commits.
        """
        now = time.time()

        # Winning positions
        if winning_token_id in self._positions:
            pos = self._positions[winning_token_id]
            payout = pos.size * Decimal("1.0")
            pnl = payout - (pos.avg_entry_price * pos.size)
            self.bankroll += payout
            self._realized_pnl += pnl

            await self.db.close_position_deferred(
                token_id=winning_token_id,
                close_price=Decimal("1.0"),
                pnl=pnl,
                closed_at=now,
            )
            cost = float(pos.avg_entry_price * pos.size)
            win_side = outcome if outcome else "UP"
            self._record_recent_trade(winning_token_id, pnl, cost, side=win_side)
            del self._positions[winning_token_id]

            self._total_trades += 1
            self._winning_trades += 1

            logger.info(
                "paper_resolution_win",
                token_id=winning_token_id[:8],
                payout=float(payout),
                pnl=float(pnl),
            )

        # Losing positions
        if losing_token_id in self._positions:
            pos = self._positions[losing_token_id]
            pnl = -(pos.avg_entry_price * pos.size)
            self._realized_pnl += pnl

            await self.db.close_position_deferred(
                token_id=losing_token_id,
                close_price=Decimal("0.0"),
                pnl=pnl,
                closed_at=now,
            )
            cost = float(pos.avg_entry_price * pos.size)
            lose_side = "DOWN" if outcome == "UP" else "UP"
            self._record_recent_trade(losing_token_id, pnl, cost, side=lose_side)
            del self._positions[losing_token_id]

            self._total_trades += 1

            logger.info(
                "paper_resolution_loss",
                token_id=losing_token_id[:8],
                loss=float(pnl),
            )

        # Sync after resolution
        self._sync_to_state()

    @property
    def total_pnl(self) -> Decimal:
        return self._realized_pnl

    @property
    def equity(self) -> Decimal:
        """Current equity = bankroll + cost basis of open positions.

        Uses cost basis (size × avg_entry_price), not mark-to-market,
        because binary markets resolve to $1 or $0. This represents
        total capital deployed, which is the correct risk measure.
        For true mark-to-market, each position would need current_market_price.
        """
        unrealized = sum(
            pos.size * pos.avg_entry_price
            for pos in self._positions.values()
        )
        return self.bankroll + unrealized

    @property
    def positions(self) -> dict[str, Position]:
        return self._positions.copy()

    @property
    def current_bankroll(self) -> Decimal:
        return self.bankroll

    @property
    def current_positions(self) -> dict[str, Position]:
        return self._positions.copy()

    @property
    def current_pnl(self) -> Decimal:
        return self._realized_pnl
