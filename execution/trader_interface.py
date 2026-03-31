from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Optional

from data.models import Order, Position


class TraderInterface(ABC):
    """Abstract interface for order execution — both paper and live implement this."""

    @abstractmethod
    async def place_order(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        market_condition_id: str,
    ) -> Order:
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        ...

    @abstractmethod
    async def cancel_all_orders(self) -> int:
        """Cancel all open orders. Returns count of cancelled orders."""
        ...

    @abstractmethod
    async def get_open_orders(self) -> list[Order]:
        ...

    @abstractmethod
    async def get_balances(self) -> dict[str, Decimal]:
        """Return {'usdc': Decimal, 'token_positions': {...}}."""
        ...

    @property
    def current_bankroll(self) -> Decimal:
        """Current available bankroll. Override in subclasses."""
        return Decimal("0")

    @property
    def current_positions(self) -> dict[str, Position]:
        """Current open positions. Override in subclasses."""
        return {}

    @property
    def current_pnl(self) -> Decimal:
        """Session realized P&L. Override in subclasses."""
        return Decimal("0")

    async def sync_state(self, active_token_ids: Optional[list[str]] = None) -> None:
        """Refresh local state from the exchange (positions, bankroll).

        No-op for paper/sim-live (state is tracked in-memory).
        LiveTrader overrides to poll the CLOB API.
        """
        pass
