from abc import ABC, abstractmethod
from decimal import Decimal

from data.models import Order


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
