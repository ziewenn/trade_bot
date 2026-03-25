import time
from decimal import Decimal
from typing import Optional

from data.models import Order, OrderStatus, Side
from execution.trader_interface import TraderInterface
from monitoring.logger import get_logger

logger = get_logger("live_trader")


class LiveTrader(TraderInterface):
    """Real Polymarket CLOB order execution via py-clob-client AsyncClobClient.

    Safety gates:
    1. trading_mode must be "live"
    2. POLYMARKET_PRIVATE_KEY must be set
    3. User must have typed "CONFIRM LIVE TRADING" at startup
    """

    def __init__(
        self,
        private_key: str,
        funder_address: str,
        clob_url: str = "https://clob.polymarket.com",
        chain_id: int = 137,
    ):
        self._clob_url = clob_url
        self._private_key = private_key
        self._funder_address = funder_address
        self._chain_id = chain_id
        self._client = None
        self._fee_rate_cache: dict[str, int] = {}  # token_id -> fee_rate_bps
        self._initialized = False

    async def initialize(self):
        """Initialize the CLOB client and derive API credentials."""
        try:
            from py_clob_client.client import ClobClient

            self._client = ClobClient(
                host=self._clob_url,
                key=self._private_key,
                chain_id=self._chain_id,
                signature_type=0,  # EOA wallet
                funder=self._funder_address,
            )

            # Derive API credentials (do this once at startup)
            creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(creds)
            self._initialized = True
            logger.info("live_trader_initialized")

        except ImportError:
            raise RuntimeError(
                "py-clob-client is required for live trading. "
                "Install with: pip install py-clob-client"
            )
        except Exception as e:
            logger.error("live_trader_init_failed", error=str(e))
            raise

    async def _get_fee_rate(self, token_id: str) -> int:
        """Fetch fee rate BPS for a token. Cached per token."""
        if token_id not in self._fee_rate_cache:
            rate = self._client.get_fee_rate_bps(token_id=token_id)
            self._fee_rate_cache[token_id] = rate
            logger.debug("fee_rate_fetched", token_id=token_id[:8], rate=rate)
        return self._fee_rate_cache[token_id]

    async def place_order(
        self,
        token_id: str,
        side: str,
        price: Decimal,
        size: Decimal,
        market_condition_id: str,
    ) -> Order:
        if not self._initialized:
            raise RuntimeError("LiveTrader not initialized. Call initialize() first.")

        from py_clob_client.clob_types import OrderArgs, OrderType

        fee_rate = await self._get_fee_rate(token_id)

        order_args = OrderArgs(
            price=float(price),
            size=float(size),
            side=side,
            token_id=token_id,
            fee_rate_bps=fee_rate,
        )

        start = time.monotonic()
        signed_order = self._client.create_order(order_args)
        response = self._client.post_order(signed_order, order_type=OrderType.GTC)
        latency_ms = (time.monotonic() - start) * 1000

        order_id = response.get("orderID", response.get("order_id", ""))
        status_str = response.get("status", "live")

        logger.info(
            "live_order_placed",
            order_id=order_id[:8] if order_id else "unknown",
            side=side,
            price=float(price),
            size=float(size),
            latency_ms=round(latency_ms, 1),
        )

        return Order(
            order_id=order_id,
            token_id=token_id,
            side=Side(side),
            price=price,
            size=size,
            status=OrderStatus.LIVE if status_str == "live" else OrderStatus.REJECTED,
            fee_rate_bps=fee_rate,
            market_condition_id=market_condition_id,
        )

    async def cancel_order(self, order_id: str) -> bool:
        if not self._initialized:
            return False

        try:
            self._client.cancel(order_id=order_id)
            logger.debug("live_order_cancelled", order_id=order_id[:8])
            return True
        except Exception as e:
            logger.error("live_cancel_failed", order_id=order_id[:8], error=str(e))
            return False

    async def cancel_all_orders(self) -> int:
        if not self._initialized:
            return 0

        try:
            self._client.cancel_all()
            logger.info("live_cancelled_all")
            return -1  # SDK doesn't return count
        except Exception as e:
            logger.error("live_cancel_all_failed", error=str(e))
            return 0

    async def get_open_orders(self) -> list[Order]:
        if not self._initialized:
            return []

        try:
            response = self._client.get_orders(params={"status": "live"})
            orders = []
            for o in response:
                orders.append(Order(
                    order_id=o.get("id", ""),
                    token_id=o.get("asset_id", ""),
                    side=Side(o.get("side", "BUY")),
                    price=Decimal(str(o.get("price", "0"))),
                    size=Decimal(str(o.get("original_size", "0"))),
                    status=OrderStatus.LIVE,
                ))
            return orders
        except Exception as e:
            logger.error("live_get_orders_failed", error=str(e))
            return []

    async def get_balances(self) -> dict[str, Decimal]:
        # py-clob-client doesn't have a direct balance check
        # The user should check via Polymarket UI or Polygon chain
        return {"usdc": Decimal("0"), "positions": {}}

    async def resolve_market(
        self,
        winning_token_id: str,
        losing_token_id: str,
    ):
        """Handle market resolution for live trading.

        On Polymarket, resolved markets are settled automatically by the
        protocol — winning shares redeem for $1, losing for $0. No action
        needed from the trader.
        """
        logger.info(
            "live_market_resolved",
            winning_token=winning_token_id[:8],
            losing_token=losing_token_id[:8],
        )

    def clear_fee_cache(self):
        """Clear fee rate cache (needed on market transition)."""
        self._fee_rate_cache.clear()
