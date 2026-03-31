import time
from decimal import Decimal
from typing import Optional

from data.models import Order, OrderStatus, Position, Side
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
        initial_bankroll: float = 0.0,
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

        self._bankroll = Decimal(str(initial_bankroll))
        self._positions: dict[str, Position] = {}
        self._realized_pnl = Decimal("0")
        self._last_sync_mono: float = 0.0
        self._last_trade_timestamp: Optional[int] = None  # For incremental get_trades

    async def initialize(self):
        """Initialize the CLOB client and derive API credentials."""
        try:
            from py_clob_client.client import ClobClient

            self._client = ClobClient(
                host=self._clob_url,
                key=self._private_key,
                chain_id=self._chain_id,
                signature_type=1,  # Polymarket proxy wallet (email/Google login)
                funder=self._funder_address,
            )

            # Derive API credentials (do this once at startup)
            creds = self._client.create_or_derive_api_creds()
            self._client.set_api_creds(creds)
            self._initialized = True

            # Immediately sync real balance from Polymarket (ignore positions until active market)
            await self.sync_state(active_token_ids=[])

            # Debug: write balance info to file
            with open("data/balance_debug.txt", "w") as f:
                f.write(f"initialized: {self._initialized}\n")
                f.write(f"bankroll: {self._bankroll}\n")
                f.write(f"positions: {len(self._positions)}\n")

            logger.info(
                "live_trader_initialized",
                usdc_balance=float(self._bankroll),
                positions=len(self._positions),
            )

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
        response = self._client.post_order(signed_order, orderType=OrderType.GTC)
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
        if not self._initialized:
            return {"usdc": self._bankroll, "positions": {}}

        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            result = self._client.get_balance_allowance(params)
            balance_str = result.get("balance", "0") if isinstance(result, dict) else "0"
            usdc = Decimal(balance_str) / Decimal("1000000")
            return {"usdc": usdc, "positions": {
                tid: pos.size for tid, pos in self._positions.items()
            }}
        except Exception as e:
            logger.error("live_get_balances_failed", error=str(e))
            return {"usdc": self._bankroll, "positions": {}}

    async def sync_state(self, active_token_ids: Optional[list[str]] = None) -> None:
        """Poll CLOB API for current positions and balance.

        Called every order manager cycle to keep bankroll, positions,
        and P&L in sync with the exchange.

        Uses get_balance_allowance for USDC balance (the SDK's correct method)
        and get_trades to reconstruct open positions from recent fills.
        """
        if not self._initialized:
            logger.info("sync_state_skipped_not_initialized")
            return

        now = time.monotonic()
        if now - self._last_sync_mono < 2.0:
            return
        self._last_sync_mono = now
        logger.info("sync_state_running")

        # Sync positions from trades (incremental: only fetch new trades)
        try:
            from py_clob_client.clob_types import TradeParams

            # Use after= parameter to fetch only trades since last sync
            if self._last_trade_timestamp is not None:
                trades = self._client.get_trades(after=self._last_trade_timestamp)
            else:
                trades = self._client.get_trades()

            if isinstance(trades, list) and trades:
                # Track latest timestamp for next incremental fetch
                for t in trades:
                    ts = t.get("match_time", t.get("timestamp", 0))
                    if isinstance(ts, (int, float)) and ts > (self._last_trade_timestamp or 0):
                        self._last_trade_timestamp = int(ts)

                # On first sync (no cached positions), build from full history
                # On subsequent syncs, apply incremental updates
                if not self._positions and self._last_trade_timestamp is None:
                    # Full rebuild
                    net: dict[str, dict] = {}
                    for t in trades:
                        token_id = t.get("asset_id", "")
                        if not token_id:
                            continue
                        side = t.get("side", "BUY")
                        size = Decimal(str(t.get("size", "0")))
                        price = Decimal(str(t.get("price", "0")))

                        if token_id not in net:
                            net[token_id] = {"size": Decimal("0"), "cost": Decimal("0"), "condition_id": ""}

                        if side == "BUY":
                            net[token_id]["size"] += size
                            net[token_id]["cost"] += size * price
                        else:
                            net[token_id]["size"] -= size
                            net[token_id]["cost"] -= size * price

                    new_positions: dict[str, Position] = {}
                    for token_id, data in net.items():
                        if active_token_ids is not None and token_id not in active_token_ids:
                            continue
                        if data["size"] > 0:
                            avg_price = data["cost"] / data["size"] if data["size"] > 0 else Decimal("0")
                            new_positions[token_id] = Position(
                                token_id=token_id,
                                outcome="",
                                size=data["size"],
                                avg_entry_price=avg_price,
                                market_condition_id=data["condition_id"],
                            )
                    self._positions = new_positions
                else:
                    # Incremental update: apply new trades to existing positions
                    for t in trades:
                        token_id = t.get("asset_id", "")
                        if not token_id:
                            continue
                        if active_token_ids is not None and token_id not in active_token_ids:
                            continue
                        side = t.get("side", "BUY")
                        size = Decimal(str(t.get("size", "0")))
                        price = Decimal(str(t.get("price", "0")))

                        existing = self._positions.get(token_id)
                        if side == "BUY":
                            if existing:
                                total_size = existing.size + size
                                existing.avg_entry_price = (
                                    (existing.avg_entry_price * existing.size + price * size) / total_size
                                )
                                existing.size = total_size
                            else:
                                self._positions[token_id] = Position(
                                    token_id=token_id,
                                    outcome="",
                                    size=size,
                                    avg_entry_price=price,
                                    market_condition_id="",
                                )
                        else:  # SELL
                            if existing:
                                existing.size -= size
                                if existing.size <= 0:
                                    del self._positions[token_id]

        except Exception as e:
            logger.warning("live_sync_positions_failed", error=str(e))

        # Sync USDC balance
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
            result = self._client.get_balance_allowance(params)
            # Debug: append raw response to file
            with open("data/balance_debug.txt", "a") as f:
                f.write(f"raw_result_type: {type(result).__name__}\n")
                f.write(f"raw_result: {result}\n")
            logger.info(
                "balance_api_raw_response",
                result_type=type(result).__name__,
                result=str(result),
            )
            balance_str = result.get("balance", "0") if isinstance(result, dict) else "0"
            self._bankroll = Decimal(balance_str) / Decimal("1000000")
        except Exception as e:
            logger.warning("live_sync_balance_failed", error=str(e))

        logger.debug(
            "live_state_synced",
            bankroll=float(self._bankroll),
            positions=len(self._positions),
        )

    @property
    def current_bankroll(self) -> Decimal:
        return self._bankroll

    @property
    def current_positions(self) -> dict[str, Position]:
        return self._positions.copy()

    @property
    def current_pnl(self) -> Decimal:
        return self._realized_pnl

    async def resolve_market(
        self,
        winning_token_id: str,
        losing_token_id: str,
    ):
        """Handle market resolution for live trading.

        On Polymarket, resolved markets are settled automatically by the
        protocol — winning shares redeem for $1, losing for $0.
        Sync state afterward to pick up updated balance.
        """
        logger.info(
            "live_market_resolved",
            winning_token=winning_token_id[:8],
            losing_token=losing_token_id[:8],
        )
        self._last_sync_mono = 0.0
        await self.sync_state()

    def clear_fee_cache(self):
        """Clear fee rate cache (needed on market transition)."""
        self._fee_rate_cache.clear()
