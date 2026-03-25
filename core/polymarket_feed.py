import asyncio
import json
import time
from decimal import Decimal
from typing import Callable, Awaitable, Optional

import websockets
from websockets.exceptions import ConnectionClosed

from data.models import OrderBook, OrderBookLevel
from monitoring.logger import get_logger

logger = get_logger("polymarket_feed")


class PolymarketFeed:
    """Polymarket CLOB WebSocket consumer with silent-freeze watchdog."""

    HEARTBEAT_INTERVAL = 10  # Send {} every 10 seconds
    FREEZE_TIMEOUT = 15  # Force reconnect if no message for 15s
    FREEZE_CHECK_INTERVAL = 5  # Check every 5 seconds

    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self._running = False
        self._ws = None
        self._token_ids: list[str] = []
        self._last_message_time: float = 0.0

        # Local orderbook state: asset_id -> {price_str -> OrderBookLevel}
        self._bids: dict[str, dict[str, OrderBookLevel]] = {}
        self._asks: dict[str, dict[str, OrderBookLevel]] = {}

    async def start(
        self,
        token_ids: list[str],
        on_book_update: Callable[[OrderBook, str], Awaitable[None]],
        on_trade: Optional[Callable[[str, Decimal, Decimal, int], Awaitable[None]]] = None,
    ):
        """Start consuming Polymarket WebSocket.

        Args:
            token_ids: List of token IDs to subscribe to.
            on_book_update: Callback(orderbook, asset_id) on book changes.
            on_trade: Callback(asset_id, price, size, timestamp_ms) on trades.
        """
        self._running = True
        self._token_ids = token_ids
        backoff = 1.0

        while self._running:
            try:
                logger.info("polymarket_connecting", url=self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # We handle heartbeats manually
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._last_message_time = time.monotonic()
                    backoff = 1.0
                    logger.info("polymarket_connected")

                    # Subscribe
                    await self._subscribe(ws, self._token_ids)

                    # Start heartbeat and freeze watchdog
                    heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws))
                    watchdog_task = asyncio.create_task(
                        self._freeze_watchdog(ws)
                    )

                    try:
                        async for raw_msg in ws:
                            if not self._running:
                                break

                            self._last_message_time = time.monotonic()

                            try:
                                msgs = json.loads(raw_msg)
                                # Can be a single message or a list
                                if isinstance(msgs, dict):
                                    msgs = [msgs]

                                for msg in msgs:
                                    await self._handle_message(
                                        msg, on_book_update, on_trade
                                    )
                            except Exception as e:
                                logger.warning(
                                    "polymarket_parse_error",
                                    error=str(e),
                                    error_type=type(e).__name__,
                                )
                    finally:
                        heartbeat_task.cancel()
                        watchdog_task.cancel()

            except ConnectionClosed as e:
                logger.warning(
                    "polymarket_disconnected",
                    code=e.code,
                    reason=str(e.reason),
                )
            except Exception as e:
                logger.error("polymarket_error", error=str(e))

            # Clear local book state on reconnect
            self._bids.clear()
            self._asks.clear()

            if self._running:
                logger.info("polymarket_reconnecting", backoff_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _subscribe(self, ws, token_ids: list[str]):
        sub_msg = {
            "assets_ids": token_ids,
            "type": "market",
            "custom_feature_enabled": True,
        }
        await ws.send(json.dumps(sub_msg))
        logger.info("polymarket_subscribed", token_ids=token_ids)

    async def update_subscription(self, new_token_ids: list[str]):
        """Re-subscribe with new token IDs by forcing a reconnect.

        The Polymarket WS doesn't reliably support dynamic resubscription,
        so we close the connection and let the reconnect loop handle it
        with the new token IDs.
        """
        # Update token_ids first — the reconnect loop uses these
        self._token_ids = new_token_ids

        # Clear local orderbook state
        self._bids.clear()
        self._asks.clear()

        # Force close the WS — the start() reconnect loop will
        # automatically reconnect and subscribe with new token_ids
        if self._ws:
            try:
                logger.info(
                    "polymarket_reconnecting_for_new_market",
                    new_tokens=new_token_ids,
                )
                await self._ws.close()
            except Exception as e:
                logger.warning(
                    "polymarket_close_failed",
                    error=str(e),
                )

    async def _heartbeat_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(self.HEARTBEAT_INTERVAL)
                await ws.send("{}")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("polymarket_heartbeat_error", error=str(e))

    async def _freeze_watchdog(self, ws):
        """Detect silent freeze and force reconnect."""
        try:
            while True:
                await asyncio.sleep(self.FREEZE_CHECK_INTERVAL)
                elapsed = time.monotonic() - self._last_message_time
                if elapsed > self.FREEZE_TIMEOUT:
                    logger.warning(
                        "polymarket_silent_freeze_detected",
                        seconds_since_last_msg=elapsed,
                    )
                    await ws.close()
                    return
        except asyncio.CancelledError:
            pass

    async def _handle_message(
        self,
        msg: dict,
        on_book_update: Callable[[OrderBook, str], Awaitable[None]],
        on_trade: Optional[Callable],
    ):
        event_type = msg.get("event_type", "")
        asset_id = msg.get("asset_id", "")

        if event_type == "book":
            # Full orderbook snapshot
            bids = {}
            asks = {}
            for entry in msg.get("bids", []):
                price = str(entry.get("price", entry[0]) if isinstance(entry, list) else entry.get("price", "0"))
                size = str(entry.get("size", entry[1]) if isinstance(entry, list) else entry.get("size", "0"))
                bids[price] = OrderBookLevel(
                    price=Decimal(price), size=Decimal(size)
                )
            for entry in msg.get("asks", []):
                price = str(entry.get("price", entry[0]) if isinstance(entry, list) else entry.get("price", "0"))
                size = str(entry.get("size", entry[1]) if isinstance(entry, list) else entry.get("size", "0"))
                asks[price] = OrderBookLevel(
                    price=Decimal(price), size=Decimal(size)
                )

            self._bids[asset_id] = bids
            self._asks[asset_id] = asks
            await on_book_update(self._build_orderbook(asset_id), asset_id)

        elif event_type == "price_change":
            # Incremental updates — can be wrapped in price_changes array
            changes = msg.get("price_changes", [msg])

            updated_assets = set()
            for change in changes:
                change_asset_id = change.get("asset_id", asset_id)
                price_str = str(change.get("price", "0"))
                size_str = str(change.get("size", "0"))
                side = change.get("side", "")

                if change_asset_id not in self._bids:
                    self._bids[change_asset_id] = {}
                if change_asset_id not in self._asks:
                    self._asks[change_asset_id] = {}

                book_side = (
                    self._bids[change_asset_id]
                    if side == "BUY"
                    else self._asks[change_asset_id]
                )

                if Decimal(size_str) == 0:
                    book_side.pop(price_str, None)
                else:
                    book_side[price_str] = OrderBookLevel(
                        price=Decimal(price_str), size=Decimal(size_str)
                    )
                updated_assets.add(change_asset_id)

            for aid in updated_assets:
                await on_book_update(self._build_orderbook(aid), aid)

        elif event_type == "last_trade_price":
            if on_trade:
                price = Decimal(str(msg.get("price", "0")))
                size = Decimal(str(msg.get("size", "0")))
                ts = msg.get("timestamp", int(time.time() * 1000))
                await on_trade(asset_id, price, size, ts)

    def _build_orderbook(self, asset_id: str) -> OrderBook:
        bids = sorted(
            self._bids.get(asset_id, {}).values(),
            key=lambda x: x.price,
            reverse=True,
        )
        asks = sorted(
            self._asks.get(asset_id, {}).values(),
            key=lambda x: x.price,
        )
        return OrderBook(
            asset_id=asset_id,
            bids=bids,
            asks=asks,
            timestamp_ms=int(time.time() * 1000),
        )

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        logger.info("polymarket_stopped")
