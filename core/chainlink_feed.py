import asyncio
import json
import time
from decimal import Decimal
from typing import Callable, Awaitable

import websockets
from websockets.exceptions import ConnectionClosed

from monitoring.logger import get_logger

logger = get_logger("chainlink_feed")


class ChainlinkFeed:
    """Polymarket RTDS WebSocket consumer for BTC/USD price feeds.

    Subscribes to BOTH topics on the same RTDS connection:
    - crypto_prices_chainlink: Chainlink oracle reports (infrequent,
      only on ~0.1% price deviation — can be stale for minutes)
    - crypto_prices: Binance-sourced prices via RTDS (frequent,
      updates every few seconds)

    The Chainlink price is authoritative for resolution.
    The RTDS Binance price is used as a real-time proxy when Chainlink
    is stale, and for anchor capture at market boundaries.
    """

    PING_INTERVAL = 5  # RTDS requires ping every 5 seconds

    def __init__(self, ws_url: str):
        self.ws_url = ws_url
        self._running = False
        self._ws = None

    async def start(
        self,
        callback: Callable[[Decimal, int], Awaitable[None]],
        binance_rtds_callback: Callable[[Decimal, int], Awaitable[None]] = None,
        anchor_batch_callback: Callable[[list[dict]], Awaitable[None]] = None,
    ):
        """Start consuming price feeds from RTDS.

        Args:
            callback: Called with (price, timestamp_ms) on each Chainlink update.
            binance_rtds_callback: Called with (price, timestamp_ms) on each
                RTDS Binance update. If None, Binance RTDS data is ignored.
            anchor_batch_callback: Called with raw batch data array from
                `crypto_prices` btc/usd topic for anchor price extraction
                at 5-minute boundaries.
        """
        self._running = True
        backoff = 1.0

        while self._running:
            try:
                logger.info("chainlink_connecting", url=self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=None,  # We handle pings manually
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    backoff = 1.0
                    logger.info("chainlink_connected")

                    # Subscribe to Chainlink oracle + RTDS price feeds
                    # crypto_prices btc/usd = Chainlink interpolated (anchor source)
                    # crypto_prices btcusdt = Binance via RTDS (real-time proxy)
                    # crypto_prices_chainlink = raw Chainlink oracle (infrequent)
                    subscriptions = [
                        {
                            "topic": "crypto_prices_chainlink",
                            "type": "*",
                            "filters": json.dumps({"symbol": "btc/usd"}),
                        },
                        {
                            "topic": "crypto_prices",
                            "type": "*",
                            "filters": json.dumps({"symbol": "btc/usd"}),
                        },
                        {
                            "topic": "crypto_prices",
                            "type": "*",
                            "filters": json.dumps({"symbol": "btcusdt"}),
                        },
                    ]
                    sub_msg = {
                        "action": "subscribe",
                        "subscriptions": subscriptions,
                    }
                    await ws.send(json.dumps(sub_msg))
                    logger.info(
                        "rtds_subscribed",
                        topics=["crypto_prices_chainlink", "crypto_prices"],
                    )

                    # Start ping task
                    ping_task = asyncio.create_task(self._ping_loop(ws))

                    try:
                        async for raw_msg in ws:
                            if not self._running:
                                break

                            # Skip empty messages (connection acks, pongs)
                            if not raw_msg or not raw_msg.strip():
                                continue

                            try:
                                msg = json.loads(raw_msg)
                                await self._handle_message(
                                    msg, callback, binance_rtds_callback,
                                    anchor_batch_callback,
                                )
                            except json.JSONDecodeError:
                                pass  # Non-JSON messages (PONG, acks) are normal
                            except (KeyError, ValueError) as e:
                                logger.warning(
                                    "chainlink_parse_error", error=str(e)
                                )
                    finally:
                        ping_task.cancel()

            except ConnectionClosed as e:
                logger.warning(
                    "chainlink_disconnected",
                    code=e.code,
                    reason=str(e.reason),
                )
            except Exception as e:
                logger.error("chainlink_error", error=str(e))

            if self._running:
                logger.info("chainlink_reconnecting", backoff_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _ping_loop(self, ws):
        try:
            while True:
                await asyncio.sleep(self.PING_INTERVAL)
                await ws.send("PING")
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.warning("chainlink_ping_error", error=str(e))

    async def _handle_message(
        self,
        msg: dict,
        chainlink_callback: Callable[[Decimal, int], Awaitable[None]],
        binance_rtds_callback: Callable[[Decimal, int], Awaitable[None]] = None,
        anchor_batch_callback: Callable[[list[dict]], Awaitable[None]] = None,
    ):
        if not isinstance(msg, dict):
            return

        # Route by topic
        topic = msg.get("topic", "")
        is_chainlink = "chainlink" in topic
        is_crypto_prices = topic == "crypto_prices"

        payload = msg.get("payload", msg)
        symbol = payload.get("symbol", "")

        # Determine which callback to use based on topic + symbol
        if is_chainlink:
            # Raw Chainlink oracle reports (infrequent, ~0.1% deviation)
            callback = chainlink_callback
        elif is_crypto_prices and symbol == "btc/usd":
            # Chainlink Data Streams interpolated feed (frequent, every second).
            # Contains the "Price to Beat" at 5-min boundaries.
            # Do NOT route to chainlink_callback — that would overwrite the
            # raw oracle price with interpolated values (~$10-20 different).
            # Anchor extraction happens via anchor_batch_callback below.
            # Use binance_rtds_callback for price display (it's RTDS data).
            if binance_rtds_callback:
                callback = binance_rtds_callback
            else:
                # Even without callback, still process for anchor extraction
                callback = None
        elif is_crypto_prices and symbol == "btcusdt":
            # RTDS Binance feed
            if binance_rtds_callback:
                callback = binance_rtds_callback
            else:
                return
        elif is_crypto_prices:
            return  # Unknown symbol
        else:
            # Not a price message — check for acks, pongs, etc.
            msg_type = msg.get("type", "")
            if msg_type not in ("subscription_ack", "pong", "heartbeat", ""):
                logger.warning(
                    "rtds_unhandled_msg",
                    type=msg_type,
                    topic=topic,
                    keys=list(msg.keys()),
                )
            return

        # Format 1: payload has direct value & timestamp
        value = payload.get("value") or payload.get("price")
        if value is not None:
            price = Decimal(str(value))
            timestamp = payload.get("timestamp")
            ts_ms = self._parse_timestamp(timestamp)
            if is_chainlink:
                logger.info(
                    "chainlink_tick",
                    price=float(price),
                )
            if callback:
                await callback(price, ts_ms)
            return

        # Format 2: payload.data is a list of {timestamp, value} entries
        # This is the primary format for crypto_prices topic
        data = payload.get("data", None)
        if isinstance(data, list) and data:
            # Extract anchor price at 5-min boundaries from btc/usd batch
            if anchor_batch_callback and symbol == "btc/usd":
                await anchor_batch_callback(data)

            # Only forward the LATEST entry as the price tick
            if callback:
                last_entry = data[-1]
                if isinstance(last_entry, dict) and "value" in last_entry:
                    price = Decimal(str(last_entry["value"]))
                    ts_ms = self._parse_timestamp(last_entry.get("timestamp"))
                    await callback(price, ts_ms)
            return

        # Format 3: payload.data is a single dict
        if isinstance(data, dict) and "value" in data:
            price = Decimal(str(data["value"]))
            ts_ms = self._parse_timestamp(data.get("timestamp"))
            if callback:
                await callback(price, ts_ms)
            return

    @staticmethod
    def _parse_timestamp(timestamp) -> int:
        if timestamp is None:
            return int(time.time() * 1000)
        ts = float(timestamp)
        # If already in ms (>1e12), use directly; otherwise convert s to ms
        return int(ts) if ts > 1e12 else int(ts * 1000)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        logger.info("chainlink_stopped")
