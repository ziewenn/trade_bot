import asyncio
import json
import time
from decimal import Decimal
from typing import Callable, Awaitable

import websockets
from websockets.exceptions import ConnectionClosed

from data.models import BinanceTick
from monitoring.logger import get_logger

logger = get_logger("binance_feed")


class BinanceFeed:
    def __init__(self, ws_url: str, lookback_ms: int = 5000):
        self.ws_url = ws_url
        self.lookback_ms = lookback_ms
        self._running = False
        self._ws = None
        self._connection_start: float = 0.0

    async def start(self, callback: Callable[[BinanceTick], Awaitable[None]]):
        self._running = True
        backoff = 1.0
        max_backoff = 30.0

        while self._running:
            try:
                logger.info("binance_connecting", url=self.ws_url)
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=60,
                    close_timeout=5,
                ) as ws:
                    self._ws = ws
                    self._connection_start = time.monotonic()
                    backoff = 1.0  # Reset on successful connect
                    logger.info("binance_connected")

                    async for raw_msg in ws:
                        if not self._running:
                            break

                        # Proactive reconnect before Binance's 24h limit
                        elapsed = time.monotonic() - self._connection_start
                        if elapsed > 23 * 3600 + 50 * 60:  # 23h50m
                            logger.info("binance_proactive_reconnect")
                            break

                        try:
                            msg = json.loads(raw_msg)
                            tick = BinanceTick(
                                price=Decimal(msg["p"]),
                                quantity=Decimal(msg["q"]),
                                timestamp_ms=msg["T"],
                                is_buyer_maker=msg["m"],
                            )
                            await callback(tick)
                        except (KeyError, ValueError) as e:
                            logger.warning("binance_parse_error", error=str(e))

            except ConnectionClosed as e:
                logger.warning("binance_disconnected", code=e.code, reason=str(e.reason))
            except (OSError, asyncio.TimeoutError) as e:
                logger.error("binance_connection_error", error=str(e))
            except Exception as e:
                logger.error("binance_unexpected_error", error=str(e), error_type=type(e).__name__)

            if self._running:
                logger.info("binance_reconnecting", backoff_s=backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
            logger.info("binance_stopped")
