"""On-chain Chainlink BTC/USD price reader via Polygon RPC.

Reads the Chainlink BTC/USD aggregator contract on Polygon to get
the latest oracle price. This is more reliable than the RTDS WebSocket
`crypto_prices_chainlink` topic which can be stale for minutes.

Used for:
1. Anchor price capture at 5-min boundaries (fallback if RTDS missed it)
2. Fresh Chainlink price when RTDS oracle is stale (>10s)

Contract: 0xc907E116054Ad103354f2D350FD2514433D57F6f (Polygon mainnet)
Method: latestRoundData() → (roundId, answer, startedAt, updatedAt, answeredInRound)
answer has 8 decimals → divide by 1e8
"""

import asyncio
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

import aiohttp

from monitoring.logger import get_logger

logger = get_logger("chainlink_onchain")

# Chainlink BTC/USD Aggregator on Polygon
AGGREGATOR_ADDRESS = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
# latestRoundData() function selector
LATEST_ROUND_DATA_SELECTOR = "0xfeaf968c"

# Public Polygon RPCs (no API key needed) — tried in order
DEFAULT_RPCS = [
    "https://polygon-bor-rpc.publicnode.com",
    "https://1rpc.io/matic",
]


@dataclass
class ChainlinkPrice:
    price: Decimal
    updated_at: int  # unix timestamp
    round_id: int
    age_seconds: float  # how stale the price is


class ChainlinkOnChain:
    """Reads Chainlink BTC/USD price directly from Polygon blockchain."""

    def __init__(self, rpc_urls: list[str] = None):
        self._rpc_urls = rpc_urls or DEFAULT_RPCS
        self._session: Optional[aiohttp.ClientSession] = None
        self._last_result: Optional[ChainlinkPrice] = None
        self._last_fetch_time: float = 0

    async def _ensure_session(self):
        if self._session and not self._session.closed:
            return
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = aiohttp.ClientSession()

    async def fetch_latest_price(self) -> Optional[ChainlinkPrice]:
        """Fetch the latest Chainlink BTC/USD price from on-chain.

        Tries multiple RPCs in order. Returns None on failure.
        Caches result for 1 second to avoid hammering RPCs.
        """
        # Rate limit: don't fetch more than once per second
        now = time.monotonic()
        if self._last_result and (now - self._last_fetch_time) < 1.0:
            return self._last_result

        await self._ensure_session()

        payload = {
            "jsonrpc": "2.0",
            "method": "eth_call",
            "params": [
                {
                    "to": AGGREGATOR_ADDRESS,
                    "data": LATEST_ROUND_DATA_SELECTOR,
                },
                "latest",
            ],
            "id": 1,
        }

        for rpc_url in self._rpc_urls:
            try:
                async with self._session.post(
                    rpc_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=3),
                ) as resp:
                    if resp.status != 200:
                        continue

                    data = await resp.json()
                    error = data.get("error")
                    if error:
                        logger.debug(
                            "chainlink_rpc_error",
                            rpc=rpc_url,
                            error=str(error),
                        )
                        continue

                    result = data.get("result", "")
                    if not result or len(result) < 258:  # Need 5 x 64 hex chars + 0x
                        continue

                    # Decode ABI: (uint80, int256, uint256, uint256, uint80)
                    hex_data = result[2:]
                    round_id = int(hex_data[0:64], 16)
                    answer = int(hex_data[64:128], 16)
                    # startedAt = int(hex_data[128:192], 16)
                    updated_at = int(hex_data[192:256], 16)

                    price = Decimal(answer) / Decimal("100000000")  # 8 decimals
                    age = time.time() - updated_at

                    result_obj = ChainlinkPrice(
                        price=price,
                        updated_at=updated_at,
                        round_id=round_id,
                        age_seconds=age,
                    )

                    self._last_result = result_obj
                    self._last_fetch_time = now
                    return result_obj

            except Exception as e:
                logger.debug(
                    "chainlink_rpc_fetch_error",
                    rpc=rpc_url,
                    error=str(e),
                )
                continue

        return None

    async def get_anchor_price(self) -> Optional[Decimal]:
        """Get the current Chainlink BTC/USD price for anchor use.

        Returns the price if fresh enough (<30s old), None otherwise.
        """
        result = await self.fetch_latest_price()
        if result and result.age_seconds < 30:
            return result.price
        return None

    async def close(self):
        if self._session:
            await self._session.close()
