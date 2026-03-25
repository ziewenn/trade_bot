import asyncio
import json
import time
from decimal import Decimal
from typing import Callable, Awaitable, Optional

import aiohttp

from data.models import MarketInfo
from monitoring.logger import get_logger

logger = get_logger("market_discovery")


class MarketDiscovery:
    """Discovers active 5-minute BTC Up/Down markets on Polymarket via Gamma API."""

    POLL_INTERVAL_NORMAL = 2  # seconds — normal polling
    POLL_INTERVAL_URGENT = 0.3  # seconds — rapid polling when boundary passed

    def __init__(self, gamma_api_url: str, playwright_fallback: bool = False):
        self.gamma_api_url = gamma_api_url
        self.playwright_fallback = playwright_fallback
        self._running = False
        self._session: Optional[aiohttp.ClientSession] = None
        self.current_market: Optional[MarketInfo] = None

    async def start(
        self,
        on_new_market: Callable[[MarketInfo], Awaitable[None]],
    ):
        """Poll for new markets and call on_new_market when one is found."""
        self._running = True
        self._session = aiohttp.ClientSession()

        self._transitioning = False  # Guard against duplicate transitions

        try:
            while self._running:
                try:
                    market = await self._find_current_market()
                    if market and not self._transitioning and (
                        not self.current_market
                        or market.condition_id != self.current_market.condition_id
                    ):
                        self._transitioning = True
                        try:
                            logger.info(
                                "new_market_found",
                                slug=market.event_slug,
                                condition_id=market.condition_id[:16],
                                start=market.start_time,
                                end=market.end_time,
                            )
                            self.current_market = market
                            await on_new_market(market)
                        finally:
                            self._transitioning = False

                except Exception as e:
                    logger.error("market_discovery_error", error=str(e))
                    self._transitioning = False

                # Poll faster when we know a boundary just passed
                if (
                    self.current_market
                    and time.time() >= self.current_market.end_time
                ):
                    await asyncio.sleep(self.POLL_INTERVAL_URGENT)
                else:
                    await asyncio.sleep(self.POLL_INTERVAL_NORMAL)
        finally:
            await self._session.close()

    async def _ensure_session(self):
        """Ensure an active aiohttp session exists."""
        if self._session and not self._session.closed:
            return
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass
        self._session = aiohttp.ClientSession()

    async def _find_current_market(self) -> Optional[MarketInfo]:
        """Find the currently active or upcoming 5-min BTC market.

        Uses proactive boundary detection: if we know the current market's
        end_time has passed, we immediately construct the next market's slug
        and create a synthetic MarketInfo while waiting for Gamma API to
        confirm. This eliminates the ~15-20s delay waiting for Gamma to
        create the new event.
        """
        await self._ensure_session()

        current_ts = int(time.time())
        current_boundary = current_ts - (current_ts % 300)
        next_boundary = current_boundary + 300

        # Proactive transition: if current market has ended, construct
        # the next market immediately without waiting for Gamma API
        if (
            self.current_market
            and current_ts >= self.current_market.end_time
        ):
            new_boundary = int(self.current_market.end_time)
            # end_time of old market = start_time of new market
            new_slug = f"btc-updown-5m-{new_boundary}"

            # Try fetching from Gamma first (fast path if already created)
            market = await self._fetch_by_slug(new_slug, new_boundary)
            if market:
                return market

            # Not on Gamma yet — try the computed boundary slug
            if new_boundary != current_boundary:
                slug = f"btc-updown-5m-{current_boundary}"
                market = await self._fetch_by_slug(slug, current_boundary)
                if market:
                    return market

            # Still not found — return None, will retry next poll
            return None

        # Normal discovery: try current window first, then next
        for boundary in [current_boundary, next_boundary]:
            slug = f"btc-updown-5m-{boundary}"
            market = await self._fetch_by_slug(slug, boundary)
            if market:
                return market

        # Fallback: tag-based search
        return await self._fetch_by_tag()

    async def _fetch_by_slug(
        self, slug: str, boundary_ts: int
    ) -> Optional[MarketInfo]:
        url = f"{self.gamma_api_url}/events?slug={slug}"
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()

                if not data:
                    return None

                # Response can be a list or single object
                events = data if isinstance(data, list) else [data]

                for event in events:
                    market = self._parse_event(event, slug)
                    if market:
                        return market

        except Exception as e:
            logger.debug("slug_fetch_failed", slug=slug, error=str(e))

        return None

    async def _fetch_by_tag(self) -> Optional[MarketInfo]:
        url = f"{self.gamma_api_url}/events?tag=btc-5min&active=true&closed=false&limit=1"
        try:
            async with self._session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()

                if not data:
                    return None

                events = data if isinstance(data, list) else [data]
                for event in events:
                    market = self._parse_event(event)
                    if market:
                        return market

        except Exception as e:
            logger.debug("tag_fetch_failed", error=str(e))

        return None

    def _parse_event(
        self, event: dict, slug: str = ""
    ) -> Optional[MarketInfo]:
        """Parse a Gamma API event response into a MarketInfo."""
        try:
            markets = event.get("markets", [])
            if not markets:
                return None

            for market in markets:
                token_id_up = None
                token_id_down = None

                # Method 1: tokens array (some responses)
                tokens = market.get("tokens", [])
                if tokens and len(tokens) >= 2:
                    for token in tokens:
                        outcome = token.get("outcome", "")
                        if outcome.lower() == "up":
                            token_id_up = token.get("token_id")
                        elif outcome.lower() == "down":
                            token_id_down = token.get("token_id")

                # Method 2: clobTokenIds + outcomes (Gamma API format)
                if not token_id_up or not token_id_down:
                    clob_ids_raw = market.get("clobTokenIds", "")
                    outcomes_raw = market.get("outcomes", [])

                    # Both fields can be JSON strings or lists
                    clob_ids = self._parse_json_field(clob_ids_raw)
                    outcomes = self._parse_json_field(outcomes_raw)

                    if len(clob_ids) >= 2 and len(outcomes) >= 2:
                        for i, outcome in enumerate(outcomes):
                            if outcome.lower() == "up" and i < len(clob_ids):
                                token_id_up = clob_ids[i]
                            elif outcome.lower() == "down" and i < len(clob_ids):
                                token_id_down = clob_ids[i]

                if not token_id_up or not token_id_down:
                    continue

                condition_id = market.get("conditionId", market.get("condition_id", ""))
                event_slug = slug or event.get("slug", "")

                # Parse times — eventStartTime is the actual 5-min window start
                # startDate is market creation time (too early), endDate is window end
                start_str = market.get("eventStartTime", "") or event.get("startTime", "") or market.get("startDate", "")
                end_str = market.get("endDate", market.get("end_date", ""))

                start_time = self._parse_time(start_str)
                end_time = self._parse_time(end_str)

                if not condition_id or not start_time or not end_time:
                    continue

                # Extract anchor price from event/market data
                anchor_price = self._extract_anchor_price(event, market)

                logger.info(
                    "market_parsed",
                    slug=event_slug,
                    condition_id=condition_id[:16],
                    up_token=token_id_up[:16],
                    down_token=token_id_down[:16],
                    end_time=end_str,
                    anchor_price=float(anchor_price) if anchor_price else None,
                )

                return MarketInfo(
                    event_slug=event_slug,
                    condition_id=condition_id,
                    token_id_up=token_id_up,
                    token_id_down=token_id_down,
                    start_time=start_time,
                    end_time=end_time,
                    anchor_price=anchor_price,
                )

        except (KeyError, TypeError) as e:
            logger.debug("parse_event_error", error=str(e))

        return None

    @staticmethod
    def _extract_anchor_price(event: dict, market: dict) -> Optional[Decimal]:
        """Try to extract anchor price from Gamma API event/market data.

        NOTE: Live API probing (2026-03-25) confirmed that the Gamma API
        does NOT currently return priceToBeat or any anchor price field.
        The primary anchor source is RTDS WebSocket (crypto_prices btc/usd
        topic at 5-min boundaries). This method is kept as future-proofing
        in case Polymarket adds the field to the API.
        """
        # Check event-level metadata (eventMetadata.priceToBeat)
        for source in (event, market):
            metadata = source.get("eventMetadata") or source.get("metadata") or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (ValueError, TypeError):
                    metadata = {}
            ptb = metadata.get("priceToBeat")
            if ptb is not None:
                try:
                    price = Decimal(str(ptb))
                    if price > 1000:
                        return price
                except Exception:
                    pass

        # Check direct fields (future-proofing)
        for field_name in ("priceToBeat", "strikePrice", "anchorPrice",
                           "groupItemThreshold"):
            val = market.get(field_name) or event.get(field_name)
            if val:
                try:
                    price = Decimal(str(val))
                    if price > 1000:
                        return price
                except Exception:
                    pass

        return None

    @staticmethod
    def _parse_json_field(raw) -> list:
        """Parse a field that may be a JSON string or already a list."""
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str) and raw:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list):
                    return parsed
            except (ValueError, TypeError):
                pass
        return []

    @staticmethod
    def _parse_time(time_str: str) -> Optional[float]:
        """Parse ISO timestamp or unix timestamp to float."""
        if not time_str:
            return None
        try:
            # Try as float (unix timestamp)
            return float(time_str)
        except ValueError:
            pass
        try:
            from datetime import datetime, timezone

            # Try ISO format
            if time_str.endswith("Z"):
                time_str = time_str[:-1] + "+00:00"
            dt = datetime.fromisoformat(time_str)
            return dt.timestamp()
        except ValueError:
            return None

    async def fetch_price_to_beat(self, slug: str) -> Optional[Decimal]:
        """Fetch priceToBeat for a given slug from Polymarket page.

        Fetches the slug's event page and extracts eventMetadata.priceToBeat
        from the __NEXT_DATA__ JSON. This works for resolved/past windows.

        For the CURRENT window's anchor, call this with the PREVIOUS slug
        (since priceToBeat[N-1] = anchor for window N-1, and the previous
        window's closing Chainlink price = current window's anchor).
        """
        await self._ensure_session()

        try:
            url = f"https://polymarket.com/event/{slug}"
            async with self._session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status != 200:
                    return None

                body = await resp.text()

                nd_json = self._extract_next_data(body)
                if nd_json is None:
                    return None

                result = self._find_price_to_beat_in_json(nd_json, slug)
                if result:
                    logger.info(
                        "price_to_beat_scraped",
                        slug=slug,
                        price=float(result),
                    )
                return result

        except Exception as e:
            logger.debug(
                "price_to_beat_scrape_failed",
                slug=slug,
                error=str(e),
            )

        return None

    @staticmethod
    def _find_price_to_beat_in_json(
        obj, target_slug: str
    ) -> Optional[Decimal]:
        """Recursively search JSON for a specific slug's priceToBeat."""
        if isinstance(obj, dict):
            slug_val = obj.get("slug", "") or obj.get("ticker", "")
            if slug_val == target_slug:
                # Found our event — extract priceToBeat from eventMetadata
                em = obj.get("eventMetadata") or {}
                if isinstance(em, str):
                    try:
                        em = json.loads(em)
                    except (ValueError, TypeError):
                        em = {}
                ptb = em.get("priceToBeat")
                if ptb is not None:
                    try:
                        price = Decimal(str(ptb))
                        if price > 1000:
                            return price
                    except (ValueError, TypeError):
                        pass
            # Recurse into values
            for v in obj.values():
                result = MarketDiscovery._find_price_to_beat_in_json(
                    v, target_slug
                )
                if result:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = MarketDiscovery._find_price_to_beat_in_json(
                    item, target_slug
                )
                if result:
                    return result
        return None

    async def fetch_final_price(self, slug: str) -> Optional[Decimal]:
        """Fetch the finalPrice from a resolved market's Polymarket page.

        After a market resolves, its eventMetadata gets a `finalPrice` field.
        Since priceToBeat[N] = finalPrice[N-1], this gives us the exact
        anchor for the next window.
        """
        await self._ensure_session()

        try:
            url = f"https://polymarket.com/event/{slug}"
            async with self._session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as resp:
                if resp.status != 200:
                    return None

                body = await resp.text()

                nd_json = self._extract_next_data(body)
                if nd_json is None:
                    return None

                return self._find_field_in_json(nd_json, slug, "finalPrice")

        except Exception as e:
            logger.debug("final_price_fetch_failed", slug=slug, error=str(e))
            return None

    @staticmethod
    def _find_field_in_json(
        obj, target_slug: str, field: str
    ) -> Optional[Decimal]:
        """Find a specific eventMetadata field for a given slug in JSON."""
        if isinstance(obj, dict):
            slug_val = obj.get("slug", "") or obj.get("ticker", "")
            if slug_val == target_slug:
                em = obj.get("eventMetadata") or {}
                if isinstance(em, str):
                    try:
                        em = json.loads(em)
                    except (ValueError, TypeError):
                        em = {}
                val = em.get(field)
                if val is not None:
                    try:
                        price = Decimal(str(val))
                        if price > 1000:
                            return price
                    except (ValueError, TypeError):
                        pass
            for v in obj.values():
                result = MarketDiscovery._find_field_in_json(v, target_slug, field)
                if result:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = MarketDiscovery._find_field_in_json(item, target_slug, field)
                if result:
                    return result
        return None

    @staticmethod
    def _extract_next_data(html: str) -> Optional[dict]:
        """Extract __NEXT_DATA__ JSON from page HTML using dual regex patterns.

        Handles both Next.js SSR embedding styles:
        1. <script id="__NEXT_DATA__" type="application/json">...</script>
        2. window.__NEXT_DATA__ = {...}; (older/alternative style)
        """
        import re

        # Pattern 1: Standard Next.js script tag (most common)
        match = re.search(
            r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html, re.DOTALL,
        )
        if match:
            try:
                return json.loads(match.group(1))
            except (json.JSONDecodeError, ValueError):
                pass

        # Pattern 2: window.__NEXT_DATA__ assignment (fallback)
        match = re.search(
            r'window\.__NEXT_DATA__\s*=\s*({.*?})\s*;?\s*</script>',
            html, re.DOTALL,
        )
        if match:
            try:
                return json.loads(match.group(1))
            except (json.JSONDecodeError, ValueError):
                pass

        return None

    async def scrape_authoritative_anchor(
        self, slug: str, prev_slug: Optional[str] = None
    ) -> tuple[Optional[Decimal], str]:
        """Try all scraping strategies to get the definitive anchor price.

        Attempts in order:
        1. Current slug's priceToBeat via plain HTTP
        2. Previous slug's finalPrice via plain HTTP (priceToBeat[N] == finalPrice[N-1])
        3. Playwright JS rendering (if enabled in settings)

        Returns (price, source_label) or (None, "").
        """
        # 1. Current slug priceToBeat (plain HTTP)
        ptb = await self.fetch_price_to_beat(slug)
        if ptb is not None:
            return ptb, "scraped_priceToBeat"

        # 2. Previous slug finalPrice (plain HTTP)
        if prev_slug:
            fp = await self.fetch_final_price(prev_slug)
            if fp is not None:
                return fp, "scraped_prev_finalPrice"

        # 3. Playwright fallback (if enabled)
        if self.playwright_fallback:
            pw_price = await self._scrape_via_playwright(slug)
            if pw_price is not None:
                return pw_price, "scraped_playwright"

        return None, ""

    async def _scrape_via_playwright(self, slug: str) -> Optional[Decimal]:
        """Scrape priceToBeat using Playwright for JS-rendered pages.

        Lazily initializes a single headless Chromium browser instance
        that is reused across calls. The browser is closed in stop().
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning(
                "playwright_not_installed",
                hint="pip install playwright && playwright install chromium",
            )
            return None

        try:
            if not hasattr(self, "_pw_browser") or self._pw_browser is None:
                self._pw_instance = await async_playwright().start()
                self._pw_browser = await self._pw_instance.chromium.launch(
                    headless=True
                )
                logger.info("playwright_browser_launched")

            page = await self._pw_browser.new_page()
            try:
                url = f"https://polymarket.com/event/{slug}"
                await page.goto(url, wait_until="networkidle", timeout=15000)

                # Extract __NEXT_DATA__ from rendered DOM
                nd_text = await page.evaluate(
                    """() => {
                        const el = document.getElementById('__NEXT_DATA__');
                        return el ? el.textContent : null;
                    }"""
                )
                if nd_text:
                    nd = json.loads(nd_text)
                    result = self._find_price_to_beat_in_json(nd, slug)
                    if result:
                        logger.info(
                            "playwright_price_to_beat_scraped",
                            slug=slug,
                            price=float(result),
                        )
                        return result
            finally:
                await page.close()

        except Exception as e:
            logger.debug(
                "playwright_scrape_failed",
                slug=slug,
                error=str(e),
            )

        return None

    async def fetch_binance_price_at(self, timestamp_s: float) -> Optional[Decimal]:
        """Fetch the BTC price from Binance at a specific timestamp.

        Uses the 1-minute kline API to get the open price of the candle
        containing `timestamp_s`. This is a close approximation of the
        Chainlink price at that time (typically within $1-5).

        Used as a fallback when the bot starts mid-cycle and has no
        Chainlink history from market start_time.
        """
        await self._ensure_session()

        # Align to the start of the minute
        minute_boundary = int(timestamp_s) - (int(timestamp_s) % 60)
        start_ms = minute_boundary * 1000

        url = (
            f"https://api.binance.com/api/v3/klines"
            f"?symbol=BTCUSDT&interval=1m&startTime={start_ms}&limit=1"
        )
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data and len(data) > 0:
                    open_price = Decimal(str(data[0][1]))
                    logger.info(
                        "binance_historical_price",
                        timestamp=timestamp_s,
                        price=float(open_price),
                    )
                    return open_price
        except Exception as e:
            logger.warning("binance_historical_fetch_error", error=str(e))
        return None

    async def fetch_resolved_outcome(
        self, slug: str, retries: int = 6, delay: float = 5.0
    ) -> Optional[str]:
        """Poll the Gamma API for a market's resolved outcome.

        Returns "Up" or "Down" if resolved, None if not yet resolved
        or on failure. Retries up to `retries` times with `delay` between.
        """
        await self._ensure_session()

        url = f"{self.gamma_api_url}/events?slug={slug}"

        for attempt in range(retries):
            try:
                async with self._session.get(
                    url, timeout=aiohttp.ClientTimeout(total=10)
                ) as resp:
                    if resp.status != 200:
                        await asyncio.sleep(delay)
                        continue

                    data = await resp.json()
                    if not data:
                        await asyncio.sleep(delay)
                        continue

                    events = data if isinstance(data, list) else [data]
                    for event in events:
                        for market in event.get("markets", []):
                            status = market.get("umaResolutionStatus", "")
                            if status == "resolved":
                                outcomes = self._parse_json_field(
                                    market.get("outcomes", [])
                                )
                                prices = self._parse_json_field(
                                    market.get("outcomePrices", [])
                                )
                                # Find the winning outcome (price == "1")
                                for i, price in enumerate(prices):
                                    if str(price) == "1" and i < len(outcomes):
                                        winner = outcomes[i]
                                        logger.info(
                                            "api_resolution_fetched",
                                            slug=slug,
                                            winner=winner,
                                            attempt=attempt + 1,
                                        )
                                        return winner

            except Exception as e:
                logger.debug(
                    "resolution_fetch_error",
                    slug=slug,
                    attempt=attempt + 1,
                    error=str(e),
                )

            if attempt < retries - 1:
                await asyncio.sleep(delay)

        logger.warning(
            "resolution_fetch_failed",
            slug=slug,
            retries=retries,
        )
        return None

    async def stop(self):
        self._running = False
        if self._session:
            await self._session.close()
        # Close Playwright browser if it was initialized
        if hasattr(self, "_pw_browser") and self._pw_browser:
            try:
                await self._pw_browser.close()
                await self._pw_instance.stop()
            except Exception:
                pass
            self._pw_browser = None
            self._pw_instance = None
        logger.info("market_discovery_stopped")
