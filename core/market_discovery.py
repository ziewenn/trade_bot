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

        The Gamma API returns eventMetadata.priceToBeat for active and
        resolved markets. This is the AUTHORITATIVE anchor price — the
        exact value Polymarket uses for resolution. However, it may not
        be populated immediately when a new market is first created
        (can take a few seconds to appear).
        """
        # Check event-level metadata (eventMetadata.priceToBeat)
        for source_name, source in [("event", event), ("market", market)]:
            metadata = source.get("eventMetadata") or source.get("metadata") or {}
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except (ValueError, TypeError):
                    metadata = {}

            if metadata:
                logger.debug(
                    "eventMetadata_found",
                    source=source_name,
                    keys=list(metadata.keys()) if isinstance(metadata, dict) else str(type(metadata)),
                    has_priceToBeat="priceToBeat" in metadata if isinstance(metadata, dict) else False,
                    has_finalPrice="finalPrice" in metadata if isinstance(metadata, dict) else False,
                )

            ptb = metadata.get("priceToBeat")
            if ptb is not None:
                try:
                    price = Decimal(str(ptb))
                    if price > 1000:
                        logger.info(
                            "anchor_from_eventMetadata",
                            source=source_name,
                            price=float(price),
                        )
                        return price
                except Exception:
                    pass

        # Check direct fields (groupItemThreshold is used in some event types)
        for field_name in ("priceToBeat", "strikePrice", "anchorPrice",
                           "groupItemThreshold"):
            val = market.get(field_name) or event.get(field_name)
            if val:
                try:
                    price = Decimal(str(val))
                    if price > 1000:
                        logger.info(
                            "anchor_from_direct_field",
                            field=field_name,
                            price=float(price),
                        )
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

        Strategies (in order):
        0. HTML regex — extract rendered "Price to beat" text from SSR HTML
        1. eventMetadata.priceToBeat (works for resolved markets)
        2. crypto-prices query openPrice (works for ACTIVE markets)
        """
        await self._ensure_session()

        try:
            import re
            import time as _time

            url = f"https://polymarket.com/event/{slug}?_t={int(_time.time())}"
            async with self._session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Cache-Control": "no-cache, no-store",
                },
            ) as resp:
                if resp.status != 200:
                    return None

                body = await resp.text()

                # Strategy 0: Extract "Price to beat" directly from rendered HTML
                # Matches: <span>Price to beat</span>...</span><span ...>$68,215.16</span>
                ptb_match = re.search(
                    r'Price to beat</span>.*?>\$([0-9,]+\.\d{2})<',
                    body,
                    re.DOTALL,
                )
                if ptb_match:
                    try:
                        price = Decimal(ptb_match.group(1).replace(",", ""))
                        if price > 1000:
                            logger.info(
                                "price_to_beat_scraped",
                                slug=slug,
                                price=float(price),
                                source="html_regex",
                            )
                            return price
                    except (ValueError, ArithmeticError):
                        pass

                nd_json = self._extract_next_data(body)
                if nd_json is None:
                    return None

                # Strategy 1: eventMetadata.priceToBeat (resolved markets)
                result = self._find_price_to_beat_in_json(nd_json, slug)
                if result:
                    logger.info(
                        "price_to_beat_scraped",
                        slug=slug,
                        price=float(result),
                        source="eventMetadata",
                    )
                    return result

                # Strategy 2: crypto-prices query (handles both fresh and stale SSR)
                result = self._find_open_price_in_queries(nd_json, slug)
                if result:
                    return result

                logger.debug(
                    "price_to_beat_not_found_in_page",
                    slug=slug,
                )
                return None

        except Exception as e:
            logger.debug(
                "price_to_beat_scrape_failed",
                slug=slug,
                error=str(e),
            )

        return None

    @staticmethod
    def _find_open_price_in_queries(
        nd_json: dict, slug: str
    ) -> Optional[Decimal]:
        """Extract the priceToBeat from the 'crypto-prices' dehydrated query.

        The crypto-prices queryKey contains the window time range:
        ['crypto-prices', 'price', 'BTC', '<startTime>', 'fiveminute', '<endTime>']

        We parse the slug's boundary timestamp and compare against the query's
        start time to detect stale/cached SSR data:

        - **Fresh** (query start == slug boundary): use ``openPrice``
          (= this window's priceToBeat)
        - **Stale** (query start != slug boundary, typically previous window):
          use ``closePrice`` (= previous window's final price = this window's
          priceToBeat)

        Both paths yield the correct priceToBeat for the current window.
        """
        try:
            # Parse expected boundary from slug  e.g. "btc-updown-5m-1774538400"
            slug_boundary = int(slug.rsplit("-", 1)[1])
        except (ValueError, IndexError):
            return None

        try:
            queries = (
                nd_json
                .get("props", {})
                .get("pageProps", {})
                .get("dehydratedState", {})
                .get("queries", [])
            )

            for q in queries:
                key = q.get("queryKey", [])
                if not key or key[0] != "crypto-prices":
                    continue

                data = q.get("state", {}).get("data", {})
                if not isinstance(data, dict):
                    continue

                # Parse query start time from queryKey[3]
                query_start_ts = None
                if len(key) > 3 and isinstance(key[3], str):
                    try:
                        from datetime import datetime, timezone

                        dt = datetime.fromisoformat(
                            key[3].replace("Z", "+00:00")
                        )
                        query_start_ts = int(dt.timestamp())
                    except (ValueError, TypeError):
                        pass

                if query_start_ts == slug_boundary:
                    # Fresh data — openPrice is this window's priceToBeat
                    open_price = data.get("openPrice")
                    if open_price is not None:
                        try:
                            price = Decimal(str(open_price))
                            if price > 1000:
                                logger.info(
                                    "price_to_beat_from_openPrice",
                                    price=float(price),
                                    query_start=key[3],
                                    fresh=True,
                                )
                                return price
                        except (ValueError, TypeError, ArithmeticError):
                            pass
                else:
                    # Stale data (previous window cached during active state).
                    # closePrice = previous window's final price = current
                    # priceToBeat. But closePrice is None if the previous
                    # window was still active when the page was cached.
                    close_price = data.get("closePrice")
                    if close_price is not None:
                        try:
                            price = Decimal(str(close_price))
                            if price > 1000:
                                logger.info(
                                    "price_to_beat_from_stale_closePrice",
                                    price=float(price),
                                    query_start=key[3] if len(key) > 3 else "?",
                                    expected_start=slug_boundary,
                                    fresh=False,
                                )
                                return price
                        except (ValueError, TypeError, ArithmeticError):
                            pass

                    # closePrice is None — page was cached while previous
                    # window was active. Both openPrice and past-results
                    # would return the PREVIOUS cycle's anchor (wrong).
                    # Return None to fall through to prev slug page scrape.
                    logger.debug(
                        "stale_page_no_closePrice",
                        query_start=key[3] if len(key) > 3 else "?",
                        expected_start=slug_boundary,
                    )
                    return None

            # Fallback: 'past-results' — last entry's closePrice
            # Only use if its endTime matches the slug boundary
            # (otherwise the data is stale and off by one window)
            for q in queries:
                key = q.get("queryKey", [])
                if not key or key[0] != "past-results":
                    continue
                data = q.get("state", {}).get("data", {})
                if isinstance(data, dict):
                    results = data.get("data", {}).get("results", [])
                    if results:
                        last = results[-1]
                        # Validate: last result's endTime must match slug boundary
                        end_time = last.get("endTime", "")
                        if end_time:
                            try:
                                from datetime import datetime, timezone

                                et = datetime.fromisoformat(
                                    end_time.replace("Z", "+00:00")
                                )
                                end_ts = int(et.timestamp())
                                if end_ts != slug_boundary:
                                    logger.debug(
                                        "past_results_stale",
                                        end_ts=end_ts,
                                        slug_boundary=slug_boundary,
                                    )
                                    continue
                            except (ValueError, TypeError):
                                continue

                        close_price = last.get("closePrice")
                        if close_price is not None:
                            try:
                                price = Decimal(str(close_price))
                                if price > 1000:
                                    logger.info(
                                        "price_to_beat_from_past_results",
                                        price=float(price),
                                    )
                                    return price
                            except (ValueError, TypeError, ArithmeticError):
                                pass

        except (KeyError, TypeError, AttributeError):
            pass

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
        """Fetch the closing/final price from a resolved market's page.

        Tries multiple strategies (fastest available first):
        1. crypto-prices query closePrice — available IMMEDIATELY after
           window ends (SSR snapshot of the last live price)
        2. eventMetadata.finalPrice — takes minutes to populate after resolution

        Since priceToBeat[N] = finalPrice[N-1] = closePrice[N-1],
        this gives us the exact anchor for the next window.
        """
        await self._ensure_session()

        try:
            import time as _time

            url = f"https://polymarket.com/event/{slug}?_t={int(_time.time())}"
            async with self._session.get(
                url,
                timeout=aiohttp.ClientTimeout(total=8),
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Cache-Control": "no-cache, no-store",
                },
            ) as resp:
                if resp.status != 200:
                    return None

                body = await resp.text()

                nd_json = self._extract_next_data(body)
                if nd_json is None:
                    return None

                # Strategy 1: crypto-prices closePrice (available immediately)
                close = self._find_close_price_in_queries(nd_json)
                if close is not None:
                    logger.info(
                        "final_price_from_closePrice",
                        slug=slug,
                        price=float(close),
                    )
                    return close

                # Strategy 2: eventMetadata.finalPrice (delayed)
                return self._find_field_in_json(nd_json, slug, "finalPrice")

        except Exception as e:
            logger.debug("final_price_fetch_failed", slug=slug, error=str(e))
            return None

    @staticmethod
    def _find_close_price_in_queries(nd_json: dict) -> Optional[Decimal]:
        """Extract closePrice from the 'crypto-prices' dehydrated query.

        On a resolved market's page, the crypto-prices query contains
        the final closePrice — the last Chainlink Data Streams value
        before the window ended. This IS the finalPrice and equals
        the next window's priceToBeat.

        Available immediately in the SSR data, unlike eventMetadata.finalPrice
        which takes minutes to populate.
        """
        try:
            queries = (
                nd_json
                .get("props", {})
                .get("pageProps", {})
                .get("dehydratedState", {})
                .get("queries", [])
            )

            for q in queries:
                key = q.get("queryKey", [])
                if not key:
                    continue

                if key[0] == "crypto-prices":
                    data = q.get("state", {}).get("data", {})
                    if isinstance(data, dict):
                        close_price = data.get("closePrice")
                        if close_price is not None:
                            try:
                                price = Decimal(str(close_price))
                                if price > 1000:
                                    return price
                            except (ValueError, TypeError, ArithmeticError):
                                pass

        except (KeyError, TypeError, AttributeError):
            pass

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

    async def fetch_anchor_from_gamma_api(
        self, slug: str, prev_slug: Optional[str] = None
    ) -> tuple[Optional[Decimal], str]:
        """Fetch priceToBeat directly from Gamma API JSON endpoint.

        Much faster than page scraping (~100ms vs ~2s). The Gamma API
        returns eventMetadata.priceToBeat for active markets. If not
        available, tries the previous slug's eventMetadata.finalPrice
        (since priceToBeat[N] = finalPrice[N-1]).

        Returns (price, source_label) or (None, "").
        """
        await self._ensure_session()

        # Try current slug's priceToBeat
        price = await self._fetch_ptb_from_gamma(slug)
        if price is not None:
            return price, "gamma_api_priceToBeat"

        # Try previous slug's finalPrice
        if prev_slug:
            price = await self._fetch_final_price_from_gamma(prev_slug)
            if price is not None:
                return price, "gamma_api_prev_finalPrice"

        return None, ""

    async def _fetch_ptb_from_gamma(self, slug: str) -> Optional[Decimal]:
        """Fetch priceToBeat from Gamma API for a specific slug."""
        url = f"{self.gamma_api_url}/events?slug={slug}"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data:
                    return None

                events = data if isinstance(data, list) else [data]
                for event in events:
                    # Check event-level eventMetadata
                    price = self._extract_metadata_field(event, "priceToBeat")
                    if price is not None:
                        return price

                    # Also check market-level
                    for market in event.get("markets", []):
                        price = self._extract_metadata_field(market, "priceToBeat")
                        if price is not None:
                            return price

                    # Log what we actually got for debugging
                    em = event.get("eventMetadata") or event.get("metadata")
                    logger.debug(
                        "gamma_api_no_priceToBeat",
                        slug=slug,
                        has_eventMetadata=em is not None,
                        eventMetadata_preview=str(em)[:200] if em else None,
                    )

        except Exception as e:
            logger.debug("gamma_api_ptb_fetch_failed", slug=slug, error=str(e))

        return None

    async def _fetch_final_price_from_gamma(self, slug: str) -> Optional[Decimal]:
        """Fetch finalPrice from Gamma API for a resolved slug."""
        url = f"{self.gamma_api_url}/events?slug={slug}"
        try:
            async with self._session.get(
                url, timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if not data:
                    return None

                events = data if isinstance(data, list) else [data]
                for event in events:
                    price = self._extract_metadata_field(event, "finalPrice")
                    if price is not None:
                        logger.info(
                            "gamma_api_finalPrice_found",
                            slug=slug,
                            price=float(price),
                        )
                        return price

                    for market in event.get("markets", []):
                        price = self._extract_metadata_field(market, "finalPrice")
                        if price is not None:
                            logger.info(
                                "gamma_api_finalPrice_found",
                                slug=slug,
                                price=float(price),
                                source="market",
                            )
                            return price

        except Exception as e:
            logger.debug("gamma_api_fp_fetch_failed", slug=slug, error=str(e))

        return None

    @staticmethod
    def _extract_metadata_field(obj: dict, field: str) -> Optional[Decimal]:
        """Extract a numeric field from eventMetadata (handles string JSON)."""
        metadata = obj.get("eventMetadata") or obj.get("metadata") or {}
        if isinstance(metadata, str):
            try:
                metadata = json.loads(metadata)
            except (ValueError, TypeError):
                return None
        if not isinstance(metadata, dict):
            return None
        val = metadata.get(field)
        if val is not None:
            try:
                price = Decimal(str(val))
                if price > 1000:
                    return price
            except (ValueError, TypeError, ArithmeticError):
                pass
        return None

    async def scrape_authoritative_anchor(
        self, slug: str, prev_slug: Optional[str] = None
    ) -> tuple[Optional[Decimal], str]:
        """Try all strategies to get the definitive anchor price.

        Attempts in order (Playwright first — instant via JS-rendered DOM):
        1. Persistent Playwright browser — reads rendered "Price to beat"
           directly from the live DOM (1-3s, same as user's browser)
        2. HTTP page scrape CURRENT slug (CDN-cached, may be stale 60-75s)
        3. HTTP page scrape PREVIOUS slug — closePrice (fallback)
        4. Gamma API JSON — priceToBeat / finalPrice (slow, 60-75s)

        Returns (price, source_label) or (None, "").
        """
        # 1. Persistent Playwright browser — instant JS-rendered price
        pw_price = await self.read_price_to_beat_live(slug)
        if pw_price is not None:
            return pw_price, "playwright_priceToBeat"

        # 2. HTTP page scrape CURRENT slug (handles fresh + stale SSR)
        ptb = await self.fetch_price_to_beat(slug)
        if ptb is not None:
            return ptb, "scraped_priceToBeat"

        # 3. Previous slug's closePrice via HTTP page scrape (fallback)
        if prev_slug:
            fp = await self.fetch_final_price(prev_slug)
            if fp is not None:
                return fp, "scraped_prev_closePrice"

        # 4. Gamma API direct (slower to populate metadata)
        price, source = await self.fetch_anchor_from_gamma_api(slug, prev_slug)
        if price is not None:
            return price, source

        return None, ""

    # ------------------------------------------------------------------
    # Persistent Playwright browser — keeps a headless Chromium page open
    # on the Polymarket market page so JS-rendered "Price to beat" is
    # available instantly at cycle transitions (1-3s vs 60-75s CDN wait).
    # ------------------------------------------------------------------

    async def start_persistent_browser(self, initial_slug: str | None = None):
        """Launch a persistent headless Chromium and navigate to the market page.

        Call once at bot startup. The page stays open and auto-updates via
        Polymarket's client-side JS, just like a real browser tab.
        """
        try:
            from playwright.async_api import async_playwright
        except ImportError:
            logger.warning(
                "playwright_not_installed",
                hint="pip install playwright && playwright install chromium",
            )
            return

        try:
            if not hasattr(self, "_pw_browser") or self._pw_browser is None:
                self._pw_instance = await async_playwright().start()
                self._pw_browser = await self._pw_instance.chromium.launch(
                    headless=True,
                )
                logger.info("playwright_browser_launched")

            self._pw_page = await self._pw_browser.new_page()

            if initial_slug:
                url = f"https://polymarket.com/event/{initial_slug}"
                await self._pw_page.goto(url, wait_until="domcontentloaded", timeout=20000)
                logger.info("playwright_initial_page_loaded", slug=initial_slug)

        except Exception as e:
            logger.error("playwright_start_failed", error=str(e))
            self._pw_page = None

    async def read_price_to_beat_live(self, slug: str) -> Optional[Decimal]:
        """Read "Price to beat" from the persistent Playwright page.

        Navigates to the slug's page (if not already there), waits for
        client-side JS to render the price, and extracts it from the DOM.
        Returns the price or None on failure.
        """
        if not hasattr(self, "_pw_page") or self._pw_page is None:
            return None

        import re as _re

        try:
            url = f"https://polymarket.com/event/{slug}"
            current_url = self._pw_page.url

            # Navigate only if we're not already on this slug's page
            if slug not in current_url:
                await self._pw_page.goto(
                    url, wait_until="domcontentloaded", timeout=10000
                )

            # Wait for the "Price to beat" text to appear in the DOM
            try:
                await self._pw_page.wait_for_selector(
                    "text=Price to beat", timeout=5000
                )
            except Exception:
                # Text might already be present, or selector format differs
                pass

            # Small delay for JS to finish rendering the price value
            await asyncio.sleep(0.5)

            # Strategy A: JS evaluation to find the price near "Price to beat"
            price_str = await self._pw_page.evaluate(
                """() => {
                    const spans = document.querySelectorAll('span');
                    for (const span of spans) {
                        if (span.textContent.trim() === 'Price to beat') {
                            // Walk up to find the container with the dollar value
                            let el = span.parentElement;
                            for (let i = 0; i < 5 && el; i++) {
                                const match = el.textContent.match(/\\$([0-9,]+\\.\\d{2})/);
                                if (match) return match[1].replace(/,/g, '');
                                el = el.parentElement;
                            }
                        }
                    }
                    return null;
                }"""
            )

            if price_str:
                try:
                    price = Decimal(price_str)
                    if price > 1000:
                        logger.info(
                            "playwright_live_price_to_beat",
                            slug=slug,
                            price=float(price),
                            source="js_eval",
                        )
                        return price
                except (ValueError, ArithmeticError):
                    pass

            # Strategy B: Regex on rendered HTML (not SSR — includes JS output)
            body = await self._pw_page.content()
            ptb_match = _re.search(
                r'Price to beat</span>.*?>\$([0-9,]+\.\d{2})<',
                body,
                _re.DOTALL,
            )
            if ptb_match:
                try:
                    price = Decimal(ptb_match.group(1).replace(",", ""))
                    if price > 1000:
                        logger.info(
                            "playwright_live_price_to_beat",
                            slug=slug,
                            price=float(price),
                            source="html_regex",
                        )
                        return price
                except (ValueError, ArithmeticError):
                    pass

            logger.debug("playwright_price_not_found", slug=slug)
            return None

        except Exception as e:
            logger.warning(
                "playwright_live_read_failed",
                slug=slug,
                error=str(e),
            )
            # Page may have crashed — try to recover with a new page
            try:
                if hasattr(self, "_pw_browser") and self._pw_browser:
                    self._pw_page = await self._pw_browser.new_page()
                    logger.info("playwright_page_recovered")
            except Exception:
                self._pw_page = None
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
        # Close persistent Playwright page and browser
        if hasattr(self, "_pw_page") and self._pw_page:
            try:
                await self._pw_page.close()
            except Exception:
                pass
            self._pw_page = None
        if hasattr(self, "_pw_browser") and self._pw_browser:
            try:
                await self._pw_browser.close()
                await self._pw_instance.stop()
            except Exception:
                pass
            self._pw_browser = None
            self._pw_instance = None
        logger.info("market_discovery_stopped")
