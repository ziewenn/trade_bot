#!/usr/bin/env python3
"""Polymarket BTC 5-Minute Latency Arbitrage Bot — Main Entry Point."""

import asyncio
import os
import signal
import sys
import time
from decimal import Decimal

from config.settings import Settings
from core.binance_feed import BinanceFeed
from core.chainlink_feed import ChainlinkFeed
from core.chainlink_onchain import ChainlinkOnChain
from core.market_discovery import MarketDiscovery
from core.order_manager import OrderManager
from core.polymarket_feed import PolymarketFeed
from core.risk_manager import RiskManager
from core.state import SharedState
from core.strategy import Strategy
from data.database import Database
from data.models import MarketInfo, MarketPhase
from execution.paper_trader import PaperTrader
from execution.live_trader import LiveTrader
from execution.trader_interface import TraderInterface
from monitoring.alerts import TelegramAlerts
from monitoring.dashboard import Dashboard
from monitoring.logger import setup_logging, get_logger


logger = get_logger("main")


class Bot:
    """Main bot orchestrator."""

    def __init__(self):
        self.settings: Settings = None
        self.state: SharedState = None
        self.db: Database = None
        self.binance_feed: BinanceFeed = None
        self.chainlink_feed: ChainlinkFeed = None
        self.polymarket_feed: PolymarketFeed = None
        self.market_discovery: MarketDiscovery = None
        self.strategy: Strategy = None
        self.trader: TraderInterface = None
        self.order_manager: OrderManager = None
        self.risk_manager: RiskManager = None
        self.dashboard: Dashboard = None
        self.alerts: TelegramAlerts = None
        self._shutdown_event = asyncio.Event()
        self._background_tasks: set[asyncio.Task] = set()

    def _track_task(self, task: asyncio.Task, name: str):
        """Track a background task and log errors when it completes."""
        self._background_tasks.add(task)

        def _on_done(t: asyncio.Task):
            self._background_tasks.discard(t)
            if t.cancelled():
                return
            exc = t.exception()
            if exc:
                logger.error(
                    "background_task_failed",
                    task_name=name,
                    error=str(exc),
                    error_type=type(exc).__name__,
                )

        task.add_done_callback(_on_done)

    async def initialize(self):
        """Load config, init all components."""
        # 1. Load settings
        self.settings = Settings()
        setup_logging(self.settings.log_level)

        logger.info(
            "bot_initializing",
            mode=self.settings.trading_mode,
            bankroll=self.settings.initial_bankroll,
        )

        # 2. Live mode safety gate (skip in headless mode)
        if self.settings.trading_mode == "live" and not self.settings.headless:
            print("\n" + "=" * 60)
            print("  WARNING: LIVE TRADING MODE")
            print("  Real orders will be placed on Polymarket.")
            print("  Real USDC will be at risk.")
            print("=" * 60)
            print("\nType 'CONFIRM LIVE TRADING' to proceed:")

            confirmation = input("> ").strip()
            if confirmation != "CONFIRM LIVE TRADING":
                print("Live trading not confirmed. Exiting.")
                sys.exit(1)

        # 3. Initialize database
        os.makedirs(os.path.dirname(self.settings.db_path) or "data", exist_ok=True)
        self.db = Database(self.settings.db_path)
        await self.db.connect()

        # 4. Initialize shared state
        self.state = SharedState()

        # 5. Initialize feeds
        self.binance_feed = BinanceFeed(
            ws_url=self.settings.binance_ws_url,
            lookback_ms=self.settings.binance_price_lookback_ms,
        )
        self.chainlink_feed = ChainlinkFeed(ws_url=self.settings.chainlink_ws_url)
        self.chainlink_onchain = ChainlinkOnChain()
        self.polymarket_feed = PolymarketFeed(ws_url=self.settings.polymarket_ws_url)
        self.market_discovery = MarketDiscovery(
            gamma_api_url=self.settings.gamma_api_url,
            playwright_fallback=self.settings.anchor_scrape_playwright_fallback,
        )

        # 6. Initialize strategy
        self.strategy = Strategy(self.settings)

        # 7. Initialize trader
        if self.settings.trading_mode == "live":
            live_trader = LiveTrader(
                private_key=self.settings.polymarket_private_key.get_secret_value(),
                funder_address=self.settings.polymarket_funder_address,
                clob_url=self.settings.polymarket_clob_url,
            )
            await live_trader.initialize()
            self.trader = live_trader
        elif self.settings.trading_mode == "sim-live":
            from execution.simulated_live_trader import SimulatedLiveTrader

            self.trader = SimulatedLiveTrader(
                initial_bankroll=self.settings.initial_bankroll,
                database=self.db,
                settings=self.settings,
            )
            self.trader.set_shared_state(self.state)
            logger.info(
                "sim_live_mode",
                order_latency=f"{self.settings.sim_order_latency_min_ms}-{self.settings.sim_order_latency_max_ms}ms",
                rejection_rate=f"{self.settings.sim_rejection_rate:.0%}",
                fill_rate=f"{self.settings.sim_fill_rate:.0%}",
                slippage=f"{self.settings.sim_slippage_cents}¢",
            )
        else:
            self.trader = PaperTrader(
                initial_bankroll=self.settings.initial_bankroll,
                database=self.db,
            )
            self.trader.set_shared_state(self.state)

        # 8. Initialize order manager
        self.order_manager = OrderManager(
            trader=self.trader,
            strategy=self.strategy,
            state=self.state,
            settings=self.settings,
        )

        # 9. Initialize risk manager
        self.risk_manager = RiskManager(self.settings, self.db, self.state)
        self.risk_manager.set_halt_callback(self._on_risk_halt)

        # 10. Initialize monitoring
        self.dashboard = Dashboard(
            self.settings, self.state, self.risk_manager, self.order_manager
        )
        self.alerts = TelegramAlerts(
            bot_token=self.settings.telegram_bot_token,
            chat_id=self.settings.telegram_chat_id,
        )

        logger.info("bot_initialized")

    async def run(self):
        """Run all concurrent tasks."""
        await self.alerts.bot_started(self.settings.trading_mode)

        try:
            async with asyncio.TaskGroup() as tg:
                # Data feeds
                tg.create_task(self._run_binance_feed())
                tg.create_task(self._run_chainlink_feed())
                tg.create_task(self._run_market_discovery())

                # Core trading loop
                tg.create_task(self.order_manager.run_cancel_replace_loop())

                # Risk monitoring
                tg.create_task(
                    self.risk_manager.run_stale_data_watchdog(
                        self.order_manager.cancel_all
                    )
                )

                # Monitoring — terminal dashboard only when not headless
                if not self.settings.headless:
                    tg.create_task(self.dashboard.run())
                tg.create_task(self.alerts.start())

                # Web dashboard (for remote monitoring)
                if self.settings.dashboard_enabled:
                    from monitoring.web_dashboard import WebDashboard

                    web = WebDashboard(
                        self.settings,
                        self.state,
                        self.risk_manager,
                        self.order_manager,
                    )
                    tg.create_task(web.run())

                # DB maintenance
                tg.create_task(self._cleanup_loop())

                # Wait for shutdown
                tg.create_task(self._wait_for_shutdown())

        except* KeyboardInterrupt:
            logger.info("keyboard_interrupt")
        except* Exception as eg:
            for exc in eg.exceptions:
                logger.error("task_group_error", error=str(exc))

    async def shutdown(self):
        """Graceful shutdown sequence."""
        logger.info("shutdown_started")

        # 1. Cancel all open orders
        try:
            await self.order_manager.cancel_all()
        except Exception as e:
            logger.error("shutdown_cancel_failed", error=str(e))

        # 2. Close feeds
        await self.binance_feed.stop()
        await self.chainlink_feed.stop()
        await self.chainlink_onchain.close()
        await self.polymarket_feed.stop()
        await self.market_discovery.stop()

        # 3. Flush database
        await self.db.flush()

        # 4. Log final stats
        pnl = float(self.state.session_pnl)
        logger.info(
            "session_complete",
            total_trades=self.state.total_trades,
            winning_trades=self.state.winning_trades,
            session_pnl=pnl,
        )

        # 5. Send shutdown alert
        await self.alerts.bot_stopped(pnl)

        # 6. Close connections
        await self.alerts.close()
        await self.db.close()

        logger.info("shutdown_complete")

    # --- Feed runners ---

    async def _run_binance_feed(self):
        async def on_tick(tick):
            self.state.update_binance_price(tick.price, tick.timestamp_ms)
            self.state.binance_connected = True
            self.strategy.update_volatility(float(tick.price), tick.timestamp_ms)

        await self.binance_feed.start(on_tick)

    async def _run_chainlink_feed(self):
        async def on_chainlink_price(price, timestamp_ms):
            self.state.update_chainlink_price(price, timestamp_ms)
            self.state.chainlink_connected = True

        async def on_rtds_binance_price(price, timestamp_ms):
            self.state.update_rtds_binance_price(price, timestamp_ms)
            self.state.chainlink_connected = True  # RTDS connection is alive

        async def on_anchor_batch(data_array):
            """Capture anchor price at 5-min boundaries from RTDS batch.

            The RTDS `crypto_prices` `btc/usd` batch entry at `ts % 300 == 0`
            is the EXACT priceToBeat used by Polymarket for resolution.
            Verified: RTDS value matches Polymarket's priceToBeat to 14 decimal places.
            """
            for entry in data_array:
                ts_ms = entry.get("timestamp", 0)
                ts_s = ts_ms // 1000
                if ts_s > 0 and ts_s % 300 == 0 and ts_s not in self.state.captured_anchors:
                    try:
                        price = Decimal(str(entry["value"]))
                        if price > 1000:
                            self.state.captured_anchors[ts_s] = price
                            logger.info(
                                "anchor_captured_at_boundary",
                                window_ts=ts_s,
                                price=float(price),
                            )
                    except (ValueError, KeyError):
                        pass
                    break

            # Prune old anchors
            while len(self.state.captured_anchors) > 20:
                oldest = min(self.state.captured_anchors)
                del self.state.captured_anchors[oldest]

        # Background task: on-chain Chainlink refresh + proactive anchor capture
        async def chainlink_onchain_refresher():
            """Periodically fetch on-chain Chainlink price via Polygon RPC.

            Two jobs:
            1. Keep state.chainlink_price fresh (every 5s when RTDS stale)
            2. Proactively capture anchor at 5-min boundaries (every 1s near boundary)

            The RTDS btc/usd batch is unreliable (often doesn't arrive),
            so we use a timer-based approach to capture on-chain price
            at the exact boundary moment.
            """
            while True:
                try:
                    now_s = int(time.time())
                    current_boundary = now_s - (now_s % 300)
                    next_boundary = current_boundary + 300
                    secs_to_boundary = next_boundary - now_s

                    # Near boundary (within 3s): poll every 0.5s for precise capture
                    if secs_to_boundary <= 3 or (now_s - current_boundary) <= 2:
                        await asyncio.sleep(0.5)
                        # Check if we already have this boundary
                        boundary_ts = current_boundary if (now_s - current_boundary) <= 2 else next_boundary
                        if boundary_ts not in self.state.captured_anchors:
                            result = await self.chainlink_onchain.fetch_latest_price()
                            if result and result.price > 1000:
                                # Only capture if we're within 2s of the boundary
                                now_check = int(time.time())
                                if abs(now_check - boundary_ts) <= 2:
                                    self.state.captured_anchors[boundary_ts] = result.price
                                    self.state.update_chainlink_price(
                                        result.price,
                                        int(result.updated_at * 1000),
                                    )
                                    logger.info(
                                        "anchor_captured_proactive",
                                        window_ts=boundary_ts,
                                        price=float(result.price),
                                        age_s=round(result.age_seconds, 1),
                                    )
                                    # Prune old anchors
                                    while len(self.state.captured_anchors) > 20:
                                        oldest = min(self.state.captured_anchors)
                                        del self.state.captured_anchors[oldest]
                        continue

                    # Normal: refresh chainlink price every 5s when stale
                    await asyncio.sleep(5)
                    chainlink_age = time.monotonic() - self.state.chainlink_tick_time
                    if self.state.chainlink_price == 0 or chainlink_age > 10:
                        result = await self.chainlink_onchain.fetch_latest_price()
                        if result and result.price > 1000:
                            self.state.update_chainlink_price(
                                result.price,
                                int(result.updated_at * 1000),
                            )
                except asyncio.CancelledError:
                    break
                except Exception:
                    pass

        refresh_task = asyncio.create_task(chainlink_onchain_refresher())

        try:
            await self.chainlink_feed.start(
                on_chainlink_price,
                binance_rtds_callback=on_rtds_binance_price,
                anchor_batch_callback=on_anchor_batch,
            )
        finally:
            refresh_task.cancel()

    async def _resolve_via_api(
        self, old_market: MarketInfo, closing_price
    ):
        """Resolve old market positions using Polymarket API outcome (background).

        Polls the Gamma API for the authoritative UP/DOWN outcome, then
        settles positions accordingly. Falls back to local price comparison
        only if the API is unreachable after all retries.
        """
        try:
            # Poll API for authoritative outcome (retries built in)
            api_outcome = await self.market_discovery.fetch_resolved_outcome(
                old_market.event_slug
            )

            if api_outcome:
                btc_went_up = api_outcome.lower() == "up"
                source = "api"
            elif old_market.anchor_price and closing_price and closing_price > 0:
                # API failed — last resort: local price comparison
                btc_went_up = closing_price > old_market.anchor_price
                source = "local_fallback"
                logger.warning(
                    "resolution_api_failed_using_local",
                    anchor=float(old_market.anchor_price),
                    closing_price=float(closing_price),
                    market=old_market.event_slug,
                )
            else:
                logger.error(
                    "resolution_failed",
                    reason="no API outcome and no valid prices for fallback",
                    market=old_market.event_slug,
                )
                return

            winning_token = (
                old_market.token_id_up if btc_went_up
                else old_market.token_id_down
            )
            losing_token = (
                old_market.token_id_down if btc_went_up
                else old_market.token_id_up
            )

            logger.info(
                "market_resolution",
                source=source,
                outcome="UP" if btc_went_up else "DOWN",
                market=old_market.event_slug,
            )

            await self.trader.resolve_market(winning_token, losing_token)

        except Exception as e:
            logger.error(
                "background_resolution_error",
                market=old_market.event_slug,
                error=str(e),
            )

    async def _run_market_discovery(self):
        async def on_new_market(market: MarketInfo):
            old_market = self.state.current_market
            logger.info(
                "market_transition",
                old=old_market.event_slug if old_market else None,
                new=market.event_slug,
            )

            # Snapshot the closing price NOW and transition immediately.
            # Don't wait for end_time — that causes a 20-30s freeze.
            # Resolution accuracy comes from the Polymarket API (background),
            # not from exact timing of our price capture.
            # Prefer Chainlink (authoritative), but if stale (>30s old),
            # use RTDS Binance price (Polymarket's real-time BTC view).
            chainlink_stale = (
                time.monotonic() - self.state.chainlink_tick_time > 30
                if self.state.chainlink_tick_time > 0
                else True
            )
            if self.state.chainlink_price > 0 and not chainlink_stale:
                closing_price = self.state.chainlink_price
                closing_source = "chainlink"
            elif self.state.rtds_binance_price > 0:
                closing_price = self.state.rtds_binance_price
                closing_source = "rtds_binance"
            elif self.state.binance_price > 0:
                closing_price = self.state.binance_price
                closing_source = "binance_direct"
            else:
                closing_price = None
                closing_source = "none"

            if closing_price and old_market:
                logger.info(
                    "closing_price_captured",
                    source=closing_source,
                    price=float(closing_price),
                    chainlink_stale=chainlink_stale,
                    market=old_market.event_slug,
                )

            # --- TRANSITION TO NEW MARKET (no local resolution) ---
            # Positions from old market will be resolved in background via
            # Polymarket API (authoritative outcome). They show as "?" briefly.
            await self.order_manager.on_market_transition()

            self.state.current_market = market
            self.state.market_phase = MarketPhase.OPEN

            # Set anchor price FAST — don't block on slow sources.
            # Use instant sources as INTERIM anchor, then scrape page for
            # the AUTHORITATIVE priceToBeat in background.
            window_ts = int(market.start_time) - (int(market.start_time) % 300)
            anchor = None
            source = None

            # 1) RTDS batch capture at boundary (already in memory, instant)
            captured = self.state.get_captured_anchor(window_ts)
            if captured is not None:
                anchor = captured
                source = "rtds_batch"

            # 2) On-chain Chainlink (fast RPC call, <1s)
            if anchor is None:
                onchain = await self.chainlink_onchain.fetch_latest_price()
                if onchain and onchain.age_seconds < 30:
                    anchor = onchain.price
                    source = "chainlink_onchain"

            # 3) Current Chainlink price from state (instant)
            if anchor is None and self.state.chainlink_price > 1000:
                anchor = self.state.chainlink_price
                source = "chainlink_state"

            # 4) Current Binance price (last resort, instant)
            if anchor is None and self.state.binance_price > 0:
                anchor = self.state.binance_price
                source = "binance_current"

            if anchor is not None:
                market.anchor_price = anchor
                self.state.anchor_source = source
                self.state.anchor_is_authoritative = False  # Interim until scrape confirms
                logger.info(
                    "anchor_interim_set",
                    source=source,
                    anchor=float(anchor),
                    market=market.event_slug,
                )
            else:
                self.state.anchor_source = ""
                self.state.anchor_is_authoritative = False
                logger.warning(
                    "anchor_price_missing",
                    market=market.event_slug,
                )

            # Background: scrape Polymarket event page for authoritative priceToBeat.
            # This is the ONLY reliable source for the exact anchor price.
            # Strategy: priceToBeat[N] = finalPrice[N-1].
            # Phase 1: fast retries every 3s for ~30s (page may update quickly)
            # Phase 2: slower retries every 15s for ~150s (covers delayed updates)
            async def _scrape_authoritative_anchor(mkt, slug, prev_slug):
                """Scrape priceToBeat as the authoritative anchor source."""
                phases = [
                    (self.settings.anchor_scrape_phase1_attempts,
                     self.settings.anchor_scrape_phase1_interval_s),
                    (self.settings.anchor_scrape_phase2_attempts,
                     self.settings.anchor_scrape_phase2_interval_s),
                ]

                attempt_num = 0
                for count, interval in phases:
                    for _ in range(count):
                        attempt_num += 1
                        await asyncio.sleep(interval)

                        # Check if market changed (don't update stale market)
                        if self.state.current_market != mkt:
                            return

                        try:
                            price, scrape_source = await (
                                self.market_discovery.scrape_authoritative_anchor(
                                    slug, prev_slug
                                )
                            )
                            if price is not None:
                                old = float(mkt.anchor_price) if mkt.anchor_price else 0
                                delta = abs(old - float(price))
                                mkt.anchor_price = price
                                self.state.anchor_source = scrape_source
                                self.state.anchor_is_authoritative = True
                                logger.info(
                                    "anchor_authoritative_set",
                                    source=scrape_source,
                                    anchor=float(price),
                                    interim_delta=round(delta, 2),
                                    attempt=attempt_num,
                                    market=slug,
                                )
                                return
                        except Exception as e:
                            logger.debug(
                                "anchor_scrape_attempt_failed",
                                attempt=attempt_num,
                                error=str(e),
                            )

                # All retries exhausted — promote interim anchor
                self.state.anchor_is_authoritative = True
                logger.warning(
                    "anchor_scrape_exhausted",
                    slug=slug,
                    total_attempts=attempt_num,
                    using_interim=self.state.anchor_source,
                    anchor=float(mkt.anchor_price) if mkt.anchor_price else 0,
                )

            if self.settings.anchor_scrape_enabled:
                prev_slug = old_market.event_slug if old_market else None
                scrape_task = asyncio.create_task(
                    _scrape_authoritative_anchor(
                        market, market.event_slug, prev_slug
                    )
                )
                self._track_task(scrape_task, f"scrape_anchor_{market.event_slug}")
            else:
                # Scraping disabled — treat interim as authoritative
                self.state.anchor_is_authoritative = True

            # --- RESOLVE OLD MARKET VIA API (background) ---
            # Poll Polymarket API for authoritative outcome, then resolve positions.
            # This takes ~10-30s but doesn't block trading on the new market.
            if old_market:
                task = asyncio.create_task(
                    self._resolve_via_api(old_market, closing_price)
                )
                self._track_task(task, f"resolve_{old_market.event_slug}")

            # Re-subscribe Polymarket WS with new token IDs
            token_ids = [market.token_id_up, market.token_id_down]
            await self.polymarket_feed.update_subscription(token_ids)

            # Record market snapshot (no commit — flush loop handles it)
            if market.anchor_price:
                await self.db.insert_market_snapshot_deferred(
                    condition_id=market.condition_id,
                    anchor_price=market.anchor_price,
                    start_time=market.start_time,
                    end_time=market.end_time,
                )

            # Clear live trader fee cache on market transition
            if hasattr(self.trader, "clear_fee_cache"):
                self.trader.clear_fee_cache()

        # Start initial Polymarket WS subscription once first market is found
        first_market = True

        async def on_market_with_ws_init(market: MarketInfo):
            nonlocal first_market
            if first_market:
                # Start polymarket feed with initial tokens
                token_ids = [market.token_id_up, market.token_id_down]

                async def on_book(book, asset_id):
                    # Use current market (not captured first market) for token comparison
                    current = self.state.current_market
                    is_up = current is not None and asset_id == current.token_id_up
                    self.state.update_orderbook(book, is_up)
                    self.state.polymarket_connected = True

                async def on_trade(asset_id, price, size, ts):
                    if isinstance(self.trader, PaperTrader):
                        await self.trader.on_trade_event(asset_id, price, size, ts)

                # Start polymarket feed in a separate task
                task = asyncio.create_task(
                    self.polymarket_feed.start(token_ids, on_book, on_trade)
                )
                self._track_task(task, "polymarket_feed")
                first_market = False

            await on_new_market(market)

        await self.market_discovery.start(on_market_with_ws_init)

    async def _cleanup_loop(self):
        """Periodic maintenance: flush DB frequently, purge old ticks less often."""
        flush_count = 0
        while True:
            try:
                await self.db.flush()
                flush_count += 1

                # Purge price ticks older than 24 hours every 10 flushes (~5 min)
                if flush_count % 10 == 0:
                    cutoff_ms = int((time.time() - 86400) * 1000)
                    await self.db.purge_old_ticks(cutoff_ms)
            except Exception as e:
                logger.debug("cleanup_error", error=str(e))

            await asyncio.sleep(30)  # Flush every 30 seconds

    async def _on_risk_halt(self, reason: str):
        """Called when risk manager triggers a halt."""
        await self.order_manager.cancel_all()
        await self.alerts.risk_halt(reason)

    async def _wait_for_shutdown(self):
        """Wait for the shutdown event."""
        await self._shutdown_event.wait()


async def main():
    bot = Bot()

    # Handle SIGINT/SIGTERM
    loop = asyncio.get_running_loop()

    def signal_handler():
        logger.info("signal_received")
        bot._shutdown_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, signal_handler)
        except NotImplementedError:
            # Windows doesn't support add_signal_handler for asyncio loops.
            # KeyboardInterrupt is caught below instead.
            pass

    try:
        await bot.initialize()
        await bot.run()
    except KeyboardInterrupt:
        logger.info("keyboard_interrupt_shutdown")
    finally:
        await bot.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
