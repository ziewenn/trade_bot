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
        self.state.starting_bankroll = self.settings.initial_bankroll

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
        if self.settings.anchor_scrape_playwright_fallback:
            await self.market_discovery.start_persistent_browser()

        # 6. Initialize strategy
        self.strategy = Strategy(self.settings)

        # 7. Initialize trader
        if self.settings.trading_mode == "live":
            live_trader = LiveTrader(
                private_key=self.settings.polymarket_private_key.get_secret_value(),
                funder_address=self.settings.polymarket_funder_address,
                initial_bankroll=self.settings.initial_bankroll,
                clob_url=self.settings.polymarket_clob_url,
            )
            await live_trader.initialize()
            self.trader = live_trader
            # Use real equity from Polymarket as starting bankroll
            real_balance = float(live_trader.current_bankroll)
            position_cost = sum(
                float(p.avg_entry_price * p.size) 
                for p in live_trader.current_positions.values()
            )
            total_equity = real_balance + position_cost
            if total_equity >= 0:
                self.state.starting_bankroll = total_equity
                self.settings.initial_bankroll = total_equity
                logger.info("live_equity_synced", equity=total_equity, cash=real_balance)
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

        # 8. Initialize risk manager
        self.risk_manager = RiskManager(self.settings, self.db, self.state)
        self.risk_manager.set_halt_callback(self._on_risk_halt)

        # 9. Initialize order manager
        self.order_manager = OrderManager(
            trader=self.trader,
            strategy=self.strategy,
            state=self.state,
            settings=self.settings,
            risk_manager=self.risk_manager,
        )

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

                # Web dashboard (always enabled for web UI control)
                from monitoring.web_dashboard import WebDashboard

                web = WebDashboard(
                    self.settings,
                    self.state,
                    self.risk_manager,
                    self.order_manager,
                    self.alerts,
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
            self.state.update_chainlink_price(price, timestamp_ms, source="RTDS")
            self.state.chainlink_connected = True

        async def on_rtds_binance_price(price, timestamp_ms):
            self.state.update_rtds_binance_price(price, timestamp_ms)
            self.state.chainlink_connected = True  # RTDS connection is alive

        async def on_anchor_batch(data_array):
            """Capture anchor price at 5-min boundaries from RTDS batch.

            The RTDS `crypto_prices` `btc/usd` batch entry at `ts % 300 == 0`
            is the Chainlink Data Streams price at that boundary — the same
            feed Polymarket uses for priceToBeat. Background scrape verifies
            with Gamma API's eventMetadata.priceToBeat when it becomes available.
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

            # Always track the latest RTDS btc/usd price for pre-boundary snapshot.
            # This is the Chainlink Data Streams price — same as Polymarket's display.
            # At boundary crossings, the last value = next cycle's anchor.
            if data_array:
                last_entry = data_array[-1]
                try:
                    price = Decimal(str(last_entry["value"]))
                    ts_ms = last_entry.get("timestamp", 0)
                    if price > 1000:
                        self.state.update_rtds_btcusd_price(price, ts_ms)
                except (ValueError, KeyError):
                    pass

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
                    # NOTE: On-chain Chainlink is NOT authoritative for anchors
                    # (only updates on 0.5% deviation). This is a last-resort
                    # fallback — the pre-boundary RTDS snapshot is preferred.
                    if secs_to_boundary <= 3 or (now_s - current_boundary) <= 2:
                        await asyncio.sleep(0.5)
                        # Check if we already have this boundary from RTDS sources
                        boundary_ts = current_boundary if (now_s - current_boundary) <= 2 else next_boundary
                        if boundary_ts not in self.state.captured_anchors and boundary_ts not in self.state.pre_boundary_anchors:
                            result = await self.chainlink_onchain.fetch_latest_price()
                            if result and result.price > 1000:
                                # Only capture if we're within 2s of the boundary
                                now_check = int(time.time())
                                if abs(now_check - boundary_ts) <= 2:
                                    self.state.captured_anchors[boundary_ts] = result.price
                                    self.state.update_chainlink_price(
                                        result.price,
                                        int(result.updated_at * 1000),
                                        source="ON-CHAIN",
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

                    # Check RTDS Chainlink staleness — poll aggressively when stale
                    rtds_age = time.monotonic() - self.state.chainlink_rtds_tick_time if self.state.chainlink_rtds_tick_time > 0 else float("inf")
                    chainlink_age = time.monotonic() - self.state.chainlink_tick_time if self.state.chainlink_tick_time > 0 else float("inf")

                    if self.state.chainlink_price == 0 or rtds_age > 10:
                        # RTDS stale: poll on-chain every 2s for fast fallback
                        await asyncio.sleep(2)
                        result = await self.chainlink_onchain.fetch_latest_price()
                        if result and result.price > 1000:
                            self.state.update_chainlink_price(
                                result.price,
                                int(result.updated_at * 1000),
                                source="ON-CHAIN",
                            )
                    else:
                        # RTDS healthy: check less frequently
                        await asyncio.sleep(5)
                except asyncio.CancelledError:
                    break
                except Exception:
                    pass

        # Background task: snapshot last RTDS btc/usd price at 5-min boundaries
        async def rtds_boundary_watcher():
            """Watch for 5-min boundary crossings and snapshot the last RTDS
            btc/usd price as the pre-boundary anchor.

            The RTDS `crypto_prices` `btc/usd` feed is the Chainlink Data
            Streams price. It is an APPROXIMATION of the anchor — close but
            not exact (can differ by $10-50 from actual priceToBeat). Used
            as an interim value until the Gamma API confirms the real anchor.
            """
            last_boundary = int(time.time()) // 300 * 300

            while True:
                try:
                    await asyncio.sleep(0.2)  # Check 5x/sec
                    now_s = int(time.time())
                    current_boundary = now_s // 300 * 300

                    if current_boundary > last_boundary:
                        # Boundary crossed! Snapshot the last RTDS btc/usd price.
                        price = self.state.rtds_btcusd_last_price
                        mono = self.state.rtds_btcusd_last_mono
                        age = time.monotonic() - mono if mono > 0 else float("inf")

                        if price > 1000 and age < 5.0:
                            # Fresh: received within last 5 seconds
                            self.state.pre_boundary_anchors[current_boundary] = price
                            logger.info(
                                "pre_boundary_anchor_captured",
                                window_ts=current_boundary,
                                price=float(price),
                                age_s=round(age, 2),
                            )
                        elif price > 1000 and age < 15.0:
                            # Stale but usable
                            self.state.pre_boundary_anchors[current_boundary] = price
                            logger.warning(
                                "pre_boundary_anchor_stale",
                                window_ts=current_boundary,
                                price=float(price),
                                age_s=round(age, 2),
                            )
                        else:
                            logger.warning(
                                "pre_boundary_anchor_missed",
                                window_ts=current_boundary,
                                rtds_age_s=round(age, 2) if mono > 0 else "never",
                            )

                        # Prune old entries
                        while len(self.state.pre_boundary_anchors) > 20:
                            oldest = min(self.state.pre_boundary_anchors)
                            del self.state.pre_boundary_anchors[oldest]

                        # Pre-emptive anchor scrape: start immediately at
                        # boundary crossing (T+0), before market discovery
                        # finds the new market. The previous slug's page
                        # closePrice is available right now.
                        if self.settings.anchor_scrape_enabled:
                            new_slug = f"btc-updown-5m-{current_boundary}"
                            old_slug = f"btc-updown-5m-{current_boundary - 300}"
                            task = asyncio.create_task(
                                self._preemptive_anchor_scrape(
                                    current_boundary, new_slug, old_slug
                                )
                            )
                            self._track_task(
                                task, f"preemptive_anchor_{current_boundary}"
                            )

                        last_boundary = current_boundary
                except asyncio.CancelledError:
                    break
                except Exception:
                    pass

        refresh_task = asyncio.create_task(chainlink_onchain_refresher())
        boundary_task = asyncio.create_task(rtds_boundary_watcher())

        try:
            await self.chainlink_feed.start(
                on_chainlink_price,
                binance_rtds_callback=on_rtds_binance_price,
                anchor_batch_callback=on_anchor_batch,
            )
        finally:
            refresh_task.cancel()
            boundary_task.cancel()

    async def _preemptive_anchor_scrape(
        self, boundary_ts: int, new_slug: str, old_slug: str
    ):
        """Scrape anchor at boundary crossing, before market discovery.

        Triggered by rtds_boundary_watcher at T+0. Retries up to 5 times
        with 2s intervals (~10s window) to allow the Polymarket CDN to
        refresh with the new closePrice/openPrice.

        Stores result in pre_scraped_anchors for on_new_market to pick up.
        If on_new_market already ran (set a non-authoritative anchor),
        directly updates the live market state so trading unblocks immediately.
        """
        max_attempts = 5
        retry_interval = 1.0

        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                await asyncio.sleep(retry_interval)

            # If anchor already became authoritative (e.g. on_new_market got
            # Gamma API priceToBeat, or background scrape succeeded), stop.
            if self.state.anchor_is_authoritative:
                logger.debug(
                    "preemptive_scrape_stopped_already_authoritative",
                    boundary=boundary_ts,
                    source=self.state.anchor_source,
                )
                return

            try:
                price, source = await (
                    self.market_discovery.scrape_authoritative_anchor(
                        new_slug, old_slug
                    )
                )
                if price is not None:
                    self.state.pre_scraped_anchors[boundary_ts] = (price, source)

                    # If on_new_market already ran and set a non-authoritative
                    # anchor, directly upgrade the live market state now.
                    mkt = self.state.current_market
                    if (
                        mkt is not None
                        and mkt.event_slug == new_slug
                        and not self.state.anchor_is_authoritative
                    ):
                        old_anchor = float(mkt.anchor_price) if mkt.anchor_price else 0
                        mkt.anchor_price = price
                        self.state.anchor_source = source
                        self.state.anchor_is_authoritative = True
                        self.state.anchor_set_mono = time.monotonic()
                        logger.info(
                            "preemptive_anchor_live_upgrade",
                            boundary=boundary_ts,
                            slug=new_slug,
                            price=float(price),
                            source=source,
                            attempt=attempt,
                            interim_delta=round(abs(old_anchor - float(price)), 2),
                        )
                    else:
                        logger.info(
                            "preemptive_anchor_scraped",
                            boundary=boundary_ts,
                            slug=new_slug,
                            price=float(price),
                            source=source,
                            attempt=attempt,
                        )
                    return
            except Exception as e:
                logger.debug(
                    "preemptive_anchor_scrape_attempt_failed",
                    boundary=boundary_ts,
                    attempt=attempt,
                    error=str(e),
                )

        logger.warning(
            "preemptive_anchor_scrape_exhausted",
            boundary=boundary_ts,
            slug=new_slug,
            attempts=max_attempts,
        )

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

            # Check positions for P&L alert before resolving
            total_cost = 0.0
            total_payout = 0.0
            had_positions = False
            for token_id in [winning_token, losing_token]:
                if token_id in self.state.open_positions:
                    pos = self.state.open_positions[token_id]
                    # size is number of shares, avg_entry_price is decimal
                    cost = float(pos.size * pos.avg_entry_price)
                    payout = float(pos.size) if token_id == winning_token else 0.0
                    total_cost += cost
                    total_payout += payout
                    had_positions = True

            pnl = total_payout - total_cost

            await self.trader.resolve_market(
                winning_token, losing_token,
                outcome="UP" if btc_went_up else "DOWN",
            )

            # Send Telegram Alert if we had positions
            if had_positions:
                emoji = "🎉" if pnl > 0 else ("💀" if pnl < 0 else "😐")
                status = "WON" if pnl > 0 else ("LOST" if pnl < 0 else "BREAK EVEN")
                msg = (
                    f"{emoji} <b>Trade Resolved: {status}</b>\n"
                    f"Market: {old_market.event_slug}\n"
                    f"Position Cost: ${total_cost:.2f}\n"
                    f"Payout: ${total_payout:.2f}\n"
                    f"Net P&L: <b>${pnl:+.2f}</b>"
                )
                asyncio.create_task(self.alerts.send_alert(msg))

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
            # Prefer Chainlink Data Streams (rtds_btcusd) — this is the EXACT
            # feed Polymarket uses for priceToBeat/resolution. The raw Chainlink
            # oracle price can be ~$10-20 off from Data Streams.
            # Fallback chain: Data Streams > raw Chainlink > RTDS Binance > Binance
            rtds_btcusd_stale = (
                time.monotonic() - self.state.rtds_btcusd_last_mono > 10
                if self.state.rtds_btcusd_last_mono > 0
                else True
            )
            chainlink_stale = (
                time.monotonic() - self.state.chainlink_tick_time > 30
                if self.state.chainlink_tick_time > 0
                else True
            )
            if self.state.rtds_btcusd_last_price > 0 and not rtds_btcusd_stale:
                closing_price = self.state.rtds_btcusd_last_price
                closing_source = "chainlink_data_streams"
            elif self.state.chainlink_price > 0 and not chainlink_stale:
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

            # Set anchor price for new cycle.
            # Key insight: priceToBeat[N] = finalPrice[N-1] = closing price
            # of the previous cycle. We already captured closing_price above
            # at the exact transition moment, so use it directly.
            # Background scrape will verify with Gamma API priceToBeat.
            window_ts = int(market.start_time) - (int(market.start_time) % 300)
            anchor = None
            source = None
            is_authoritative = False

            # 0) Gamma API priceToBeat (from _parse_event) — exact anchor
            # Populated if the Gamma API returned eventMetadata.priceToBeat
            # when the market was discovered (usually only after ~60s).
            if market.anchor_price is not None and market.anchor_price > 1000:
                anchor = market.anchor_price
                source = "gamma_api_priceToBeat"
                is_authoritative = True

            # 1) Closing price of previous cycle — interim estimate only.
            # Can be ~$30+ off from Polymarket's actual priceToBeat.
            # NOT authoritative: trading stays blocked until scrape succeeds.
            if anchor is None and closing_price is not None and closing_price > 1000:
                anchor = closing_price
                source = f"prev_closing_{closing_source}"
                is_authoritative = False

            # 2) Pre-scraped anchor from boundary watcher (launched at T+0)
            if anchor is None:
                pre_scraped = self.state.pre_scraped_anchors.get(window_ts)
                if pre_scraped is not None:
                    anchor, source = pre_scraped
                    is_authoritative = True

            # 3) RTDS batch exact match (ts % 300 == 0)
            if anchor is None:
                captured = self.state.get_captured_anchor(window_ts)
                if captured is not None:
                    anchor = captured
                    source = "rtds_batch"
                    is_authoritative = True

            # 4) Pre-boundary snapshot (last RTDS btc/usd before crossing)
            if anchor is None:
                pre_boundary = self.state.pre_boundary_anchors.get(window_ts)
                if pre_boundary is not None:
                    anchor = pre_boundary
                    source = "rtds_pre_boundary"
                    is_authoritative = True

            # 5) On-chain Chainlink — slightly stale but usable
            if anchor is None:
                onchain = await self.chainlink_onchain.fetch_latest_price()
                if onchain and onchain.age_seconds < 30:
                    anchor = onchain.price
                    source = "chainlink_onchain"
                    is_authoritative = False

            # 6) Current Chainlink state — last resort
            if anchor is None and self.state.chainlink_price > 1000:
                anchor = self.state.chainlink_price
                source = "chainlink_state"
                is_authoritative = False

            # NOTE: Binance intentionally NOT used as anchor source.
            # It's a different price feed and can diverge from Chainlink.

            if anchor is not None:
                market.anchor_price = anchor
                self.state.anchor_source = source
                self.state.anchor_is_authoritative = is_authoritative
                if is_authoritative:
                    self.state.anchor_set_mono = time.monotonic()
                log_fn = logger.info if is_authoritative else logger.warning
                log_fn(
                    "anchor_set",
                    source=source,
                    authoritative=is_authoritative,
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

            # Background: fetch exact priceToBeat from page scrape / Gamma API.
            # Corrects the interim anchor (rtds_batch/pre_boundary) if the
            # actual priceToBeat differs. Trading is NOT blocked while waiting.
            # CDN/Gamma API typically refresh ~60-75s after cycle transition.
            async def _scrape_authoritative_anchor(mkt, slug, prev_slug):
                """Scrape priceToBeat as the authoritative anchor source."""
                window_ts = int(mkt.start_time) - (int(mkt.start_time) % 300)

                # Check if preemptive scrape or another source already set authoritative
                if self.state.anchor_is_authoritative:
                    logger.debug(
                        "scrape_skipped_already_authoritative",
                        slug=slug,
                        source=self.state.anchor_source,
                    )
                    return

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
                        # First attempt immediate, then sleep between retries
                        if attempt_num > 1:
                            await asyncio.sleep(interval)

                        # Check if market changed (don't update stale market)
                        if self.state.current_market != mkt:
                            return

                        # Check if preemptive scrape upgraded anchor while we slept
                        if self.state.anchor_is_authoritative:
                            logger.debug(
                                "scrape_stopped_authoritative_during_retry",
                                slug=slug,
                                source=self.state.anchor_source,
                                attempt=attempt_num,
                            )
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
                                self.state.anchor_set_mono = time.monotonic()
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
                self.state.anchor_set_mono = time.monotonic()
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
                self.state.anchor_set_mono = time.monotonic()

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
                    if hasattr(self.trader, "on_trade_event"):
                        await self.trader.on_trade_event(asset_id, price, size, ts)

                # Start polymarket feed in a separate task
                task = asyncio.create_task(
                    self.polymarket_feed.start(token_ids, on_book, on_trade)
                )
                self._track_task(task, "polymarket_feed")
                first_market = False

            await on_new_market(market)

        # Launch persistent Playwright browser for instant anchor scraping.
        # The page stays open and auto-updates via Polymarket's client-side JS.
        now_s = int(time.time())
        current_boundary = now_s - (now_s % 300)
        initial_slug = f"btc-updown-5m-{current_boundary}"
        await self.market_discovery.start_persistent_browser(initial_slug)

        await self.market_discovery.start(on_market_with_ws_init)

    async def _cleanup_loop(self):
        """Periodic maintenance: flush DB frequently, purge old ticks less often."""
        flush_count = 0
        while True:
            try:
                await self.db.flush()
                flush_count += 1

                # Save daily stats every flush
                from datetime import datetime, timezone
                today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                equity = float(self.state.paper_bankroll) + sum(
                    float(p.avg_entry_price * p.size)
                    for p in self.state.open_positions.values()
                )
                await self.db.upsert_daily_stats(
                    date_str=today,
                    total_trades=self.state.total_trades,
                    winning_trades=self.state.winning_trades,
                    total_pnl=self.state.session_pnl,
                    max_drawdown=Decimal(str(round(self.risk_manager.drawdown_from_peak, 6))),
                    peak_equity=Decimal(str(round(self.risk_manager.risk_state.peak_equity, 2))),
                    closing_equity=Decimal(str(round(equity, 2))),
                    is_paper=self.settings.trading_mode != "live",
                )

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
