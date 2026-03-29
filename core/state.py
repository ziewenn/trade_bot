import time
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Optional

from data.models import MarketInfo, MarketPhase, OrderBook, Order, Position


@dataclass
class SharedState:
    """Single mutable state object shared across all components.

    Feeds write to this, strategy reads from it.
    Uses atomic reference replacement — no locks needed for reads.
    """

    # Current market
    current_market: Optional[MarketInfo] = None
    market_phase: MarketPhase = MarketPhase.PRE_OPEN

    # Binance price feed
    binance_price: Decimal = Decimal("0")
    binance_tick_time: float = 0.0  # time.monotonic()
    binance_connected: bool = False

    # Chainlink oracle price (infrequent — only on ~0.1% deviation)
    chainlink_price: Decimal = Decimal("0")
    chainlink_tick_time: float = 0.0
    chainlink_connected: bool = False
    chainlink_source: str = ""  # "RTDS" or "ON-CHAIN" — which source last updated chainlink_price

    # On-chain Chainlink (separate tracking for fallback monitoring)
    chainlink_rtds_tick_time: float = 0.0  # last RTDS Chainlink update
    chainlink_onchain_tick_time: float = 0.0  # last on-chain Chainlink update

    # RTDS Binance price (frequent — updates every few seconds)
    # This is Polymarket's view of BTC price, used as proxy when
    # Chainlink is stale and for anchor capture at market boundaries.
    rtds_binance_price: Decimal = Decimal("0")
    rtds_binance_tick_time: float = 0.0

    # Polymarket orderbooks
    orderbook_up: Optional[OrderBook] = None
    orderbook_down: Optional[OrderBook] = None
    polymarket_tick_time: float = 0.0
    polymarket_connected: bool = False

    # Rolling Binance price window for volatility
    price_history: list[tuple[int, Decimal]] = field(default_factory=list)
    # (timestamp_ms, price)

    # Active orders and positions
    open_orders: dict[str, Order] = field(default_factory=dict)
    open_positions: dict[str, Position] = field(default_factory=dict)

    # Session stats
    session_start: float = field(default_factory=time.time)
    total_trades: int = 0
    winning_trades: int = 0
    session_pnl: Decimal = Decimal("0")
    paper_bankroll: Decimal = Decimal("0")  # Synced from PaperTrader
    starting_bankroll: float = 0.0  # Set once at init for dashboard verification
    trading_paused: bool = False  # Web dashboard start/stop control

    # Recent trades for dashboard display: list of (direction, pnl, cost, timestamp)
    recent_trades: list[dict] = field(default_factory=list)

    # RTDS btc/usd (Chainlink Data Streams) continuous tracking
    # This is close to Polymarket's display price but NOT exact for anchoring.
    # Used as interim approximation; authoritative anchor comes from Gamma API.
    rtds_btcusd_last_price: Decimal = Decimal("0")
    rtds_btcusd_last_ts_ms: int = 0
    rtds_btcusd_last_mono: float = 0.0  # time.monotonic() when received

    # Captured anchor prices at 5-minute boundaries
    # Maps window_start_ts (int) -> Chainlink BTC/USD price (Decimal)
    # Populated by the Chainlink callback on each boundary crossing
    captured_anchors: dict[int, Decimal] = field(default_factory=dict)

    # Pre-boundary anchor snapshots (last RTDS btc/usd before each boundary)
    # Maps window_start_ts (int) -> last Data Streams price before crossing
    pre_boundary_anchors: dict[int, Decimal] = field(default_factory=dict)

    # Pre-scraped authoritative anchors (from boundary watcher at T+0)
    # Maps window_start_ts (int) -> (price, source_label)
    # Populated by _preemptive_anchor_scrape, consumed by on_new_market
    pre_scraped_anchors: dict[int, tuple] = field(default_factory=dict)

    # Anchor confidence tracking
    anchor_source: str = ""                  # e.g. "rtds_batch", "rtds_pre_boundary", "scraped_priceToBeat"
    anchor_is_authoritative: bool = False    # True for rtds_batch, pre_boundary, scraped, gamma_api sources
    anchor_set_mono: float = 0.0            # time.monotonic() when authoritative anchor was set

    # Strategy state
    current_true_prob: float = 0.5
    current_edge: float = 0.0
    current_signal_direction: Optional[str] = None

    def update_binance_price(self, price: Decimal, timestamp_ms: int):
        self.binance_price = price
        self.binance_tick_time = time.monotonic()
        self.price_history.append((timestamp_ms, price))
        # Trim to last 60 seconds of data
        cutoff = timestamp_ms - 60_000
        self.price_history = [
            (ts, p) for ts, p in self.price_history if ts >= cutoff
        ]

    def update_chainlink_price(self, price: Decimal, timestamp_ms: int, source: str = ""):
        self.chainlink_price = price
        self.chainlink_tick_time = time.monotonic()
        if source:
            self.chainlink_source = source
        if source == "RTDS":
            self.chainlink_rtds_tick_time = time.monotonic()
        elif source == "ON-CHAIN":
            self.chainlink_onchain_tick_time = time.monotonic()

    def update_rtds_btcusd_price(self, price: Decimal, timestamp_ms: int):
        """Track the latest RTDS btc/usd (Chainlink Data Streams) price.

        Continuously overwritten. At boundary crossings, the last value
        becomes the pre-boundary anchor snapshot (approximate, not exact).
        """
        self.rtds_btcusd_last_price = price
        self.rtds_btcusd_last_ts_ms = timestamp_ms
        self.rtds_btcusd_last_mono = time.monotonic()

    def update_rtds_binance_price(self, price: Decimal, timestamp_ms: int):
        self.rtds_binance_price = price
        self.rtds_binance_tick_time = time.monotonic()

    # Note: anchor capture logic moved to main.py on_anchor_batch callback
    # which fetches on-chain Chainlink at boundary detection for max accuracy.

    def get_captured_anchor(self, window_ts: int) -> Optional[Decimal]:
        """Get the captured Chainlink anchor price for a specific window."""
        return self.captured_anchors.get(window_ts)

    def update_orderbook(self, book: OrderBook, is_up_token: bool):
        if is_up_token:
            self.orderbook_up = book
        else:
            self.orderbook_down = book
        self.polymarket_tick_time = time.monotonic()

    def get_remaining_seconds(self) -> float:
        if not self.current_market:
            return 0.0
        return max(0.0, self.current_market.end_time - time.time())

    def get_elapsed_seconds(self) -> float:
        if not self.current_market:
            return 0.0
        return max(0.0, time.time() - self.current_market.start_time)

    @property
    def best_bid_up(self) -> Optional[Decimal]:
        return self.orderbook_up.best_bid if self.orderbook_up else None

    @property
    def best_ask_up(self) -> Optional[Decimal]:
        return self.orderbook_up.best_ask if self.orderbook_up else None

    @property
    def best_bid_down(self) -> Optional[Decimal]:
        return self.orderbook_down.best_bid if self.orderbook_down else None

    @property
    def best_ask_down(self) -> Optional[Decimal]:
        return self.orderbook_down.best_ask if self.orderbook_down else None
