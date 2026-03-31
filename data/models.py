from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum
from typing import Optional
import time
import uuid


class Side(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderStatus(Enum):
    PENDING = "pending"
    LIVE = "live"
    FILLED = "filled"
    PARTIALLY_FILLED = "partial"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class MarketPhase(Enum):
    PRE_OPEN = "pre_open"
    OPEN = "open"
    CLOSING = "closing"
    RESOLVED = "resolved"


class TradingMode(Enum):
    PAPER = "paper"
    LIVE = "live"


@dataclass(slots=True)
class BinanceTick:
    price: Decimal
    quantity: Decimal
    timestamp_ms: int
    is_buyer_maker: bool


@dataclass(slots=True)
class OrderBookLevel:
    price: Decimal
    size: Decimal


@dataclass(slots=True)
class OrderBook:
    asset_id: str
    bids: list[OrderBookLevel]
    asks: list[OrderBookLevel]
    timestamp_ms: int

    @property
    def best_bid(self) -> Optional[Decimal]:
        return self.bids[0].price if self.bids else None

    @property
    def best_ask(self) -> Optional[Decimal]:
        return self.asks[0].price if self.asks else None

    @property
    def mid_price(self) -> Optional[Decimal]:
        if self.best_bid is not None and self.best_ask is not None:
            return (self.best_bid + self.best_ask) / 2
        return None


@dataclass(slots=True)
class MarketInfo:
    event_slug: str
    condition_id: str
    token_id_up: str
    token_id_down: str
    start_time: float
    end_time: float
    anchor_price: Optional[Decimal] = None


@dataclass(slots=True)
class Signal:
    direction: str  # "UP" or "DOWN"
    true_prob: float
    market_prob: float
    edge: float
    kelly_size: Decimal
    token_id: str
    price: Decimal
    timestamp_ms: int


@dataclass(slots=True)
class Order:
    order_id: str
    token_id: str
    side: Side
    price: Decimal
    size: Decimal
    status: OrderStatus
    created_at: float = field(default_factory=time.time)
    filled_size: Decimal = Decimal("0")
    avg_fill_price: Decimal = Decimal("0")
    fee_rate_bps: int = 0
    market_condition_id: str = ""

    @staticmethod
    def generate_id() -> str:
        return str(uuid.uuid4())


@dataclass(slots=True)
class Position:
    token_id: str
    outcome: str  # "Up" or "Down"
    size: Decimal
    avg_entry_price: Decimal
    market_condition_id: str
    unrealized_pnl: Decimal = Decimal("0")
    realized_pnl: Decimal = Decimal("0")


@dataclass(slots=True)
class Trade:
    trade_id: str
    order_id: str
    token_id: str
    side: Side
    price: Decimal
    size: Decimal
    fee: Decimal
    pnl: Decimal
    timestamp: float
    is_paper: bool
    market_condition_id: str
    outcome: str = ""  # "Up" or "Down"

    @staticmethod
    def generate_id() -> str:
        return str(uuid.uuid4())


@dataclass
class RiskState:
    bankroll: float
    peak_equity: float
    starting_equity: float
    daily_pnl: float
    daily_pnl_reset_utc: str
    open_positions: dict[str, Position] = field(default_factory=dict)
    total_exposure: float = 0.0
    is_halted: bool = False
    halt_reason: Optional[str] = None
    last_binance_tick: float = 0.0
    last_polymarket_tick: float = 0.0
