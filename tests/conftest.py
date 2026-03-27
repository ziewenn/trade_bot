import asyncio
import os
import sys
import pytest
import pytest_asyncio

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config.settings import Settings
from data.database import Database
from core.state import SharedState


@pytest.fixture
def settings():
    """Default paper-mode settings for testing."""
    return Settings(
        trading_mode="paper",
        initial_bankroll=1000.0,
        max_position_pct=0.02,
        daily_loss_limit_pct=0.03,
        drawdown_halt_pct=0.08,
        kelly_fraction=0.25,
        min_edge_pct=0.02,
        max_concurrent_exposure_pct=0.15,
        volatility_per_second=0.0005,
        market_entry_window_sec=240,
        market_entry_min_elapsed_sec=180,
        market_exit_buffer_sec=30,
        min_price_gap_usd=50.0,
        binance_prediction_weight=0.4,
        order_offset_cents=0.01,
        cancel_replace_interval_ms=500,
    )


@pytest_asyncio.fixture
async def database(tmp_path):
    """In-memory SQLite database for testing."""
    db_path = str(tmp_path / "test.db")
    db = Database(db_path)
    await db.connect()
    yield db
    await db.close()


@pytest.fixture
def shared_state():
    """Fresh shared state for testing."""
    return SharedState()
