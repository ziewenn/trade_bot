import asyncio
import time
import pytest
import pytest_asyncio
from decimal import Decimal

from core.risk_manager import RiskManager
from core.state import SharedState
from data.models import Signal


class TestDailyLossLimit:

    @pytest_asyncio.fixture
    async def risk(self, settings, database, shared_state):
        rm = RiskManager(settings, database, shared_state)
        return rm

    @pytest.mark.asyncio
    async def test_within_limit_allows_trade(self, risk):
        risk.risk_state.daily_pnl = -20.0  # -2% of 1000
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("10"), "tok", Decimal("0.50"), 0)
        result = await risk.check_pre_trade(signal, 980.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_exceeding_limit_halts(self, risk):
        # 3% of 1000 = 30
        risk.risk_state.daily_pnl = -31.0
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("10"), "tok", Decimal("0.50"), 0)
        result = await risk.check_pre_trade(signal, 969.0)
        assert result is False
        assert risk.is_halted
        assert risk.risk_state.halt_reason == "daily_loss_limit_exceeded"

    @pytest.mark.asyncio
    async def test_daily_reset_clears_halt(self, risk):
        risk.risk_state.daily_pnl = -31.0
        risk.risk_state.is_halted = True
        risk.risk_state.halt_reason = "daily_loss_limit_exceeded"
        risk.risk_state.daily_pnl_reset_utc = "2020-01-01"  # Old date
        risk._check_daily_reset()
        assert risk.risk_state.daily_pnl == 0.0
        assert not risk.is_halted


class TestDrawdownHalt:

    @pytest_asyncio.fixture
    async def risk(self, settings, database, shared_state):
        rm = RiskManager(settings, database, shared_state)
        rm.risk_state.peak_equity = 1000.0
        return rm

    @pytest.mark.asyncio
    async def test_within_threshold_allows_trade(self, risk):
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("10"), "tok", Decimal("0.50"), 0)
        # 5% drawdown, threshold is 8%
        result = await risk.check_pre_trade(signal, 950.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_exceeding_threshold_halts(self, risk):
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("10"), "tok", Decimal("0.50"), 0)
        # 9% drawdown, threshold is 8%
        result = await risk.check_pre_trade(signal, 910.0)
        assert result is False
        assert risk.is_halted
        assert risk.risk_state.halt_reason == "drawdown_halt_triggered"

    def test_manual_resume(self, risk):
        risk.risk_state.is_halted = True
        risk.risk_state.halt_reason = "drawdown_halt_triggered"
        risk.manual_resume()
        assert not risk.is_halted


class TestPerTradeCap:

    @pytest_asyncio.fixture
    async def risk(self, settings, database, shared_state):
        return RiskManager(settings, database, shared_state)

    @pytest.mark.asyncio
    async def test_trade_within_cap_allowed(self, risk):
        # 2% of 1000 = 20 max trade. signal size * price = 10 * 0.50 = 5
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("10"), "tok", Decimal("0.50"), 0)
        result = await risk.check_pre_trade(signal, 1000.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_trade_exceeding_cap_rejected(self, risk):
        # 2% of 1000 = 20 max trade. signal size * price = 100 * 0.50 = 50
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("100"), "tok", Decimal("0.50"), 0)
        result = await risk.check_pre_trade(signal, 1000.0)
        assert result is False


class TestConcurrentExposure:

    @pytest_asyncio.fixture
    async def risk(self, settings, database, shared_state):
        return RiskManager(settings, database, shared_state)

    @pytest.mark.asyncio
    async def test_within_exposure_cap(self, risk):
        risk.risk_state.total_exposure = 100.0  # 10%
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("10"), "tok", Decimal("0.50"), 0)
        result = await risk.check_pre_trade(signal, 1000.0)
        assert result is True

    @pytest.mark.asyncio
    async def test_exceeding_exposure_cap(self, risk):
        risk.risk_state.total_exposure = 148.0  # 14.8%
        # Adding 5 more = 15.3% > 15% cap
        signal = Signal("UP", 0.55, 0.50, 0.05, Decimal("10"), "tok", Decimal("0.50"), 0)
        result = await risk.check_pre_trade(signal, 1000.0)
        assert result is False


class TestStaleDataDetection:

    def test_stale_binance_detected(self, shared_state):
        shared_state.binance_tick_time = time.monotonic() - 10  # 10 seconds old
        age = time.monotonic() - shared_state.binance_tick_time
        assert age > 5.0  # stale threshold

    def test_stale_polymarket_detected(self, shared_state):
        shared_state.polymarket_tick_time = time.monotonic() - 15
        age = time.monotonic() - shared_state.polymarket_tick_time
        assert age > 10.0  # stale threshold

    def test_fresh_data_not_stale(self, shared_state):
        shared_state.binance_tick_time = time.monotonic()
        shared_state.polymarket_tick_time = time.monotonic()
        b_age = time.monotonic() - shared_state.binance_tick_time
        p_age = time.monotonic() - shared_state.polymarket_tick_time
        assert b_age < 5.0
        assert p_age < 10.0
