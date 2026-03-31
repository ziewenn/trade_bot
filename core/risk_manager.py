import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, Awaitable, Optional

from config.settings import Settings
from core.state import SharedState
from data.database import Database
from data.models import RiskState, Signal
from monitoring.logger import get_logger

logger = get_logger("risk_manager")


class RiskManager:
    """Position sizing enforcement, loss limits, kill switches, stale data detection."""

    def __init__(self, settings: Settings, database: Database, state: SharedState):
        self.settings = settings
        self.db = database
        self.state = state

        self.risk_state = RiskState(
            bankroll=settings.initial_bankroll,
            peak_equity=settings.initial_bankroll,
            starting_equity=settings.initial_bankroll,
            daily_pnl=0.0,
            daily_pnl_reset_utc=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        )
        self._peak_initialized = False
        self._on_halt_callback: Optional[Callable[[str], Awaitable[None]]] = None

    def set_halt_callback(self, callback: Callable[[str], Awaitable[None]]):
        """Set callback to invoke when a halt is triggered (e.g., cancel all orders + alert)."""
        self._on_halt_callback = callback

    async def check_pre_trade(self, signal: Signal, current_equity: float) -> bool:
        """Run all pre-trade risk checks. Returns True if trade is allowed."""
        if self.risk_state.is_halted:
            return False

        # Check daily reset
        self._check_daily_reset()

        # 1. Daily loss limit
        if not self._check_daily_loss():
            await self._trigger_halt("daily_loss_limit_exceeded")
            return False

        # 2. Drawdown halt
        if not self._check_drawdown(current_equity):
            await self._trigger_halt("drawdown_halt_triggered")
            return False

        # 3. Per-trade cap
        # trade_value = dollar cost of this trade
        # signal.kelly_size = number of shares (converted from dollars in strategy.py)
        # signal.price = cost per share
        # So: shares × price_per_share = total dollar cost
        trade_value = float(signal.kelly_size) * float(signal.price)
        max_trade = self.risk_state.bankroll * self.settings.max_position_pct
        if trade_value > max_trade:
            logger.warning(
                "per_trade_cap_exceeded",
                trade_value=trade_value,
                max_trade=max_trade,
            )
            return False

        # 4. Concurrent exposure cap (all values in dollar terms)
        # risk_state.total_exposure is set by order_manager as sum(p.size * p.avg_entry_price)
        # trade_value above is also in dollars — units are consistent
        total_exposure = self.risk_state.total_exposure + trade_value
        max_exposure = self.risk_state.bankroll * self.settings.max_concurrent_exposure_pct
        if total_exposure > max_exposure:
            logger.warning(
                "concurrent_exposure_exceeded",
                total_exposure=total_exposure,
                max_exposure=max_exposure,
            )
            return False

        return True

    async def run_stale_data_watchdog(
        self,
        cancel_callback: Callable[[], Awaitable[int]],
    ):
        """Continuously check for stale data and cancel orders if detected."""
        logger.info("stale_data_watchdog_started")

        was_stale_binance = False
        was_stale_poly = False
        was_stale_chainlink = False
        was_emergency_chainlink = False

        while True:
            try:
                now = time.monotonic()

                # Binance stale check
                binance_age = now - self.state.binance_tick_time if self.state.binance_tick_time > 0 else 0
                if self.state.binance_tick_time > 0 and binance_age > self.settings.stale_binance_threshold_s:
                    if not was_stale_binance:
                        logger.warning(
                            "stale_binance_data",
                            seconds_since_last=round(binance_age, 1),
                        )
                        await cancel_callback()
                        was_stale_binance = True
                else:
                    if was_stale_binance:
                        logger.info("binance_data_resumed")
                    was_stale_binance = False

                # Polymarket stale check
                poly_age = now - self.state.polymarket_tick_time if self.state.polymarket_tick_time > 0 else 0
                if self.state.polymarket_tick_time > 0 and poly_age > self.settings.stale_polymarket_threshold_s:
                    if not was_stale_poly:
                        logger.warning(
                            "stale_polymarket_data",
                            seconds_since_last=round(poly_age, 1),
                        )
                        await cancel_callback()
                        was_stale_poly = True
                else:
                    if was_stale_poly:
                        logger.info("polymarket_data_resumed")
                    was_stale_poly = False

                # Chainlink stale check (any source — RTDS or on-chain)
                chainlink_age = now - self.state.chainlink_tick_time if self.state.chainlink_tick_time > 0 else 0
                if self.state.chainlink_tick_time > 0 and chainlink_age > self.settings.stale_chainlink_threshold_s:
                    if not was_stale_chainlink:
                        logger.warning(
                            "stale_chainlink_data",
                            seconds_since_last=round(chainlink_age, 1),
                            source=self.state.chainlink_source,
                        )
                        await cancel_callback()
                        was_stale_chainlink = True

                    # Emergency: BOTH sources stale > 30s → full halt
                    if chainlink_age > self.settings.chainlink_emergency_stale_s:
                        if not was_emergency_chainlink:
                            logger.error(
                                "chainlink_emergency_stale",
                                seconds_since_last=round(chainlink_age, 1),
                            )
                            await self._trigger_halt("chainlink_all_stale")
                            was_emergency_chainlink = True
                else:
                    if was_stale_chainlink:
                        logger.info(
                            "chainlink_data_resumed",
                            source=self.state.chainlink_source,
                        )
                    was_stale_chainlink = False

                    # Auto-resume from Chainlink emergency halt
                    if was_emergency_chainlink:
                        if self.risk_state.is_halted and self.risk_state.halt_reason == "chainlink_all_stale":
                            self.risk_state.is_halted = False
                            self.risk_state.halt_reason = None
                            logger.info("chainlink_emergency_halt_cleared")
                        was_emergency_chainlink = False

                # Update risk state timestamps
                self.risk_state.last_binance_tick = self.state.binance_tick_time
                self.risk_state.last_polymarket_tick = self.state.polymarket_tick_time

            except Exception as e:
                logger.error("stale_watchdog_error", error=str(e))

            await asyncio.sleep(1.0)

    def update_pnl(self, realized_pnl: float):
        """Update daily P&L tracking."""
        self._check_daily_reset()
        self.risk_state.daily_pnl += realized_pnl

    def update_equity(self, current_equity: float):
        """Update peak equity and bankroll tracking."""
        if not self._peak_initialized and current_equity > 0:
            self.risk_state.peak_equity = current_equity
            self.risk_state.starting_equity = current_equity
            self._peak_initialized = True
            logger.info("equity_initialized_from_exchange", initial_equity=current_equity)
            
        self.risk_state.bankroll = current_equity
        if current_equity > self.risk_state.peak_equity:
            self.risk_state.peak_equity = current_equity

    def update_exposure(self, total_exposure: float):
        """Update total open position exposure."""
        self.risk_state.total_exposure = total_exposure

    def _check_daily_loss(self) -> bool:
        """Check if daily loss limit is breached."""
        limit = self.risk_state.bankroll * self.settings.daily_loss_limit_pct
        return self.risk_state.daily_pnl >= -limit

    def _check_drawdown(self, current_equity: float) -> bool:
        """Check if drawdown from session baseline exceeds threshold."""
        if self.risk_state.starting_equity <= 0:
            return True
        drawdown = 1.0 - (current_equity / self.risk_state.starting_equity)
        return drawdown < self.settings.drawdown_halt_pct

    def _check_daily_reset(self):
        """Reset daily P&L at UTC midnight."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.risk_state.daily_pnl_reset_utc:
            logger.info(
                "daily_pnl_reset",
                previous_date=self.risk_state.daily_pnl_reset_utc,
                previous_pnl=self.risk_state.daily_pnl,
            )
            self.risk_state.daily_pnl = 0.0
            self.risk_state.daily_pnl_reset_utc = today

            # Clear daily halt if it was a daily loss halt
            if self.risk_state.is_halted and self.risk_state.halt_reason == "daily_loss_limit_exceeded":
                self.risk_state.is_halted = False
                self.risk_state.halt_reason = None
                logger.info("daily_loss_halt_cleared")

    async def _trigger_halt(self, reason: str):
        """Activate kill switch."""
        self.risk_state.is_halted = True
        self.risk_state.halt_reason = reason
        logger.error("risk_halt_triggered", reason=reason)

        if self._on_halt_callback:
            await self._on_halt_callback(reason)

    def manual_resume(self):
        """Manually resume trading after a drawdown halt. Call from dashboard/CLI."""
        if self.risk_state.is_halted and self.risk_state.halt_reason == "drawdown_halt_triggered":
            self.risk_state.is_halted = False
            self.risk_state.halt_reason = None
            logger.info("manual_resume_from_drawdown_halt")

    @property
    def is_halted(self) -> bool:
        return self.risk_state.is_halted

    @property
    def daily_pnl(self) -> float:
        return self.risk_state.daily_pnl

    @property
    def drawdown_from_peak(self) -> float:
        if self.risk_state.starting_equity <= 0:
            return 0.0
        return 1.0 - (self.risk_state.bankroll / self.risk_state.starting_equity)

    @property
    def exposure_pct(self) -> float:
        if self.risk_state.bankroll <= 0:
            return 0.0
        return self.risk_state.total_exposure / self.risk_state.bankroll
