import asyncio
import time
from typing import Optional, Union

import aiohttp
from pydantic import SecretStr

from monitoring.logger import get_logger

logger = get_logger("alerts")


class TelegramAlerts:
    """Telegram Bot API alerts for key events. No-op if not configured."""

    RATE_LIMIT_SECONDS = 10  # Max 1 message per 10 seconds

    def __init__(self, bot_token: Optional[Union[SecretStr, str]] = None, chat_id: Optional[str] = None):
        self._bot_token = bot_token
        self.chat_id = chat_id
        self.enabled = bool(bot_token and chat_id)
        self._last_send_time: float = 0.0
        self._session: Optional[aiohttp.ClientSession] = None
        self._message_queue: asyncio.Queue[str] = asyncio.Queue()

    def _get_token(self) -> str:
        if isinstance(self._bot_token, SecretStr):
            return self._bot_token.get_secret_value()
        return self._bot_token or ""

    async def start(self):
        if not self.enabled:
            logger.info("telegram_alerts_disabled")
            return

        self._session = aiohttp.ClientSession()
        logger.info("telegram_alerts_enabled")

        # Process message queue
        while True:
            try:
                message = await self._message_queue.get()
                await self._send(message)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("telegram_queue_error", error=str(e))

    async def send_alert(self, message: str):
        """Queue an alert message for sending."""
        if not self.enabled:
            return
        await self._message_queue.put(message)

    async def _send(self, message: str):
        """Send a message via Telegram Bot API with rate limiting."""
        # Rate limit
        now = time.time()
        elapsed = now - self._last_send_time
        if elapsed < self.RATE_LIMIT_SECONDS:
            await asyncio.sleep(self.RATE_LIMIT_SECONDS - elapsed)

        try:
            url = f"https://api.telegram.org/bot{self._get_token()}/sendMessage"
            payload = {
                "chat_id": self.chat_id,
                "text": message,
                "parse_mode": "HTML",
            }

            async with self._session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.warning("telegram_send_failed", status=resp.status, body=body[:200])
                else:
                    self._last_send_time = time.time()

        except Exception as e:
            logger.error("telegram_error", error=str(e))

    # Convenience methods for common alerts

    async def bot_started(self, mode: str):
        await self.send_alert(f"🤖 <b>Bot Started</b>\nMode: {mode}")

    async def bot_stopped(self, session_pnl: float):
        pnl_emoji = "📈" if session_pnl >= 0 else "📉"
        await self.send_alert(
            f"🛑 <b>Bot Stopped</b>\n{pnl_emoji} Session P&L: ${session_pnl:+,.2f}"
        )

    async def risk_halt(self, reason: str):
        await self.send_alert(f"🚨 <b>RISK HALT</b>\nReason: {reason}")

    async def connection_lost(self, feed: str):
        await self.send_alert(f"⚠️ <b>Connection Lost</b>\nFeed: {feed}")

    async def connection_restored(self, feed: str):
        await self.send_alert(f"✅ <b>Connection Restored</b>\nFeed: {feed}")

    async def large_trade(self, side: str, size: float, price: float, pnl: float):
        pnl_emoji = "📈" if pnl >= 0 else "📉"
        await self.send_alert(
            f"💰 <b>Large Trade</b>\n"
            f"Side: {side} | Size: {size:.1f} | Price: ${price:.2f}\n"
            f"{pnl_emoji} P&L: ${pnl:+,.2f}"
        )

    async def daily_summary(
        self, date: str, trades: int, win_rate: float, pnl: float, peak: float
    ):
        pnl_emoji = "📈" if pnl >= 0 else "📉"
        await self.send_alert(
            f"📊 <b>Daily Summary - {date}</b>\n"
            f"Trades: {trades} | Win Rate: {win_rate:.1f}%\n"
            f"{pnl_emoji} P&L: ${pnl:+,.2f} | Peak: ${peak:,.2f}"
        )

    async def close(self):
        if self._session:
            await self._session.close()
