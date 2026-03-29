"""Lightweight FastAPI web dashboard with SSE for real-time updates + trading controls."""

import asyncio
import json
import secrets
import time
from decimal import Decimal
from pathlib import Path

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from config.settings import Settings
from core.order_manager import OrderManager
from core.risk_manager import RiskManager
from core.state import SharedState
from monitoring.logger import get_logger

logger = get_logger("web_dashboard")

security = HTTPBasic()


def _decimal_default(obj):
    """JSON serializer for Decimal."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")


class WebDashboard:
    """FastAPI-based web dashboard running inside the bot's event loop."""

    def __init__(
        self,
        settings: Settings,
        state: SharedState,
        risk_manager: RiskManager,
        order_manager: OrderManager,
    ):
        self.settings = settings
        self.state = state
        self.risk = risk_manager
        self.orders = order_manager
        self._trading_paused = False  # Pause flag for start/stop controls

        self.app = FastAPI(title="Polymarket Bot Dashboard", docs_url=None, redoc_url=None)
        self._setup_routes()

    def _verify_auth(self, credentials: HTTPBasicCredentials = Depends(security)):
        """HTTP Basic Auth check."""
        correct = secrets.compare_digest(
            credentials.password, self.settings.dashboard_password
        )
        if not correct:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials",
                headers={"WWW-Authenticate": "Basic"},
            )
        return credentials

    def _setup_routes(self):
        app = self.app

        # Health check — unauthenticated (for Docker healthcheck)
        @app.get("/health")
        async def health():
            uptime = time.time() - self.state.session_start
            return {"status": "ok", "uptime": round(uptime, 1)}

        # Main dashboard page
        @app.get("/", response_class=HTMLResponse)
        async def index(credentials: HTTPBasicCredentials = Depends(self._verify_auth)):
            template_path = Path(__file__).parent / "templates" / "dashboard.html"
            html = template_path.read_text(encoding="utf-8")
            return HTMLResponse(html)

        # JSON state snapshot (polling fallback)
        @app.get("/api/state")
        async def get_state(credentials: HTTPBasicCredentials = Depends(self._verify_auth)):
            return self._serialize_state()

        # SSE stream — pushes state every 500ms
        @app.get("/api/stream")
        async def stream(request: Request, credentials: HTTPBasicCredentials = Depends(self._verify_auth)):
            async def event_generator():
                while True:
                    if await request.is_disconnected():
                        break
                    data = json.dumps(self._serialize_state(), default=_decimal_default)
                    yield f"data: {data}\n\n"
                    await asyncio.sleep(0.5)

            return StreamingResponse(
                event_generator(),
                media_type="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                },
            )

        # --- Trading Controls ---
        @app.post("/api/start")
        async def start_trading(credentials: HTTPBasicCredentials = Depends(self._verify_auth)):
            """Resume trading (unpause the order manager)."""
            if not self._trading_paused:
                return JSONResponse({"status": "already_running", "message": "Trading is already active"})
            self._trading_paused = False
            self.state.trading_paused = False
            if self.risk.is_halted and self.risk.risk_state.halt_reason == "user_paused":
                self.risk.risk_state.is_halted = False
                self.risk.risk_state.halt_reason = None
            logger.info("trading_resumed_via_web")
            return JSONResponse({"status": "started", "message": "Trading resumed"})

        @app.post("/api/stop")
        async def stop_trading(credentials: HTTPBasicCredentials = Depends(self._verify_auth)):
            """Pause trading — cancel all orders and halt new ones."""
            if self._trading_paused:
                return JSONResponse({"status": "already_stopped", "message": "Trading is already paused"})
            self._trading_paused = True
            self.state.trading_paused = True
            # Cancel all open orders immediately
            try:
                await self.orders.cancel_all()
            except Exception as e:
                logger.error("stop_cancel_failed", error=str(e))
            # Set risk halt so no new trades are placed
            self.risk.risk_state.is_halted = True
            self.risk.risk_state.halt_reason = "user_paused"
            logger.info("trading_paused_via_web")
            return JSONResponse({"status": "stopped", "message": "Trading paused, orders cancelled"})

    def _serialize_state(self) -> dict:
        """Convert bot state to JSON-serializable dict."""
        s = self.state
        rs = self.risk.risk_state
        market = s.current_market

        # Uptime
        uptime = time.time() - s.session_start
        hours, remainder = divmod(int(uptime), 3600)
        minutes, secs = divmod(remainder, 60)

        # Market info
        remaining = s.get_remaining_seconds()
        r_mins, r_secs = divmod(int(remaining), 60)

        # Equity = cash + position cost
        cash = float(s.paper_bankroll) if s.paper_bankroll > 0 else rs.bankroll
        position_cost = sum(
            float(p.avg_entry_price * p.size)
            for p in s.open_positions.values()
        )
        bankroll = cash + position_cost
        starting = s.starting_bankroll
        actual_pnl = bankroll - starting if starting > 0 else 0

        # Stats
        total = s.total_trades
        wins = s.winning_trades
        win_rate = (wins / total * 100) if total > 0 else 0.0
        pnl = float(s.session_pnl)

        # Orders
        orders_list = []
        for order in s.open_orders.values():
            age = int(time.time() - order.created_at)
            orders_list.append({
                "side": order.side.value,
                "price": float(order.price),
                "size": float(order.size),
                "age": age,
            })

        # Positions — calculate live uPnL and potential outcomes
        positions_list = []
        for pos in s.open_positions.values():
            side = "?"
            current_bid = None
            if market:
                if pos.token_id == market.token_id_up:
                    side = "UP"
                    current_bid = s.best_bid_up
                elif pos.token_id == market.token_id_down:
                    side = "DOWN"
                    current_bid = s.best_bid_down

            cost = float(pos.avg_entry_price * pos.size)
            value = float(current_bid * pos.size) if current_bid else cost
            upnl = value - cost
            profit_if_win = float(pos.size) * 1.0 - cost

            positions_list.append({
                "side": side,
                "shares": float(pos.size),
                "cost": cost,
                "value": value,
                "upnl": upnl,
                "if_win": profit_if_win,
            })

        # Price deltas
        price_delta = None
        chainlink_delta = None
        if market and market.anchor_price:
            if s.binance_price > 0:
                price_delta = float(s.binance_price - market.anchor_price)
            if s.chainlink_price > 0:
                chainlink_delta = float(s.chainlink_price - market.anchor_price)

        # Potential outcomes from all positions
        total_cost = sum(p["cost"] for p in positions_list)
        total_shares = sum(p["shares"] for p in positions_list)
        win_profit = total_shares * 1.0 - total_cost if total_shares > 0 else 0

        # Chainlink source & age
        cl_age = time.monotonic() - s.chainlink_tick_time if s.chainlink_tick_time > 0 else -1
        cl_source = getattr(s, "chainlink_source", "") or ""

        # Recent trades
        recent_trades = []
        for trade in reversed(s.recent_trades):
            ago = int(time.time() - trade["time"])
            recent_trades.append({
                "side": trade["side"],
                "won": trade["won"],
                "pnl": trade["pnl"],
                "cost": trade["cost"],
                "ago": f"{ago}s" if ago < 60 else f"{ago // 60}m",
            })

        return {
            "mode": self.settings.trading_mode.upper(),
            "uptime": f"{hours:02d}:{minutes:02d}:{secs:02d}",
            "trading_paused": self._trading_paused,
            "feeds": {
                "binance": s.binance_connected,
                "polymarket": s.polymarket_connected,
                "chainlink": s.chainlink_connected,
            },
            "market": {
                "slug": market.event_slug if market else None,
                "anchor": float(market.anchor_price) if market and market.anchor_price else None,
                "anchor_source": getattr(s, "anchor_source", "") or "",
                "anchor_authoritative": getattr(s, "anchor_is_authoritative", False),
                "binance_price": float(s.binance_price),
                "chainlink_price": float(s.chainlink_price),
                "chainlink_source": cl_source,
                "chainlink_age": round(cl_age, 1) if cl_age >= 0 else -1,
                "chainlink_delta": chainlink_delta,
                "price_delta": price_delta,
                "remaining": f"{r_mins}:{r_secs:02d}",
                "remaining_seconds": remaining,
                "phase": s.market_phase.value,
            },
            "strategy": {
                "prob_up": s.current_true_prob,
                "edge": s.current_edge,
                "direction": s.current_signal_direction or "-",
                "min_edge": self.settings.min_edge_pct,
                "if_win": win_profit,
                "if_lose": -total_cost if total_cost > 0 else 0,
            },
            "orders": orders_list,
            "positions": positions_list,
            "risk": {
                "halted": rs.is_halted,
                "halt_reason": rs.halt_reason,
                "bankroll": bankroll,
                "starting": starting,
                "actual_pnl": actual_pnl,
                "actual_pnl_pct": (actual_pnl / starting * 100) if starting > 0 else 0,
                "realized_pnl": pnl,
                "daily_pnl": pnl,
                "daily_limit": bankroll * self.settings.daily_loss_limit_pct,
                "drawdown": self.risk.drawdown_from_peak,
                "drawdown_halt": self.settings.drawdown_halt_pct,
                "exposure": self.risk.exposure_pct,
                "max_exposure": self.settings.max_concurrent_exposure_pct,
                "cash": cash,
            },
            "stats": {
                "trades": total,
                "wins": wins,
                "win_rate": win_rate,
                "session_pnl": pnl,
                "avg_latency_ms": self.orders.avg_cancel_replace_latency_ms,
                "peak_equity": rs.peak_equity,
            },
            "recent_trades": recent_trades,
        }

    async def run(self):
        """Start uvicorn server inside the existing event loop."""
        logger.info(
            "web_dashboard_started",
            host=self.settings.dashboard_host,
            port=self.settings.dashboard_port,
        )
        config = uvicorn.Config(
            self.app,
            host=self.settings.dashboard_host,
            port=self.settings.dashboard_port,
            log_level="warning",
            access_log=False,
        )
        server = uvicorn.Server(config)
        await server.serve()
