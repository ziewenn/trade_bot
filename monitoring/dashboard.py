import asyncio
import time
from datetime import datetime, timezone
from decimal import Decimal

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from config.settings import Settings
from core.state import SharedState
from core.risk_manager import RiskManager
from core.order_manager import OrderManager
from monitoring.logger import get_logger

logger = get_logger("dashboard")


class Dashboard:
    """Terminal-based real-time dashboard using Rich."""

    REFRESH_RATE = 0.5  # 500ms

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
        self.console = Console()

    async def run(self):
        """Run the dashboard in a loop."""
        logger.info("dashboard_started")

        with Live(
            self._build_layout(),
            console=self.console,
            refresh_per_second=2,
            screen=True,
        ) as live:
            while True:
                try:
                    live.update(self._build_layout())
                except Exception as e:
                    logger.debug("dashboard_render_error", error=str(e))
                await asyncio.sleep(self.REFRESH_RATE)

    def _build_layout(self) -> Layout:
        layout = Layout()

        layout.split_column(
            Layout(name="header", size=3),
            Layout(name="body"),
            Layout(name="footer", size=3),
        )

        layout["body"].split_row(
            Layout(name="left", ratio=1),
            Layout(name="right", ratio=1),
        )

        layout["left"].split_column(
            Layout(name="market", size=8),
            Layout(name="strategy", size=7),
            Layout(name="orders", size=10),
        )

        layout["right"].split_column(
            Layout(name="positions", size=8),
            Layout(name="risk", size=8),
            Layout(name="recent_trades", size=9),
            Layout(name="stats", size=9),
        )

        layout["header"].update(self._header_panel())
        layout["market"].update(self._market_panel())
        layout["strategy"].update(self._strategy_panel())
        layout["orders"].update(self._orders_panel())
        layout["positions"].update(self._positions_panel())
        layout["risk"].update(self._risk_panel())
        layout["recent_trades"].update(self._recent_trades_panel())
        layout["stats"].update(self._stats_panel())
        layout["footer"].update(self._footer_panel())

        return layout

    def _header_panel(self) -> Panel:
        mode = self.settings.trading_mode.upper()
        mode_color = "red bold" if mode == "LIVE" else "yellow bold" if mode == "SIM-LIVE" else "green bold"

        uptime = time.time() - self.state.session_start
        hours, remainder = divmod(int(uptime), 3600)
        minutes, seconds = divmod(remainder, 60)

        binance_status = "[green]OK[/]" if self.state.binance_connected else "[red]DOWN[/]"
        poly_status = "[green]OK[/]" if self.state.polymarket_connected else "[red]DOWN[/]"
        chain_status = "[green]OK[/]" if self.state.chainlink_connected else "[red]DOWN[/]"

        paused = getattr(self.state, 'trading_paused', False)
        trading_status = "[red bold]PAUSED[/]" if paused else "[green bold]ACTIVE[/]"

        text = (
            f"  [{mode_color}]{mode} MODE[/]  |  "
            f"Trading: {trading_status}  |  "
            f"Uptime: {hours:02d}:{minutes:02d}:{seconds:02d}  |  "
            f"Binance: {binance_status}  Polymarket: {poly_status}  Chainlink: {chain_status}"
        )
        return Panel(Text.from_markup(text), title="Polymarket BTC 5m Arb Bot", style="bold blue")

    def _market_panel(self) -> Panel:
        market = self.state.current_market

        if not market:
            return Panel("[dim]No active market[/]", title="Current Market")

        remaining = self.state.get_remaining_seconds()
        mins, secs = divmod(int(remaining), 60)

        # Chainlink delta vs anchor (this determines resolution)
        import time as _time
        cl_age = _time.monotonic() - self.state.chainlink_tick_time if self.state.chainlink_tick_time > 0 else float("inf")
        cl_source = self.state.chainlink_source or "?"

        if cl_age > 30:
            # Emergency: all sources stale
            chainlink_str = f"[bold red]STALE (>{int(cl_age)}s) — ORDERS CANCELLED[/]"
        elif cl_age > 10:
            # Stale: orders should be cancelled
            chainlink_str = f"${float(self.state.chainlink_price):,.2f}  [bold red]STALE ({cl_age:.1f}s ago)[/]"
        elif self.state.chainlink_price > 0:
            chainlink_str = f"${float(self.state.chainlink_price):,.2f}"
            # Source and age
            source_color = "green" if cl_source == "RTDS" else "yellow"
            chainlink_str += f"  [{source_color}]({cl_source}, {cl_age:.1f}s ago)[/]"
            # Delta vs anchor
            if market.anchor_price:
                cl_delta = float(self.state.chainlink_price - market.anchor_price)
                if cl_delta > 0:
                    chainlink_str += f"  [green]+${cl_delta:,.2f} (UP)[/]"
                elif cl_delta < 0:
                    chainlink_str += f"  [red]-${abs(cl_delta):,.2f} (DOWN)[/]"
        else:
            chainlink_str = "[dim]--[/]"

        # Binance delta with color + age
        binance_age = _time.monotonic() - self.state.binance_tick_time if self.state.binance_tick_time > 0 else float("inf")
        binance_str = f"${float(self.state.binance_price):,.2f}"
        if self.state.binance_tick_time > 0:
            binance_str += f"  [dim]({binance_age:.1f}s ago)[/]"
        if market.anchor_price and self.state.binance_price > 0:
            delta = float(self.state.binance_price - market.anchor_price)
            if delta > 0:
                binance_str += f"  [green]+${delta:,.2f}[/]"
            elif delta < 0:
                binance_str += f"  [red]-${abs(delta):,.2f}[/]"

        # Anchor source and confidence
        anchor_source = self.state.anchor_source or "none"
        is_auth = self.state.anchor_is_authoritative
        if market.anchor_price:
            anchor_str = f"${float(market.anchor_price):,.2f}"
            if is_auth:
                anchor_str += f"  [green]({anchor_source})[/]"
            else:
                anchor_str += f"  [yellow]({anchor_source} — trading blocked)[/]"
        else:
            anchor_str = "[yellow]Pending...[/]"

        rows = [
            f"  Slug:     {market.event_slug}",
            f"  Anchor:   {anchor_str}",
            f"  Chainlink:{chainlink_str}",
            f"  Binance:  {binance_str}",
            f"  Time:     {mins}:{secs:02d} remaining",
            f"  Phase:    {self.state.market_phase.value}",
        ]
        return Panel("\n".join(r for r in rows if r), title="Current Market")

    def _strategy_panel(self) -> Panel:
        prob = self.state.current_true_prob
        edge = self.state.current_edge
        direction = self.state.current_signal_direction or "-"
        market = self.state.current_market

        prob_bar = self._probability_bar(prob)

        # Price delta explanation (uses Chainlink — same as resolution)
        delta_text = ""
        if market and market.anchor_price and self.state.chainlink_price > 0:
            delta = float(self.state.chainlink_price - market.anchor_price)
            if delta > 0:
                delta_text = f"  [green]BTC ${delta:,.0f} above anchor[/]"
            elif delta < 0:
                delta_text = f"  [red]BTC ${abs(delta):,.0f} below anchor[/]"
            else:
                delta_text = "  [yellow]BTC at anchor[/]"

        # Direction with color
        dir_color = "green bold" if direction == "UP" else "red bold" if direction == "DOWN" else "dim"

        # Potential outcomes from current positions
        total_cost = sum(
            float(p.avg_entry_price * p.size)
            for p in self.state.open_positions.values()
        )
        total_shares = sum(
            float(p.size) for p in self.state.open_positions.values()
        )
        outcome_text = ""
        if total_shares > 0:
            win_profit = total_shares * 1.0 - total_cost
            outcome_text = (
                f"\n  [green]If correct: +${win_profit:,.2f}[/]"
                f"  [red]If wrong: -${total_cost:,.2f}[/]"
            )

        rows = [
            f"  Direction: [{dir_color}]{direction}[/]{delta_text}",
            f"  P(Up):     {prob:.2%}  {prob_bar}",
            f"  Edge:      {edge:+.2%}  (min: {self.settings.min_edge_pct:.2%})",
        ]
        if outcome_text:
            rows.append(outcome_text)

        return Panel("\n".join(rows), title="Strategy")

    def _orders_panel(self) -> Panel:
        table = Table(show_header=True, header_style="bold", expand=True, padding=(0, 1))
        table.add_column("Side", width=5)
        table.add_column("Price", width=8)
        table.add_column("Size", width=8)
        table.add_column("Age(s)", width=7)

        for order in self.state.open_orders.values():
            age = int(time.time() - order.created_at)
            side_color = "green" if order.side.value == "BUY" else "red"
            table.add_row(
                f"[{side_color}]{order.side.value}[/]",
                f"${float(order.price):.2f}",
                f"{float(order.size):.1f}",
                str(age),
            )

        if not self.state.open_orders:
            table.add_row("[dim]-[/]", "[dim]-[/]", "[dim]-[/]", "[dim]-[/]")

        return Panel(table, title=f"Open Orders ({len(self.state.open_orders)})")

    def _positions_panel(self) -> Panel:
        table = Table(show_header=True, header_style="bold", expand=True, padding=(0, 1))
        table.add_column("Side", width=5)
        table.add_column("Qty", width=6)
        table.add_column("Cost", width=9)
        table.add_column("Value", width=9)
        table.add_column("uPnL", width=10)
        table.add_column("If Win", width=10)

        market = self.state.current_market

        for pos in self.state.open_positions.values():
            # Determine side and current market price
            side, current_bid = self._get_position_side_and_bid(pos, market)
            side_color = "green" if side == "UP" else "red" if side == "DOWN" else "dim"

            cost = float(pos.avg_entry_price * pos.size)
            value = float(current_bid * pos.size) if current_bid else cost
            upnl = value - cost
            profit_if_win = float(pos.size) * 1.0 - cost  # Tokens pay $1 each

            upnl_color = "green" if upnl >= 0 else "red"
            win_color = "green" if profit_if_win >= 0 else "red"

            table.add_row(
                f"[{side_color}]{side}[/]",
                f"{float(pos.size):.0f}",
                f"${cost:,.2f}",
                f"${value:,.2f}",
                f"[{upnl_color}]${upnl:+,.2f}[/]",
                f"[{win_color}]${profit_if_win:+,.2f}[/]",
            )

        if not self.state.open_positions:
            table.add_row("[dim]-[/]", "[dim]-[/]", "[dim]-[/]", "[dim]-[/]", "[dim]-[/]", "[dim]-[/]")

        return Panel(table, title=f"Positions ({len(self.state.open_positions)})")

    def _get_position_side_and_bid(self, pos, market):
        """Determine if position is UP/DOWN and get current bid price."""
        if not market:
            return "?", None
        if pos.token_id == market.token_id_up:
            return "UP", self.state.best_bid_up
        elif pos.token_id == market.token_id_down:
            return "DOWN", self.state.best_bid_down
        return "?", None

    def _risk_panel(self) -> Panel:
        rs = self.risk.risk_state
        halt_text = f"[red bold]HALTED: {rs.halt_reason}[/]" if rs.is_halted else "[green]Active[/]"

        # Cash = paper_bankroll, Equity = cash + position cost (at entry)
        cash = float(self.state.paper_bankroll) if self.state.paper_bankroll > 0 else rs.bankroll
        position_cost = sum(
            float(p.avg_entry_price * p.size)
            for p in self.state.open_positions.values()
        )
        equity = cash + position_cost
        starting = self.state.starting_bankroll
        actual_pnl = equity - starting
        actual_pnl_color = "green" if actual_pnl >= 0 else "red"
        realized_pnl = float(self.state.session_pnl)
        realized_pnl_color = "green" if realized_pnl >= 0 else "red"
        drawdown = self.risk.drawdown_from_peak
        drawdown_color = "red" if drawdown > self.settings.drawdown_halt_pct * 0.5 else "yellow"

        rows = [
            f"  Status:     {halt_text}",
            f"  Equity:     ${equity:,.2f}  [dim](cash: ${cash:,.2f})[/]",
            f"  Started:    ${starting:,.2f}",
            f"  Total P&L:  [{actual_pnl_color}]${actual_pnl:+,.2f}[/]  ({actual_pnl/starting*100 if starting > 0 else 0:+,.1f}%)",
            f"  Realized:   [{realized_pnl_color}]${realized_pnl:+,.2f}[/]",
            f"  Drawdown:   [{drawdown_color}]{drawdown:.2%}[/]  (halt: {self.settings.drawdown_halt_pct:.0%})",
            f"  Exposure:   {self.risk.exposure_pct:.1%}  (max: {self.settings.max_concurrent_exposure_pct:.0%})",
        ]
        return Panel("\n".join(rows), title="Risk")

    def _recent_trades_panel(self) -> Panel:
        table = Table(show_header=True, header_style="bold", expand=True, padding=(0, 1))
        table.add_column("Side", width=5)
        table.add_column("Result", width=6)
        table.add_column("P&L", width=10)
        table.add_column("Cost", width=8)
        table.add_column("Ago", width=8)

        trades = self.state.recent_trades
        for trade in reversed(trades):
            side_color = "green" if trade["side"] == "UP" else "red" if trade["side"] == "DOWN" else "dim"
            result = "WIN" if trade["won"] else "LOSS"
            result_color = "green" if trade["won"] else "red"
            pnl = trade["pnl"]
            pnl_color = "green" if pnl >= 0 else "red"
            ago = int(time.time() - trade["time"])
            if ago < 60:
                ago_str = f"{ago}s"
            else:
                ago_str = f"{ago // 60}m"

            table.add_row(
                f"[{side_color}]{trade['side']}[/]",
                f"[{result_color}]{result}[/]",
                f"[{pnl_color}]${pnl:+,.2f}[/]",
                f"${trade['cost']:,.2f}",
                ago_str,
            )

        if not trades:
            table.add_row("[dim]-[/]", "[dim]-[/]", "[dim]-[/]", "[dim]-[/]", "[dim]-[/]")

        return Panel(table, title=f"Recent Trades ({len(trades)})")

    def _stats_panel(self) -> Panel:
        total = self.state.total_trades
        wins = self.state.winning_trades
        win_rate = (wins / total * 100) if total > 0 else 0.0
        pnl = float(self.state.session_pnl)
        pnl_color = "green" if pnl >= 0 else "red"

        avg_latency = self.orders.avg_cancel_replace_latency_ms

        rows = [
            f"  Trades:      {total}",
            f"  Win Rate:    {win_rate:.1f}%",
            f"  Session P&L: [{pnl_color}]${pnl:+,.2f}[/]",
            f"  Avg Latency: {avg_latency:.0f}ms (cancel/replace)",
            f"  Peak Equity: ${self.risk.risk_state.peak_equity:,.2f}",
        ]
        return Panel("\n".join(rows), title="Session Stats")

    def _footer_panel(self) -> Panel:
        now = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
        web_url = f"http://{self.settings.dashboard_host}:{self.settings.dashboard_port}"
        return Panel(f"  {now}  |  Web UI: [bold cyan]{web_url}[/]  |  Ctrl+C to stop", style="dim")

    @staticmethod
    def _probability_bar(prob: float, width: int = 20) -> str:
        filled = int(prob * width)
        empty = width - filled
        return f"[{'=' * filled}{' ' * empty}]"
