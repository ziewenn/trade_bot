from typing import Literal, Optional
from pydantic import SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="config/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Mode: "paper" (instant fills), "sim-live" (realistic delays), "live" (real orders)
    trading_mode: Literal["paper", "live", "sim-live"] = "paper"
    log_level: str = "INFO"

    # Polymarket credentials (live mode only)
    polymarket_private_key: Optional[SecretStr] = None
    polymarket_funder_address: Optional[str] = None

    # Telegram alerts (optional)
    telegram_bot_token: Optional[SecretStr] = None
    telegram_chat_id: Optional[str] = None

    # Risk parameters
    initial_bankroll: float = 50.0
    max_position_pct: float = 0.02
    daily_loss_limit_pct: float = 0.03
    drawdown_halt_pct: float = 0.08
    kelly_fraction: float = 0.25
    min_edge_pct: float = 0.05
    max_concurrent_exposure_pct: float = 0.15

    # Strategy parameters
    binance_price_lookback_ms: int = 5000
    momentum_threshold_pct: float = 0.0003
    order_offset_cents: float = 0.01
    cancel_replace_interval_ms: int = 500  # 500→1500: reduced API load for small bankroll
    market_entry_window_sec: int = 300  # Allow entry for entire 5-min cycle (exit_buffer still applies)
    market_entry_min_elapsed_sec: int = 0  # No minimum elapsed time restriction
    market_exit_buffer_sec: int = 30
    min_price_gap_usd: float = 20.0  # Min price gap (max of binance/chainlink) to trade
    binance_prediction_weight: float = 0.4  # Blend weight: 0=Chainlink only, 1=Binance only

    # Stale data thresholds
    stale_binance_threshold_s: float = 5.0
    stale_polymarket_threshold_s: float = 10.0
    stale_chainlink_threshold_s: float = 10.0  # Cancel orders if Chainlink stale > 10s
    chainlink_emergency_stale_s: float = 30.0  # Halt trading if ALL Chainlink sources stale > 30s

    # Volatility — lower = more sensitive to price moves = more trades
    volatility_per_second: float = 0.0005

    # Anchor scraping — scrape Polymarket event page for authoritative priceToBeat
    anchor_scrape_enabled: bool = True
    anchor_scrape_timeout_s: float = 8.0
    anchor_scrape_playwright_fallback: bool = False
    anchor_scrape_phase1_interval_s: float = 1.5   # fast retries between attempts
    anchor_scrape_phase2_interval_s: float = 15.0   # slower retries after phase 1
    anchor_scrape_phase1_attempts: int = 10          # ~15s of fast retries
    anchor_scrape_phase2_attempts: int = 10          # ~150s of slow retries

    # WebSocket URLs (overridable)
    binance_ws_url: str = "wss://stream.binance.com:9443/ws/btcusdt@aggTrade"
    polymarket_ws_url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    chainlink_ws_url: str = "wss://ws-live-data.polymarket.com"
    polymarket_clob_url: str = "https://clob.polymarket.com"
    gamma_api_url: str = "https://gamma-api.polymarket.com"

    # Database
    db_path: str = "data/trades.db"

    # Headless mode (skip interactive prompts, disable terminal dashboard)
    headless: bool = False

    # Simulated-live mode settings (only used when trading_mode="sim-live")
    sim_order_latency_min_ms: int = 100
    sim_order_latency_max_ms: int = 300
    sim_cancel_latency_min_ms: int = 30
    sim_cancel_latency_max_ms: int = 80
    sim_fill_latency_min_ms: int = 50
    sim_fill_latency_max_ms: int = 150
    sim_rejection_rate: float = 0.05    # 5% of orders randomly rejected
    sim_fill_rate: float = 0.70         # Only 70% of qualifying orders fill
    sim_slippage_cents: float = 0.015   # 1.5¢ slippage (vs 0.5¢ paper)

    # Web dashboard
    dashboard_enabled: bool = True
    dashboard_port: int = 8080
    dashboard_host: str = "127.0.0.1"
    dashboard_password: str = "changeme"

    @model_validator(mode="after")
    def validate_live_mode(self):
        if self.trading_mode == "live":
            if not self.polymarket_private_key:
                raise ValueError(
                    "POLYMARKET_PRIVATE_KEY is required for live trading mode"
                )
            if not self.polymarket_funder_address:
                raise ValueError(
                    "POLYMARKET_FUNDER_ADDRESS is required for live trading mode"
                )
            if self.dashboard_password == "changeme":
                raise ValueError(
                    "DASHBOARD_PASSWORD must be changed from default 'changeme' in live mode for the web UI"
                )
        return self
