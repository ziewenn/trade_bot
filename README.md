# ⚡ Polymarket BTC 5m Arb Bot

A high-frequency arbitrage trading bot built specifically for Polymarket's **BTC 5m** (Bitcoin up/down 5-minute) prediction markets. The bot continuously monitors real-time order books, on-chain Chainlink feeds, and Binance websockets to execute micro-trades and capture small percentage edges.

---

## 🌟 Key Features

* **3 Trading Modes**: 
  * `paper`: Completely virtual trading (simulated fills immediately at best bid/ask).
  * `sim-live`: Virtual trading but with realistic network latencies, simulated slippage, and random fill rejections to mimic real market conditions.
  * `live`: Genuine trading executing real orders via the Polymarket CLOB API with your USDC bankroll.

* **Advanced Strategy & Risk Enforcement**:
  * Real-time Kelly Criterion sizing for position management
  * Drawdown halting (customizable "kill switch")
  * Real-time exposure caps and daily loss limits
  * Stale data detection for multiple feeds (Chainlink, Polymarket, Binance)

* **Dual Dashboards**:
  * **Terminal UI (TUI)**: Powered by `Rich`, providing live in-terminal monitoring without a browser.
  * **Web Dashboard**: A beautiful, premium glassmorphic UI accessible locally at `http://127.0.0.1:8080`. Offers the same full feature set plus **Start/Stop Controls** to dynamically pause trading logic execution safely (cancels open orders) without terminating the running loop.

---

## 🚀 Quick Start

### 1. Requirements

* Python 3.12+ 
* Installed dependencies: `pip install -r requirements.txt`

### 2. Configuration (`.env`)

Copy `config/.env.example` to `config/.env` and configure accordingly:

```ini
# Trading Mode ("paper", "live", "sim-live")
TRADING_MODE=paper

# Live Mode Credentials (Required ONLY if trading_mode=live)
POLYMARKET_PRIVATE_KEY=your_private_hex_key
POLYMARKET_FUNDER_ADDRESS=your_wallet_address

# Web Dashboard access
DASHBOARD_PASSWORD=tradebot_admin

# Example Strategy Toggles
INITIAL_BANKROLL=50.0  # Max exposure the bot manages
```

*(Note: For security, your `.env` is ignored by Git. Never commit your private keys!)*

### 3. Run the Bot

Launch the bot by running:
```bash
python main.py
```

If you are running in `live` mode, the bot will ask for `CONFIRM LIVE TRADING` on the console as a safety precaution.

* **Terminal UI:** Starts immediately within your terminal window.
* **Web UI:** Available in your browser at `http://localhost:8080`. Use username `admin` (or any string) and the password you defined in `.env` (default: `tradebot_admin`).

---

## 🖥 Web Dashboard Features

The fast API-powered, SSE-streamed web dashboard includes everything you need to manage your system seamlessly:

* **Trading Controls:** **[▶ Start]** and **[■ Stop]** buttons. You can emergency-halt (cancel all orders) or resume execution without terminating the Python background process.
* **Real-time Equity Tracking:** Follow your Peak Equity, Realized Pnl, and Drawdown percentages live.
* **Live Order / Feed Status:** See immediately if a feed endpoint (Binance, Polymarket, or Chainlink) loses connection.
* **Position & Strategy Data:** Active Win/Loss metrics, expected edge probabilities (Kelly logic), and real-time PnL of current holdings.

---

## 🚨 Disclaimer & Safety

This bot relies on the `py-clob-client` SDK and interacts directly with Decentralized Finance protocols. 
* Do not deposit funds you cannot afford to lose.
* Ensure you start entirely in `paper` or `sim-live` mode to evaluate the strategy offsets and profitability before committing real capital.
* It is strongly advised to utilize a dedicated wallet solely for this bot, only depositing the specific amount of USDC you've allocated for trading.
