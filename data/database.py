import aiosqlite
from decimal import Decimal
from typing import Optional

from monitoring.logger import get_logger

logger = get_logger("database")

SCHEMA = """
CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_id TEXT UNIQUE NOT NULL,
    order_id TEXT NOT NULL,
    market_condition_id TEXT NOT NULL,
    token_id TEXT NOT NULL,
    side TEXT NOT NULL,
    outcome TEXT NOT NULL,
    price TEXT NOT NULL,
    size TEXT NOT NULL,
    fee TEXT DEFAULT '0',
    realized_pnl TEXT DEFAULT '0',
    is_paper BOOLEAN NOT NULL,
    timestamp REAL NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS positions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    token_id TEXT UNIQUE NOT NULL,
    market_condition_id TEXT NOT NULL,
    outcome TEXT NOT NULL,
    size TEXT NOT NULL,
    avg_entry_price TEXT NOT NULL,
    is_paper BOOLEAN NOT NULL,
    opened_at REAL NOT NULL,
    closed_at REAL,
    close_price TEXT,
    pnl TEXT
);

CREATE TABLE IF NOT EXISTS daily_stats (
    date TEXT PRIMARY KEY,
    total_trades INTEGER DEFAULT 0,
    winning_trades INTEGER DEFAULT 0,
    total_pnl TEXT DEFAULT '0',
    max_drawdown TEXT DEFAULT '0',
    peak_equity TEXT,
    closing_equity TEXT,
    is_paper BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS market_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    condition_id TEXT NOT NULL,
    anchor_price TEXT NOT NULL,
    resolution_price TEXT,
    winning_outcome TEXT,
    start_time REAL NOT NULL,
    end_time REAL NOT NULL,
    total_volume TEXT DEFAULT '0',
    captured_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS price_ticks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    price TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_price_ticks_ts ON price_ticks(timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_trades_market ON trades(market_condition_id);
CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp);
"""


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._db: Optional[aiosqlite.Connection] = None

    async def connect(self):
        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.executescript(SCHEMA)
        await self._db.commit()
        logger.info("database_connected", path=self.db_path)

    async def close(self):
        if self._db:
            await self._db.close()
            logger.info("database_closed")

    async def insert_trade(
        self,
        trade_id: str,
        order_id: str,
        market_condition_id: str,
        token_id: str,
        side: str,
        outcome: str,
        price: Decimal,
        size: Decimal,
        fee: Decimal,
        realized_pnl: Decimal,
        is_paper: bool,
        timestamp: float,
    ):
        await self._db.execute(
            """INSERT INTO trades
               (trade_id, order_id, market_condition_id, token_id, side, outcome,
                price, size, fee, realized_pnl, is_paper, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                trade_id, order_id, market_condition_id, token_id, side, outcome,
                str(price), str(size), str(fee), str(realized_pnl),
                is_paper, timestamp,
            ),
        )
        await self._db.commit()

    async def insert_position(
        self,
        token_id: str,
        market_condition_id: str,
        outcome: str,
        size: Decimal,
        avg_entry_price: Decimal,
        is_paper: bool,
        opened_at: float,
    ):
        await self._db.execute(
            """INSERT OR REPLACE INTO positions
               (token_id, market_condition_id, outcome, size, avg_entry_price,
                is_paper, opened_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                token_id, market_condition_id, outcome,
                str(size), str(avg_entry_price), is_paper, opened_at,
            ),
        )

    async def update_position_size(
        self, token_id: str, size: Decimal, avg_entry_price: Decimal
    ):
        await self._db.execute(
            """UPDATE positions SET size=?, avg_entry_price=?
               WHERE token_id=? AND closed_at IS NULL""",
            (str(size), str(avg_entry_price), token_id),
        )

    async def close_position(
        self, token_id: str, close_price: Decimal, pnl: Decimal, closed_at: float
    ):
        await self._db.execute(
            """UPDATE positions SET closed_at=?, close_price=?, pnl=?
               WHERE token_id=? AND closed_at IS NULL""",
            (closed_at, str(close_price), str(pnl), token_id),
        )
        await self._db.commit()

    async def close_position_deferred(
        self, token_id: str, close_price: Decimal, pnl: Decimal, closed_at: float
    ):
        """Close position without committing — caller must flush/commit later."""
        await self._db.execute(
            """UPDATE positions SET closed_at=?, close_price=?, pnl=?
               WHERE token_id=? AND closed_at IS NULL""",
            (closed_at, str(close_price), str(pnl), token_id),
        )

    async def get_open_positions(self, is_paper: bool) -> list[dict]:
        cursor = await self._db.execute(
            """SELECT token_id, market_condition_id, outcome, size,
                      avg_entry_price, opened_at
               FROM positions WHERE closed_at IS NULL AND is_paper=?""",
            (is_paper,),
        )
        rows = await cursor.fetchall()
        return [
            {
                "token_id": r[0],
                "market_condition_id": r[1],
                "outcome": r[2],
                "size": Decimal(r[3]),
                "avg_entry_price": Decimal(r[4]),
                "opened_at": r[5],
            }
            for r in rows
        ]

    async def insert_market_snapshot(
        self,
        condition_id: str,
        anchor_price: Decimal,
        start_time: float,
        end_time: float,
    ):
        await self._db.execute(
            """INSERT INTO market_snapshots
               (condition_id, anchor_price, start_time, end_time)
               VALUES (?, ?, ?, ?)""",
            (condition_id, str(anchor_price), start_time, end_time),
        )
        await self._db.commit()

    async def insert_market_snapshot_deferred(
        self,
        condition_id: str,
        anchor_price: Decimal,
        start_time: float,
        end_time: float,
    ):
        """Insert market snapshot without committing — deferred to periodic flush."""
        await self._db.execute(
            """INSERT INTO market_snapshots
               (condition_id, anchor_price, start_time, end_time)
               VALUES (?, ?, ?, ?)""",
            (condition_id, str(anchor_price), start_time, end_time),
        )

    async def update_market_resolution(
        self,
        condition_id: str,
        resolution_price: Decimal,
        winning_outcome: str,
    ):
        await self._db.execute(
            """UPDATE market_snapshots
               SET resolution_price=?, winning_outcome=?
               WHERE condition_id=?""",
            (str(resolution_price), winning_outcome, condition_id),
        )
        await self._db.commit()

    async def get_daily_pnl(self, date_str: str) -> Decimal:
        cursor = await self._db.execute(
            "SELECT COALESCE(SUM(CAST(realized_pnl AS REAL)), 0) FROM trades WHERE date(timestamp, 'unixepoch')=?",
            (date_str,),
        )
        row = await cursor.fetchone()
        return Decimal(str(row[0])) if row else Decimal("0")

    async def get_daily_stats(self, date_str: str) -> Optional[dict]:
        cursor = await self._db.execute(
            "SELECT * FROM daily_stats WHERE date=?", (date_str,)
        )
        row = await cursor.fetchone()
        if not row:
            return None
        return {
            "date": row[0],
            "total_trades": row[1],
            "winning_trades": row[2],
            "total_pnl": Decimal(row[3]),
            "max_drawdown": Decimal(row[4]),
            "peak_equity": Decimal(row[5]) if row[5] else None,
            "closing_equity": Decimal(row[6]) if row[6] else None,
            "is_paper": bool(row[7]),
        }

    async def upsert_daily_stats(
        self,
        date_str: str,
        total_trades: int,
        winning_trades: int,
        total_pnl: Decimal,
        max_drawdown: Decimal,
        peak_equity: Decimal,
        closing_equity: Decimal,
        is_paper: bool,
    ):
        await self._db.execute(
            """INSERT INTO daily_stats
               (date, total_trades, winning_trades, total_pnl, max_drawdown,
                peak_equity, closing_equity, is_paper)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(date) DO UPDATE SET
                total_trades=excluded.total_trades,
                winning_trades=excluded.winning_trades,
                total_pnl=excluded.total_pnl,
                max_drawdown=excluded.max_drawdown,
                peak_equity=excluded.peak_equity,
                closing_equity=excluded.closing_equity""",
            (
                date_str, total_trades, winning_trades,
                str(total_pnl), str(max_drawdown),
                str(peak_equity), str(closing_equity), is_paper,
            ),
        )
        await self._db.commit()

    async def insert_price_tick(self, source: str, price: Decimal, timestamp_ms: int):
        await self._db.execute(
            "INSERT INTO price_ticks (source, price, timestamp_ms) VALUES (?, ?, ?)",
            (source, str(price), timestamp_ms),
        )
        # Don't commit every tick — batch via periodic flush
        pass

    async def flush(self):
        if self._db:
            await self._db.commit()

    async def purge_old_ticks(self, cutoff_ms: int):
        await self._db.execute(
            "DELETE FROM price_ticks WHERE timestamp_ms < ?", (cutoff_ms,)
        )
        await self._db.commit()

    async def get_session_trades(self, is_paper: bool, limit: int = 10) -> list[dict]:
        cursor = await self._db.execute(
            """SELECT trade_id, token_id, side, outcome, price, size, fee,
                      realized_pnl, timestamp
               FROM trades WHERE is_paper=?
               ORDER BY timestamp DESC LIMIT ?""",
            (is_paper, limit),
        )
        rows = await cursor.fetchall()
        return [
            {
                "trade_id": r[0],
                "token_id": r[1],
                "side": r[2],
                "outcome": r[3],
                "price": Decimal(r[4]),
                "size": Decimal(r[5]),
                "fee": Decimal(r[6]),
                "realized_pnl": Decimal(r[7]),
                "timestamp": r[8],
            }
            for r in rows
        ]
