import sqlite3
from typing import Optional

from config import get_config

DB_FILE = get_config("DB_FILE", "super_db.db")


def init_db(conn: Optional[sqlite3.Connection] = None) -> sqlite3.Connection:
    """Initialise the SQLite database and required tables."""
    if conn is None:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            username TEXT,
            created_at TEXT,
            fetch_time TEXT,
            text TEXT,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER,
            media TEXT,
            sentiment_label TEXT,
            sentiment_score FLOAT,
            vibe_score FLOAT,
            vibe_label TEXT,
            analysis TEXT,
            approved BOOLEAN,
            source TEXT DEFAULT 'twitter'
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS high_res_prices (
            ticker TEXT,
            date TEXT,
            close REAL,
            volume REAL,
            volatility REAL,
            momentum REAL,
            PRIMARY KEY (ticker, date)
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gas_prices (
            timestamp TEXT PRIMARY KEY,
            fast_gas REAL,
            average_gas REAL,
            slow_gas REAL,
            base_fee REAL,
            source TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS dune_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            query_id TEXT,
            data TEXT,
            ingested_at TEXT
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS stock_options (
            ticker TEXT,
            option_type TEXT,
            expiration TEXT,
            strike REAL,
            last_price REAL,
            implied_vol REAL,
            open_interest INTEGER,
            volume INTEGER,
            fetch_time TEXT,
            PRIMARY KEY (ticker, option_type, expiration, strike)
        )
        """
    )
    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    return conn
