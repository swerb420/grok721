"""Asynchronous version of the simplified data pipeline."""

import asyncio
import datetime
import json
import logging
from dataclasses import asdict
from typing import List

import aiohttp
from aiohttp import ClientSession

from config import get_config
from tenacity import retry, stop_after_attempt, wait_exponential

import sqlite3

# Configuration
APIFY_TOKEN = get_config("APIFY_TOKEN", "apify_api_xxxxxxxxxx")
ETHERSCAN_KEY = get_config("ETHERSCAN_KEY", "xxxxxxxxxx")
DUNE_API_KEY = get_config("DUNE_API_KEY", "xxxxxxxxxx")
DUNE_QUERY_ID = get_config("DUNE_QUERY_ID", "5081617")
DB_FILE = get_config("DB_FILE", "super_db.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


async def init_db(conn: sqlite3.Connection | None = None) -> sqlite3.Connection:
    if conn is None:
        conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
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
    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    return conn


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, max=10))
async def fetch_json(
    session: ClientSession,
    url: str,
    method: str = "get",
    timeout: int = 30,
    **kwargs,
) -> dict:
    async with getattr(session, method)(url, timeout=timeout, **kwargs) as resp:
        resp.raise_for_status()
        return await resp.json()


async def ingest_gas_prices(session: ClientSession, conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    url = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={ETHERSCAN_KEY}"
    result = await fetch_json(session, url)
    data = result.get("result", {})
    timestamp = datetime.datetime.utcnow().isoformat()
    cur.execute(
        """
        INSERT OR REPLACE INTO gas_prices (timestamp, fast_gas, average_gas, slow_gas, base_fee, source)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            timestamp,
            float(data.get("FastGasPrice", 0)),
            float(data.get("ProposeGasPrice", 0)),
            float(data.get("SafeGasPrice", 0)),
            float(data.get("LastBlock", 0)),
            "etherscan_current",
        ),
    )
    conn.commit()
    logging.info("Stored current gas price")

    headers = {"x-dune-api-key": DUNE_API_KEY}
    url = f"https://api.dune.com/api/v1/query/{DUNE_QUERY_ID}/execute"
    execution = await fetch_json(session, url, method="post", headers=headers)
    exec_id = execution.get("execution_id")
    if not exec_id:
        return
    status_url = f"https://api.dune.com/api/v1/execution/{exec_id}/status"
    while True:
        state = (await fetch_json(session, status_url, headers=headers)).get("state")
        if state == "QUERY_STATE_COMPLETED":
            break
        await asyncio.sleep(5)
    results_url = f"https://api.dune.com/api/v1/execution/{exec_id}/results"
    res = await fetch_json(session, results_url, headers=headers)
    for row in res.get("rows", []):
        cur.execute(
            "INSERT OR IGNORE INTO gas_prices (timestamp, average_gas, source) VALUES (?, ?, ?)",
            (row.get("day", timestamp), row.get("avg_gas_gwei", 0), "dune"),
        )
    conn.commit()
    logging.info("Stored %s gas prices from Dune", len(res.get("rows", [])))


async def main() -> None:
    conn = await init_db()
    try:
        async with aiohttp.ClientSession() as session:
            await ingest_gas_prices(session, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())
