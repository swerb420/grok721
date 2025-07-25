"""Asynchronous version of the simplified data pipeline."""

import asyncio
try:
    import uvloop
except Exception:  # pragma: no cover - uvloop optional
    uvloop = None
import datetime
import logging

import aiohttp
from aiohttp import ClientSession

from config import get_config
from pipelines.db import init_db as sync_init_db
from pipelines.dune import execute_dune_query, store_dune_rows
from tenacity import retry, stop_after_attempt, wait_exponential

import sqlite3

# Configuration
APIFY_TOKEN = get_config("APIFY_TOKEN", "apify_api_xxxxxxxxxx")
ETHERSCAN_KEY = get_config("ETHERSCAN_KEY", "xxxxxxxxxx")
DUNE_API_KEY = get_config("DUNE_API_KEY", "xxxxxxxxxx")
DUNE_QUERY_ID = get_config("DUNE_QUERY_ID", "5081617")
HYPERLIQUID_STATS_QUERY_ID = get_config("HYPERLIQUID_STATS_QUERY_ID", "0")
HYPERLIQUID_QUERY_ID = get_config("HYPERLIQUID_QUERY_ID", "0")
GMX_ANALYTICS_QUERY_ID = get_config("GMX_ANALYTICS_QUERY_ID", "0")
HYPERLIQUID_FLOWS_QUERY_ID = get_config("HYPERLIQUID_FLOWS_QUERY_ID", "0")
PERPS_HYPERLIQUID_QUERY_ID = get_config("PERPS_HYPERLIQUID_QUERY_ID", "0")
GMX_IO_QUERY_ID = get_config("GMX_IO_QUERY_ID", "0")
AIRDROPS_WALLETS_QUERY_ID = get_config("AIRDROPS_WALLETS_QUERY_ID", "0")
SMART_WALLET_FINDER_QUERY_ID = get_config("SMART_WALLET_FINDER_QUERY_ID", "0")
WALLET_BALANCES_QUERY_ID = get_config("WALLET_BALANCES_QUERY_ID", "0")
DB_FILE = get_config("DB_FILE", "super_db.db")
DUNE_MAX_POLL = 60  # Maximum poll attempts (~5 minutes)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


async def async_init_db(conn: sqlite3.Connection | None = None) -> sqlite3.Connection:
    """Initialize the database using the synchronous helper."""
    return await asyncio.to_thread(sync_init_db, conn)


def store_rows(conn: sqlite3.Connection, query_id: str, rows: list[dict]) -> None:
    """Wrapper around :func:`pipelines.dune.store_dune_rows`."""
    store_dune_rows(conn, query_id, rows)


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
    try:
        data = result.get("result", {})
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Error parsing Etherscan gas oracle data: %s", exc)
        data = {}
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

    rows = await asyncio.to_thread(
        execute_dune_query,
        DUNE_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    for row in rows:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO gas_prices (timestamp, average_gas, source) VALUES (?, ?, ?)",
                (row.get("day", timestamp), row.get("avg_gas_gwei", 0), "dune"),
            )
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.warning("Error storing Dune row: %s", exc)
    conn.commit()
    logging.info("Stored %s gas prices from Dune", len(rows))


async def ingest_hyperliquid_stats(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        HYPERLIQUID_STATS_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, HYPERLIQUID_STATS_QUERY_ID, rows)
    logging.info("Stored %s Hyperliquid stats rows", len(rows))


async def ingest_hyperliquid(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        HYPERLIQUID_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, HYPERLIQUID_QUERY_ID, rows)
    logging.info("Stored %s Hyperliquid rows", len(rows))


async def ingest_gmx_analytics(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        GMX_ANALYTICS_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, GMX_ANALYTICS_QUERY_ID, rows)
    logging.info("Stored %s GMX analytics rows", len(rows))


async def ingest_hyperliquid_flows(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        HYPERLIQUID_FLOWS_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, HYPERLIQUID_FLOWS_QUERY_ID, rows)
    logging.info("Stored %s Hyperliquid flow rows", len(rows))


async def ingest_perps_hyperliquid(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        PERPS_HYPERLIQUID_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, PERPS_HYPERLIQUID_QUERY_ID, rows)
    logging.info("Stored %s perps/hyperliquid rows", len(rows))


async def ingest_gmx_io(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        GMX_IO_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, GMX_IO_QUERY_ID, rows)
    logging.info("Stored %s GMX.io rows", len(rows))


async def ingest_airdrops_wallets(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        AIRDROPS_WALLETS_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, AIRDROPS_WALLETS_QUERY_ID, rows)
    logging.info("Stored %s airdrop wallet rows", len(rows))


async def ingest_smart_wallet_finder(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        SMART_WALLET_FINDER_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, SMART_WALLET_FINDER_QUERY_ID, rows)
    logging.info("Stored %s smart wallet rows", len(rows))


async def ingest_wallet_balances(conn: sqlite3.Connection) -> None:
    rows = await asyncio.to_thread(
        execute_dune_query,
        WALLET_BALANCES_QUERY_ID,
        DUNE_API_KEY,
        max_poll=DUNE_MAX_POLL,
    )
    store_rows(conn, WALLET_BALANCES_QUERY_ID, rows)
    logging.info("Stored %s wallet balance rows", len(rows))


async def main() -> None:
    conn = await async_init_db()
    try:
        async with aiohttp.ClientSession() as session:
            await ingest_gas_prices(session, conn)
    finally:
        conn.close()


if __name__ == "__main__":
    if uvloop is not None:
        uvloop.install()
    asyncio.run(main())
