"""Gas price ingestion helpers."""
from __future__ import annotations

import datetime
import logging
import random
import time
from typing import Any, Callable

from config import get_config

try:  # pragma: no cover - optional dependency for tests
    import requests
except Exception:  # ModuleNotFoundError during tests
    requests = None  # type: ignore

from .dune import execute_dune_query

ETHERSCAN_KEY = get_config("ETHERSCAN_KEY", "xxxxxxxxxx")
DUNE_API_KEY = get_config("DUNE_API_KEY", "xxxxxxxxxx")
DUNE_QUERY_ID = get_config("DUNE_QUERY_ID", "5081617")
DUNE_MAX_POLL = 60
REQUEST_TIMEOUT = 30


def retry_func(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Retry helper with exponential backoff."""
    retries = kwargs.pop("retries", 5)
    base_backoff = kwargs.pop("base_backoff", 1)
    if func in {
        requests.get,
        requests.post,
        requests.put,
        requests.delete,
        requests.patch,
        requests.head,
        requests.options,
    }:
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - best effort logging
            last_exc = exc
            wait = base_backoff * (2 ** attempt) + random.random()
            logging.warning(
                "Retry %s/%s after error: %s", attempt + 1, retries, exc
            )
            time.sleep(wait)
    if last_exc is not None:
        raise last_exc
    raise RuntimeError(f"Failed after {retries} retries")


def ingest_gas_prices(conn) -> None:
    """Fetch gas price data from Etherscan and Dune."""
    if requests is None:
        raise RuntimeError("requests library not available")
    cur = conn.cursor()
    url = f"https://api.etherscan.io/api?module=gastracker&action=gaschart&apikey={ETHERSCAN_KEY}"
    response = retry_func(requests.get, url)
    response.raise_for_status()
    try:
        result = response.json().get("result", [])
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Error parsing Etherscan historical gas data: %s", exc)
        result = []
    for entry in result:
        try:
            timestamp = datetime.datetime.fromtimestamp(int(entry["unixTimeStamp"])).isoformat()
            average_gas = float(entry["gasPrice"])
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.warning("Malformed gas price entry: %s", exc)
            continue
        cur.execute(
            "INSERT OR IGNORE INTO gas_prices (timestamp, average_gas, source) VALUES (?, ?, ?)",
            (timestamp, average_gas, "etherscan_historical"),
        )
    conn.commit()
    logging.info("Ingested %s historical gas prices", len(result))

    url = f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={ETHERSCAN_KEY}"
    response = retry_func(requests.get, url)
    response.raise_for_status()
    try:
        result = response.json().get("result", {})
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Error parsing Etherscan gas oracle data: %s", exc)
        result = {}
    timestamp = datetime.datetime.utcnow().isoformat()
    try:
        cur.execute(
            """
            INSERT OR IGNORE INTO gas_prices (
                timestamp, fast_gas, average_gas, slow_gas, base_fee, source
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                timestamp,
                float(result.get("FastGasPrice", 0)),
                float(result.get("ProposeGasPrice", 0)),
                float(result.get("SafeGasPrice", 0)),
                float(result.get("LastBlock", 0)),
                "etherscan_current",
            ),
        )
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.warning("Error storing current gas price: %s", exc)
    else:
        conn.commit()
        logging.info("Ingested current gas prices")

    rows = execute_dune_query(DUNE_QUERY_ID, DUNE_API_KEY, max_poll=DUNE_MAX_POLL)
    for row in rows:
        try:
            timestamp = row.get("day", datetime.datetime.utcnow().isoformat())
            average_gas = row.get("avg_gas_gwei", 0)
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.warning("Malformed Dune row: %s", exc)
            continue
        cur.execute(
            "INSERT OR IGNORE INTO gas_prices (timestamp, average_gas, source) VALUES (?, ?, ?)",
            (timestamp, average_gas, "dune"),
        )
    conn.commit()
    logging.info("Ingested %s gas prices from Dune", len(rows))
