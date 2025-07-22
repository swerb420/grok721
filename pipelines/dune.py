"""Helpers for interacting with Dune Analytics."""
from __future__ import annotations

import json
import logging
import time
from typing import List

from config import get_config

try:  # pragma: no cover - optional dependency in tests
    import requests
except Exception:  # ModuleNotFoundError during tests
    requests = None  # type: ignore

DUNE_API_KEY = get_config("DUNE_API_KEY", "xxxxxxxxxx")


def execute_dune_query(
    query_id: str,
    api_key: str | None = None,
    *,
    max_poll: int = 60,
    poll_interval: float = 5.0,
) -> List[dict]:
    """Execute a Dune query and return rows of results."""
    if requests is None:
        raise RuntimeError("requests library not available")
    headers = {"x-dune-api-key": api_key or DUNE_API_KEY}
    url = f"https://api.dune.com/api/v1/query/{query_id}/execute"
    try:
        resp = requests.post(url, headers=headers)
        execution_id = resp.json().get("execution_id")
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Failed executing Dune query %s: %s", query_id, exc)
        return []
    if not execution_id:
        return []

    status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
    for _ in range(max_poll):
        try:
            state = requests.get(status_url, headers=headers).json().get("state")
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.error("Failed polling Dune status: %s", exc)
            return []
        if state == "QUERY_STATE_COMPLETED":
            break
        time.sleep(poll_interval)
    else:
        logging.warning("Dune query %s polling timed out", query_id)
        return []

    results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
    try:
        rows = requests.get(results_url, headers=headers).json().get("rows", [])
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Failed fetching Dune results: %s", exc)
        return []
    return rows


def store_dune_rows(conn, query_id: str, rows: list[dict]) -> None:
    """Store Dune query rows in the database."""
    cur = conn.cursor()
    ts = time.strftime("%Y-%m-%dT%H:%M:%S")
    for row in rows:
        cur.execute(
            "INSERT INTO dune_results (query_id, data, ingested_at) VALUES (?, ?, ?)",
            (query_id, json.dumps(row), ts),
        )
    conn.commit()
