"""Optimized lightweight database wrapper."""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from queue import Queue


class OptimizedDB:
    def __init__(self, path: str = "untrimmed.db", pool_size: int = 5) -> None:
        self.path = path
        self.pool: "Queue[sqlite3.Connection]" = Queue(maxsize=pool_size)
        for _ in range(pool_size):
            conn = sqlite3.connect(self.path, check_same_thread=False)
            conn.row_factory = sqlite3.Row
            self.pool.put(conn)

    @contextmanager
    def get_connection(self) -> sqlite3.Connection:
        conn = self.pool.get()
        try:
            yield conn
        finally:
            self.pool.put(conn)
