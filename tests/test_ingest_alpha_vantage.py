import types
import sqlite3
import pytest


def setup_in_memory_db(monkeypatch: pytest.MonkeyPatch, module):
    monkeypatch.setattr(module, "DB_FILE", ":memory:")
    return module.init_db()


def test_ingest_alpha_vantage(monkeypatch: pytest.MonkeyPatch, advanced_module, db_module):
    conn = setup_in_memory_db(monkeypatch, advanced_module)

    monkeypatch.setattr(advanced_module, "ALPHA_ECON_SERIES", ["GDP"])
    monkeypatch.setattr(advanced_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(advanced_module, "retry_func", lambda func, *a, **kw: func(*a, **kw))

    def dummy_get(url, *a, **kw):
        return types.SimpleNamespace(
            json=lambda: {"data": [{"date": "2023-01-01", "value": "1"}]},
            raise_for_status=lambda: None,
        )

    monkeypatch.setattr(advanced_module.requests, "get", dummy_get)

    advanced_module.ingest_alpha_vantage_economic(conn)

    rows = conn.cursor().execute(
        "SELECT series, date, value, source FROM economic_indicators"
    ).fetchall()

    assert rows == [("GDP", "2023-01-01", 1.0, "alpha_vantage")]
