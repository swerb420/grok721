import types
import logging


def setup_in_memory_db(monkeypatch, db_module):
    monkeypatch.setattr(db_module, "DB_FILE", ":memory:")
    return db_module.init_db()


# Test successful query execution returns rows

def test_execute_dune_query_success(monkeypatch, dune_module):
    monkeypatch.setattr(dune_module.time, "sleep", lambda s: None)

    def dummy_post(url, *a, **kw):
        assert "execute" in url
        return types.SimpleNamespace(json=lambda: {"execution_id": "abc"})

    def dummy_get(url, *a, **kw):
        if "status" in url:
            return types.SimpleNamespace(json=lambda: {"state": "QUERY_STATE_COMPLETED"})
        if "results" in url:
            return types.SimpleNamespace(json=lambda: {"rows": [{"foo": 1}]})
        raise AssertionError(f"Unexpected GET {url}")

    monkeypatch.setattr(dune_module.requests, "post", dummy_post)
    monkeypatch.setattr(dune_module.requests, "get", dummy_get)

    rows = dune_module.execute_dune_query("123", "token", max_poll=1)
    assert rows == [{"foo": 1}]


# Test errors during execution are logged and return empty results

def test_execute_dune_query_error_logs(monkeypatch, dune_module, caplog):
    def fail_post(*a, **kw):
        raise RuntimeError("boom")

    monkeypatch.setattr(dune_module.requests, "post", fail_post)

    with caplog.at_level(logging.ERROR):
        rows = dune_module.execute_dune_query("123", "token")
    assert rows == []
    assert any("Failed executing Dune query" in rec.getMessage() for rec in caplog.records)


# Test storing rows into the database

def test_store_dune_rows_inserts(monkeypatch, dune_module, db_module):
    conn = setup_in_memory_db(monkeypatch, db_module)
    rows = [{"foo": "bar"}, {"baz": 1}]

    dune_module.store_dune_rows(conn, "q1", rows)

    cur = conn.cursor()
    stored = cur.execute("SELECT query_id, data FROM dune_results ORDER BY id").fetchall()
    assert len(stored) == 2
    assert stored[0][0] == "q1"
    assert stored[0][1] == "{\"foo\": \"bar\"}"
    assert stored[1][1] == "{\"baz\": 1}"
