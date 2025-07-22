import types
import sqlite3
import pytest


def setup_in_memory_db(monkeypatch: pytest.MonkeyPatch, db_module):
    monkeypatch.setattr(db_module, 'DB_FILE', ':memory:')
    return db_module.init_db()


def test_ingest_gas_prices_inserts(monkeypatch: pytest.MonkeyPatch, gas_module, dune_module, db_module):
    conn = setup_in_memory_db(monkeypatch, db_module)

    monkeypatch.setattr(gas_module, 'DUNE_MAX_POLL', 1)
    monkeypatch.setattr(gas_module.time, 'sleep', lambda s: None)
    monkeypatch.setattr(gas_module, 'retry_func', lambda func, *a, **kw: func(*a, **kw))

    def dummy_get(url, *a, **kw):
        if 'gaschart' in url:
            return types.SimpleNamespace(json=lambda: {'result': [{'unixTimeStamp': '1', 'gasPrice': '42'}]}, raise_for_status=lambda: None)
        if 'gasoracle' in url:
            return types.SimpleNamespace(json=lambda: {'result': {'FastGasPrice': '10', 'ProposeGasPrice': '12', 'SafeGasPrice': '8', 'LastBlock': '123'}}, raise_for_status=lambda: None)
        if 'status' in url:
            return types.SimpleNamespace(json=lambda: {'state': 'QUERY_STATE_COMPLETED'}, raise_for_status=lambda: None)
        if 'results' in url:
            return types.SimpleNamespace(json=lambda: {'rows': [{'day': '2023-01-01', 'avg_gas_gwei': 30}]}, raise_for_status=lambda: None)
        raise AssertionError(f'Unexpected GET {url}')

    def dummy_post(url, *a, **kw):
        if 'execute' in url:
            return types.SimpleNamespace(json=lambda: {'execution_id': 'xyz'}, raise_for_status=lambda: None)
        raise AssertionError(f'Unexpected POST {url}')

    monkeypatch.setattr(gas_module.requests, 'get', dummy_get)
    monkeypatch.setattr(gas_module.requests, 'post', dummy_post)
    monkeypatch.setattr(dune_module.requests, 'get', dummy_get)
    monkeypatch.setattr(dune_module.requests, 'post', dummy_post)

    gas_module.ingest_gas_prices(conn)

    cur = conn.cursor()
    rows = cur.execute(
        'SELECT timestamp, fast_gas, average_gas, slow_gas, base_fee, source FROM gas_prices'
    ).fetchall()

    assert len(rows) == 3
    data = {row[5]: row for row in rows}

    hist = data['etherscan_historical']
    assert hist[2] == 42.0

    current = data['etherscan_current']
    assert current[1] == 10.0
    assert current[2] == 12.0
    assert current[3] == 8.0
    assert current[4] == 123.0

    dune = data['dune']
    assert dune[2] == 30

