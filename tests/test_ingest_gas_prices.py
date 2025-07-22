import sys
import types
import importlib
import sqlite3
import datetime

import pytest

# Provide lightweight stubs so that the main module can be imported without
# optional third party packages installed in the test environment.
if 'apify_client' not in sys.modules:
    apify_client = types.ModuleType('apify_client')
    apify_client.ApifyClient = lambda token: None
    sys.modules['apify_client'] = apify_client

if 'telegram' not in sys.modules:
    telegram = types.ModuleType('telegram')
    telegram.Bot = lambda token: None
    sys.modules['telegram'] = telegram

if 'apscheduler.schedulers.background' not in sys.modules:
    background = types.ModuleType('background')
    background.BackgroundScheduler = lambda *a, **k: None
    schedulers = types.ModuleType('schedulers')
    schedulers.background = background
    apscheduler = types.ModuleType('apscheduler')
    apscheduler.schedulers = schedulers
    sys.modules['apscheduler'] = apscheduler
    sys.modules['apscheduler.schedulers'] = schedulers
    sys.modules['apscheduler.schedulers.background'] = background

if 'transformers' not in sys.modules:
    transformers = types.ModuleType('transformers')
    transformers.pipeline = lambda *a, **k: (lambda text: [{'label': 'POSITIVE', 'score': 1.0}])
    transformers.AutoTokenizer = types.SimpleNamespace(from_pretrained=lambda *a, **k: None)
    transformers.AutoModelForSequenceClassification = types.SimpleNamespace(from_pretrained=lambda *a, **k: None)
    sys.modules['transformers'] = transformers

if 'requests' not in sys.modules:
    requests = types.ModuleType('requests')
    def _resp():
        class R:
            def json(self):
                return {}
        return R()
    requests.get = lambda *a, **k: _resp()
    requests.post = lambda *a, **k: _resp()
    sys.modules['requests'] = requests

main = importlib.import_module('main')


def setup_in_memory_db(monkeypatch: pytest.MonkeyPatch) -> sqlite3.Connection:
    monkeypatch.setattr(main, 'DB_FILE', ':memory:')
    return main.init_db()


def test_ingest_gas_prices_inserts(monkeypatch: pytest.MonkeyPatch):
    conn = setup_in_memory_db(monkeypatch)

    monkeypatch.setattr(main, 'DUNE_MAX_POLL', 1)
    monkeypatch.setattr(main.time, 'sleep', lambda s: None)
    monkeypatch.setattr(main, 'retry_func', lambda func, *a, **kw: func(*a, **kw))

    def dummy_get(url, *a, **kw):
        if 'gaschart' in url:
            return types.SimpleNamespace(json=lambda: {'result': [{'unixTimeStamp': '1', 'gasPrice': '42'}]})
        if 'gasoracle' in url:
            return types.SimpleNamespace(json=lambda: {'result': {'FastGasPrice': '10', 'ProposeGasPrice': '12', 'SafeGasPrice': '8', 'LastBlock': '123'}})
        if 'status' in url:
            return types.SimpleNamespace(json=lambda: {'state': 'QUERY_STATE_COMPLETED'})
        if 'results' in url:
            return types.SimpleNamespace(json=lambda: {'rows': [{'day': '2023-01-01', 'avg_gas_gwei': 30}]})
        raise AssertionError(f'Unexpected GET {url}')

    def dummy_post(url, *a, **kw):
        if 'execute' in url:
            return types.SimpleNamespace(json=lambda: {'execution_id': 'xyz'})
        raise AssertionError(f'Unexpected POST {url}')

    monkeypatch.setattr(main.requests, 'get', dummy_get)
    monkeypatch.setattr(main.requests, 'post', dummy_post)

    main.ingest_gas_prices(conn)

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
