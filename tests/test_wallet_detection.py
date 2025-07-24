import sqlite3
import types


def setup_db(monkeypatch, wallet_module):
    monkeypatch.setattr(wallet_module, "DB_FILE", ":memory:")
    conn = sqlite3.connect(":memory:")
    wallet_module.init_wallet_db(conn)
    return conn


class DummyResp:
    def __init__(self, data):
        self._data = data
    def json(self):
        return self._data
    def raise_for_status(self):
        pass


def test_ingest_quicknode(monkeypatch, wallet_module):
    conn = setup_db(monkeypatch, wallet_module)
    data = {"result": {"value": [{"address": "w1", "uiAmount": 1}]}}
    monkeypatch.setattr(wallet_module.requests, "post", lambda *a, **k: DummyResp(data))
    wallet_module.ingest_quicknode_wallet_alerts(conn)
    rows = conn.execute("SELECT wallet, source FROM wallets").fetchall()
    assert rows[0] == ("w1", "quicknode")


def test_ingest_helius(monkeypatch, wallet_module):
    conn = setup_db(monkeypatch, wallet_module)
    data = [{"timestamp": "2023-01-01", "token": "sol", "amount": 2}]
    monkeypatch.setattr(wallet_module.requests, "get", lambda *a, **k: DummyResp(data))
    wallet_module.ingest_helius_wallet(conn)
    row = conn.execute("SELECT token, source FROM wallets").fetchone()
    assert row == ("sol", "helius")


def test_ingest_bitquery(monkeypatch, wallet_module):
    conn = setup_db(monkeypatch, wallet_module)
    data = {"data": {"solana": {"transfers": [{"block": {"timestamp": {"iso8601": "2023-01-01"}}, "receiverAddress": "w2", "amount": 3}]}}}
    monkeypatch.setattr(wallet_module.requests, "post", lambda *a, **k: DummyResp(data))
    wallet_module.ingest_bitquery_transfers(conn)
    row = conn.execute("SELECT wallet, source FROM wallets").fetchone()
    assert row == ("w2", "bitquery")


def test_ingest_wallet_scanner(monkeypatch, wallet_module):
    conn = setup_db(monkeypatch, wallet_module)
    data = {"transactions": [{"timestamp": "2023", "wallet": "w3", "token": "sol", "amount": 4}]}
    monkeypatch.setattr(wallet_module.requests, "get", lambda *a, **k: DummyResp(data))
    wallet_module.ingest_solana_wallet_scanner(conn)
    row = conn.execute("SELECT wallet, source FROM wallets").fetchone()
    assert row == ("w3", "scanner")


def test_ingest_geyser(monkeypatch, wallet_module):
    conn = setup_db(monkeypatch, wallet_module)
    data = {"events": [{"timestamp": "2023", "wallet": "w4", "token": "sol", "amount": 5}]}
    monkeypatch.setattr(wallet_module.requests, "get", lambda *a, **k: DummyResp(data))
    wallet_module.ingest_yellowstone_geyser(conn)
    row = conn.execute("SELECT wallet, source FROM wallets").fetchone()
    assert row == ("w4", "geyser")
