import types


def setup_in_memory_db(monkeypatch, db_module):
    monkeypatch.setattr(db_module, "DB_FILE", ":memory:")
    return db_module.init_db()


class DummyDF:
    def __init__(self, rows):
        self.rows = rows

    def iterrows(self):
        for i, row in enumerate(self.rows):
            yield i, row


class DummyChain:
    def __init__(self):
        self.calls = DummyDF([
            {"strike": 1, "lastPrice": 0.5, "impliedVolatility": 0.2, "openInterest": 10, "volume": 5}
        ])
        self.puts = DummyDF([
            {"strike": 1, "lastPrice": 0.4, "impliedVolatility": 0.3, "openInterest": 8, "volume": 7}
        ])


class DummyTicker:
    def __init__(self, ticker):
        self.options = ["2023-01-01"]

    def option_chain(self, exp):
        return DummyChain()


def test_ingest_yfinance_options(monkeypatch, options_module, db_module):
    conn = setup_in_memory_db(monkeypatch, db_module)
    monkeypatch.setattr(options_module, "yf", types.SimpleNamespace(Ticker=DummyTicker))
    options_module.ingest_yfinance_options(conn, ["AAPL"])
    rows = conn.cursor().execute(
        "SELECT ticker, option_type, strike FROM stock_options"
    ).fetchall()
    assert len(rows) == 2
    assert {r[1] for r in rows} == {"call", "put"}
