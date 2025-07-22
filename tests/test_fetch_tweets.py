import types
import pytest


def setup_in_memory_db(monkeypatch, db_module):
    monkeypatch.setattr(db_module, "DB_FILE", ":memory:")
    return db_module.init_db()


class FailingDataset:
    def __init__(self, items):
        self.items = items
        self.calls = 0

    def iterate_items(self):
        self.calls += 1
        if self.calls == 1:
            def gen():
                raise RuntimeError("boom")
                yield  # pragma: no cover - never reached
            return gen()
        return iter(self.items)


class DummyClient:
    def __init__(self, dataset):
        self.dataset_obj = dataset

    def actor(self, actor_id):
        return types.SimpleNamespace(call=lambda run_input: {"defaultDatasetId": "d1"})

    def dataset(self, dataset_id):
        return self.dataset_obj

    def user(self):
        return types.SimpleNamespace(get=lambda: {})


def test_iterate_with_retry(monkeypatch, tweets_module, db_module):
    conn = setup_in_memory_db(monkeypatch, db_module)
    items = [{"id": "1", "user": {"username": "alice"}, "created_at": "2023", "text": "hi"}]
    dataset = FailingDataset(items)
    client = DummyClient(dataset)

    monkeypatch.setattr(tweets_module, "sentiment_analyzer", lambda text: [{"label": "POSITIVE", "score": 0.5}])
    monkeypatch.setattr(tweets_module, "monitor_costs", lambda c: None)
    monkeypatch.setattr(tweets_module, "retry_func", lambda func, *a, **kw: func(*a, **kw))
    monkeypatch.setattr(tweets_module.time, "sleep", lambda s: None)
    monkeypatch.setattr(tweets_module, "MAX_TWEETS_PER_USER", 1)
    monkeypatch.setattr(tweets_module, "USERNAMES", ["alice"])

    tweets_module.fetch_tweets(client, conn, None)

    rows = conn.cursor().execute("SELECT id FROM tweets").fetchall()
    assert len(rows) == 1
    assert dataset.calls == 2
