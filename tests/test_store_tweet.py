import sys
import types
import importlib
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


def setup_in_memory_db(monkeypatch):
    monkeypatch.setattr(main, "DB_FILE", ":memory:")
    return main.init_db()


def test_store_tweet_inserts(monkeypatch):
    conn = setup_in_memory_db(monkeypatch)
    monkeypatch.setattr(main, "sentiment_analyzer", lambda text: [{"label": "POSITIVE", "score": 0.5}])
    item = {
        "id": "123",
        "user": {"username": "alice"},
        "created_at": "2023-01-01T00:00:00Z",
        "text": "great news",
        "favorite_count": 10,
        "retweet_count": 2,
        "reply_count": 1,
        "media": ["img1"]
    }
    tweet = main.store_tweet(conn, item)

    cur = conn.cursor()
    row = cur.execute(
        "SELECT id, username, text, sentiment_label, sentiment_score, vibe_score, vibe_label FROM tweets WHERE id=?",
        ("123",)
    ).fetchone()

    assert row is not None
    assert row[0] == "123"
    assert row[1] == "alice"
    assert row[2] == "great news"
    assert row[3] == "POSITIVE"
    assert row[4] == 0.5
    expected_vibe, expected_label = main.compute_vibe("POSITIVE", 0.5, 10, 2, 1)
    assert pytest.approx(row[5]) == expected_vibe
    assert row[6] == expected_label

    assert tweet.text == item["text"]


