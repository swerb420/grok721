import sys
import types
import importlib
import pytest

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
compute_vibe = main.compute_vibe


def test_compute_vibe_positive():
    score, label = compute_vibe("POSITIVE", 0.9, 10, 5, 2)
    assert score > 0
    assert label


@pytest.mark.parametrize(
    "sentiment_label,sentiment_score,likes,retweets,replies,expected_label",
    [
        ("NEGATIVE", 0.5, 0, 0, 0, "Negative/Low Engagement"),
        ("POSITIVE", 0.9, -1000, -500, -250, "Negative/Low Engagement"),
        ("POSITIVE", 0.6, 300, 0, 0, "Controversial/Mixed"),
        ("POSITIVE", 0.6, 700, 0, 0, "Engaging/Neutral"),
        ("POSITIVE", 0.6, 950, 0, 0, "Hype/Positive Impact"),
    ],
)
def test_compute_vibe_labels(
    sentiment_label, sentiment_score, likes, retweets, replies, expected_label
):
    _, label = compute_vibe(sentiment_label, sentiment_score, likes, retweets, replies)
    assert label == expected_label
