import types
import pytest
from utils import compute_vibe, fetch_with_fallback


def test_compute_vibe_positive():
    score, label = compute_vibe("POSITIVE", 0.9, 10, 5, 2)
    assert score > 0
    assert label


@pytest.mark.parametrize(
    "sentiment_label,sentiment_score,likes,retweets,replies,expected_label",
    [
        ("NEGATIVE", 0.5, 0, 0, 0, "Negative/Low Engagement"),
        ("POSITIVE", 0.9, -1000, -500, -250, "Negative/Low Engagement"),
        ("POSITIVE", 0.9, None, None, None, "Controversial/Mixed"),
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


@pytest.mark.parametrize(
    "likes,retweets,replies",
    [
        (None, None, None),
        (None, 5, 1),
        (10, None, 1),
        (10, 5, None),
    ],
)
def test_compute_vibe_accepts_none(likes, retweets, replies):
    score_none, label_none = compute_vibe("POSITIVE", 0.5, likes, retweets, replies)
    score_zero, label_zero = compute_vibe(
        "POSITIVE",
        0.5,
        0 if likes is None else likes,
        0 if retweets is None else retweets,
        0 if replies is None else replies,
    )
    assert score_none == score_zero
    assert label_none == label_zero


class DummyError(Exception):
    def __init__(self, status):
        self.response = types.SimpleNamespace(status_code=status, headers={})


def test_fetch_with_fallback_retries(monkeypatch):
    import utils
    monkeypatch.setattr(utils.time, 'sleep', lambda s: None)
    calls = []

    def fake_fetch(interval='1min'):
        calls.append(interval)
        if len(calls) < 2:
            raise DummyError(429)
        return {'used': interval}

    result = fetch_with_fallback(fake_fetch)
    assert result == {'used': '5min'}
    assert calls == ['1min', '5min']


def test_fetch_with_fallback_failure(monkeypatch):
    import utils
    monkeypatch.setattr(utils.time, 'sleep', lambda s: None)

    def always_fail(interval='1min'):
        raise DummyError(500)

    with pytest.raises(DummyError):
        fetch_with_fallback(always_fail, pause=0)
