import pytest
from utils import compute_vibe


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
