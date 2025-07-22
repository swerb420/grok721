import pytest
from utils import compute_vibe


def setup_in_memory_db(monkeypatch, main):
    monkeypatch.setattr(main, "DB_FILE", ":memory:")
    return main.init_db()


def test_store_tweet_inserts(monkeypatch, main_module):
    conn = setup_in_memory_db(monkeypatch, main_module)
    monkeypatch.setattr(main_module, "sentiment_analyzer", lambda text: [{"label": "POSITIVE", "score": 0.5}])
    item = {
        "id": "123",
        "user": {"username": "alice"},
        "created_at": "2023-01-01T00:00:00Z",
        "text": "great news",
        "favorite_count": 10,
        "retweet_count": 2,
        "reply_count": 1,
        "media": ["img1"],
    }
    tweet = main_module.store_tweet(conn, item)

    cur = conn.cursor()
    row = cur.execute(
        "SELECT id, username, text, sentiment_label, sentiment_score, vibe_score, vibe_label FROM tweets WHERE id=?",
        ("123",),
    ).fetchone()

    assert row is not None
    assert row[0] == "123"
    assert row[1] == "alice"
    assert row[2] == "great news"
    assert row[3] == "POSITIVE"
    assert row[4] == 0.5
    expected_vibe, expected_label = compute_vibe("POSITIVE", 0.5, 10, 2, 1)
    assert pytest.approx(row[5]) == expected_vibe
    assert row[6] == expected_label

    assert tweet.text == item["text"]

