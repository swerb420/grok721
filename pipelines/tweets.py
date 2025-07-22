"""Tweet ingestion and sentiment analysis helpers."""
from __future__ import annotations

import datetime
import json
import logging
from dataclasses import dataclass
from typing import Any, Callable, Iterable, List

import random
import time

import sqlite3
from apify_client import ApifyClient
from telegram import Bot
from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    pipeline,
)

from config import get_config
from utils import compute_vibe

from .gas import retry_func


def iterate_with_retry(client: ApifyClient, dataset_id: str):
    """Yield dataset items, retrying on transient errors."""
    offset = 0
    supports_offset = True
    while True:
        try:
            if supports_offset:
                iterator = retry_func(
                    client.dataset(dataset_id).iterate_items,
                    offset=offset,
                )
            else:
                iterator = retry_func(client.dataset(dataset_id).iterate_items)
        except TypeError:
            supports_offset = False
            iterator = retry_func(client.dataset(dataset_id).iterate_items)
        from itertools import islice

        iterator = islice(
            retry_func(client.dataset(dataset_id).iterate_items), offset, None
        )
        got_any = False
        try:
            for item in iterator:
                got_any = True
                if supports_offset:
                    offset += 1
                yield item
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.warning("Dataset iteration failed: %s", exc)
            continue
        if not supports_offset:
            break
        if not got_any:
            break

USERNAMES = ["onchainlens", "unipcs", "stalkchain", "elonmusk", "example2"]
MAX_TWEETS_PER_USER = 1000
HISTORICAL_START = get_config("HISTORICAL_START", "2017-01-01")
        break

USERNAMES = ["onchainlens", "unipcs", "stalkchain", "elonmusk", "example2"]
MAX_TWEETS_PER_USER = int(get_config("MAX_TWEETS_PER_USER", "1000"))
HISTORICAL_START = "2017-01-01"

APIFY_TOKEN = get_config("APIFY_TOKEN", "apify_api_xxxxxxxxxx")
TELEGRAM_BOT_TOKEN = get_config("TELEGRAM_BOT_TOKEN", "xxxxxxxxxx:xxxxxxxxxx")
TELEGRAM_CHAT_ID = get_config("TELEGRAM_CHAT_ID", "xxxxxxxxxx")
ACTOR_ID = get_config("ACTOR_ID", "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest")

sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
)
fintwit_tokenizer = AutoTokenizer.from_pretrained("StephanAkkerman/FinTwitBERT")
fintwit_model = AutoModelForSequenceClassification.from_pretrained(
    "StephanAkkerman/FinTwitBERT-sentiment", num_labels=3
)


@dataclass
class TweetData:
    """Structured tweet information used for storage."""

    id: str
    username: str
    created_at: str
    text: str
    likes: int
    retweets: int
    replies: int
    media: List[str]


def store_tweet(conn: sqlite3.Connection, item: dict) -> TweetData:
    """Persist a tweet in the database."""
    cur = conn.cursor()
    tweet = TweetData(
        id=item.get("id"),
        username=item.get("user", {}).get("username"),
        created_at=item.get("created_at"),
        text=item.get("text", ""),
        likes=item.get("favorite_count", 0),
        retweets=item.get("retweet_count", 0),
        replies=item.get("reply_count", 0),
        media=item.get("media", []),
    )
    media_json = json.dumps(tweet.media)

    try:
        sentiment = sentiment_analyzer(tweet.text[:512])[0]
    except Exception as exc:  # pragma: no cover - optional model may fail
        logging.warning("Sentiment analysis failed: %s", exc)
        sentiment = {"label": "NEUTRAL", "score": 0.0}

    vibe_score, vibe_label = compute_vibe(
        sentiment["label"], sentiment["score"], tweet.likes, tweet.retweets, tweet.replies
    )

    cur.execute(
        """
        INSERT OR IGNORE INTO tweets (
            id, username, created_at, fetch_time, text, likes, retweets,
            replies, media, sentiment_label, sentiment_score, vibe_score,
            vibe_label, analysis, approved, source
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            tweet.id,
            tweet.username,
            tweet.created_at,
            datetime.datetime.utcnow().isoformat(),
            tweet.text,
            tweet.likes,
            tweet.retweets,
            tweet.replies,
            media_json,
            sentiment["label"],
            float(sentiment["score"]),
            vibe_score,
            vibe_label,
            None,
            False,
            "twitter",
        ),
    )
    conn.commit()
    return tweet


def update_tweet_analysis(conn: sqlite3.Connection, tweet_id: str, analysis: dict) -> None:
    cur = conn.cursor()
    cur.execute(
        "UPDATE tweets SET analysis = ? WHERE id = ?",
        (json.dumps(analysis), tweet_id),
    )
    conn.commit()


def send_for_approval(bot: Bot, tweet_id: str, text: str, analysis: dict) -> None:
    """Notify Telegram chat with tweet text and analysis."""
    message = f"Tweet {tweet_id}\n{text}\nAnalysis: {analysis}"
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)


def monitor_costs(client: ApifyClient) -> None:
    try:
        user = client.user().get()
        credits = user.get("usage", {}).get("total", 0)
        logging.info("Current Apify usage credits: %s", credits)
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.warning("Unable to fetch Apify usage info: %s", exc)


def iterate_with_retry_func(
    iter_func: Callable[[], Iterable[Any]], *, retries: int = 5, base_backoff: float = 1.0
) -> Iterable[Any]:
    """Yield items from ``iter_func`` with simple retry logic."""
    for attempt in range(retries):
        try:
            for item in iter_func():
                yield item
            return
        except Exception as exc:  # pragma: no cover - depends on runtime errors
            if attempt >= retries - 1:
                raise
            wait = base_backoff * (2 ** attempt) + random.random()
            logging.warning("Retry %s/%s after iteration error: %s", attempt + 1, retries, exc)
            time.sleep(wait)


def fetch_tweets(client: ApifyClient, conn: sqlite3.Connection, bot: Bot) -> None:
    """Fetch tweets via Apify actor and store them in the database."""
    input_data = {
        "twitterHandles": USERNAMES,
        "maxResults": MAX_TWEETS_PER_USER * len(USERNAMES),
        "end": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
        "start": HISTORICAL_START,
    }
    run = retry_func(client.actor(ACTOR_ID).call, run_input=input_data)
    try:
        dataset_id = run["defaultDatasetId"]
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Error parsing Apify run result: %s", exc)
        return
    for item in iterate_with_retry_func(lambda: client.dataset(dataset_id).iterate_items()):
        try:
            store_tweet(conn, item)
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.error("Error storing tweet: %s", exc)
            continue
    logging.info("Tweet ingestion complete")
    monitor_costs(client)
