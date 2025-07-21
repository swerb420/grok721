# coding: utf-8
"""Simplified data pipeline script.

This version provides a minimal working skeleton of the large example shared
in the conversation. Many external data sources require API keys and network
access. The script focuses on data ingestion for tweets and gas prices using
placeholder implementations. Functions that depend on complex logic or external
APIs are stubbed so the overall program structure runs without errors.
"""

import os
import json
import time
import datetime
import sqlite3
import logging
import random
import requests
from typing import Any, Callable, List
from dataclasses import dataclass

from apify_client import ApifyClient
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification
from telegram import Bot

# Third party libraries imported in the original snippet. They are left here so
# users can extend the script with additional functionality if desired. They are
# not required for the basic workflow implemented below.
from apscheduler.schedulers.background import BackgroundScheduler
from config import get_config

# Configuration via environment variables or .env file
APIFY_TOKEN = get_config("APIFY_TOKEN", "apify_api_xxxxxxxxxx")
TELEGRAM_BOT_TOKEN = get_config("TELEGRAM_BOT_TOKEN", "xxxxxxxxxx:xxxxxxxxxx")
TELEGRAM_CHAT_ID = get_config("TELEGRAM_CHAT_ID", "xxxxxxxxxx")
ETHERSCAN_KEY = get_config("ETHERSCAN_KEY", "xxxxxxxxxx")
DUNE_API_KEY = get_config("DUNE_API_KEY", "xxxxxxxxxx")
DUNE_QUERY_ID = get_config("DUNE_QUERY_ID", "5081617")
DB_FILE = get_config("DB_FILE", "super_db.db")
ACTOR_ID = get_config("ACTOR_ID", "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest")
USERNAMES = ["onchainlens", "unipcs", "stalkchain", "elonmusk", "example2"]
MAX_TWEETS_PER_USER = 1000
MAX_RETRIES = 5
BASE_BACKOFF = 1
HISTORICAL_START = "2020-01-01"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


# ---------------------------------------------------------------------------
# Typed data models
# ---------------------------------------------------------------------------


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


@dataclass
class GasPrice:
    """Simplified gas price representation."""

    timestamp: str
    fast_gas: float
    average_gas: float
    slow_gas: float
    base_fee: float
    source: str

# Machine learning models used for tweet sentiment
sentiment_analyzer = pipeline(
    "sentiment-analysis",
    model="distilbert-base-uncased-finetuned-sst-2-english",
)
fintwit_tokenizer = AutoTokenizer.from_pretrained("StephanAkkerman/FinTwitBERT")
fintwit_model = AutoModelForSequenceClassification.from_pretrained(
    "StephanAkkerman/FinTwitBERT-sentiment", num_labels=3
)


def retry_func(func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
    """Retry helper with exponential backoff."""
    retries = kwargs.pop("retries", MAX_RETRIES)
    base_backoff = kwargs.pop("base_backoff", BASE_BACKOFF)
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as exc:  # pragma: no cover - best effort logging
            wait = base_backoff * (2 ** attempt) + random.random()
            logging.warning("Retry %s/%s after error: %s", attempt + 1, retries, exc)
            time.sleep(wait)
    raise RuntimeError(f"Failed after {retries} retries")


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

def init_db() -> sqlite3.Connection:
    """Initialise the SQLite database and required tables."""
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            username TEXT,
            created_at TEXT,
            fetch_time TEXT,
            text TEXT,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER,
            media TEXT,
            sentiment_label TEXT,
            sentiment_score FLOAT,
            vibe_score FLOAT,
            vibe_label TEXT,
            analysis TEXT,
            approved BOOLEAN,
            source TEXT DEFAULT 'twitter'
        )
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS gas_prices (
            timestamp TEXT PRIMARY KEY,
            fast_gas REAL,
            average_gas REAL,
            slow_gas REAL,
            base_fee REAL,
            source TEXT
        )
        """
    )
    cur.execute("PRAGMA journal_mode=WAL;")
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------

def compute_vibe(sentiment_label: str, sentiment_score: float, likes: int, retweets: int, replies: int):
    """Compute a simplified "vibe" score from sentiment and engagement."""
    engagement = (likes + retweets * 2 + replies) / 1000.0 if likes is not None else 0
    base_score = sentiment_score if sentiment_label == "POSITIVE" else -sentiment_score
    vibe_score = (base_score + engagement) * 5
    vibe_score = min(max(vibe_score, 0), 10)
    if vibe_score > 7:
        vibe_label = "Hype/Positive Impact"
    elif vibe_score > 5:
        vibe_label = "Engaging/Neutral"
    elif vibe_score > 3:
        vibe_label = "Controversial/Mixed"
    else:
        vibe_label = "Negative/Low Engagement"
    return vibe_score, vibe_label


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

    sentiment = sentiment_analyzer(tweet.text[:512])[0]
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
    message = f"Tweet {tweet_id}\n{text}\nAnalysis: {analysis}"
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)


def monitor_costs(client: ApifyClient) -> None:
    try:
        user = client.user().get()
        credits = user.get("usage", {}).get("total", 0)
        logging.info("Current Apify usage credits: %s", credits)
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.warning("Unable to fetch Apify usage info: %s", exc)


def analyze_visual(url: str, text: str):
    """Placeholder for any visual analysis on media."""
    logging.debug("Visual analysis stub for %s", url)
    return None


# ---------------------------------------------------------------------------
# Data ingestion functions
# ---------------------------------------------------------------------------

def ingest_gas_prices(conn: sqlite3.Connection) -> None:
    """Fetch gas price data from Etherscan and Dune."""
    cur = conn.cursor()
    url = (
        f"https://api.etherscan.io/api?module=gastracker&action=gaschart&apikey={ETHERSCAN_KEY}"
    )
    response = retry_func(requests.get, url)
    result = response.json().get("result", [])
    for entry in result:
        timestamp = datetime.datetime.fromtimestamp(int(entry["unixTimeStamp"])).isoformat()
        average_gas = float(entry["gasPrice"])
        cur.execute(
            "INSERT OR IGNORE INTO gas_prices (timestamp, average_gas, source) VALUES (?, ?, ?)",
            (timestamp, average_gas, "etherscan_historical"),
        )
    conn.commit()
    logging.info("Ingested %s historical gas prices", len(result))

    url = (
        f"https://api.etherscan.io/api?module=gastracker&action=gasoracle&apikey={ETHERSCAN_KEY}"
    )
    response = retry_func(requests.get, url)
    result = response.json().get("result", {})
    timestamp = datetime.datetime.utcnow().isoformat()
    cur.execute(
        """
        INSERT OR IGNORE INTO gas_prices (
            timestamp, fast_gas, average_gas, slow_gas, base_fee, source
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            timestamp,
            float(result.get("FastGasPrice", 0)),
            float(result.get("ProposeGasPrice", 0)),
            float(result.get("SafeGasPrice", 0)),
            float(result.get("LastBlock", 0)),
            "etherscan_current",
        ),
    )
    conn.commit()
    logging.info("Ingested current gas prices")

    # Dune endpoint (simplified execution)
    headers = {"x-dune-api-key": DUNE_API_KEY}
    url = f"https://api.dune.com/api/v1/query/{DUNE_QUERY_ID}/execute"
    response = retry_func(requests.post, url, headers=headers)
    execution_id = response.json().get("execution_id")
    if execution_id:
        status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
        while True:
            state = retry_func(requests.get, status_url, headers=headers).json().get("state")
            if state == "QUERY_STATE_COMPLETED":
                break
            time.sleep(5)
        results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
        response = retry_func(requests.get, results_url, headers=headers)
        rows = response.json().get("rows", [])
        for row in rows:
            timestamp = row.get("day", datetime.datetime.utcnow().isoformat())
            average_gas = row.get("avg_gas_gwei", 0)
            cur.execute(
                "INSERT OR IGNORE INTO gas_prices (timestamp, average_gas, source) VALUES (?, ?, ?)",
                (timestamp, average_gas, "dune"),
            )
        conn.commit()
        logging.info("Ingested %s gas prices from Dune", len(rows))


def fetch_tweets(client: ApifyClient, conn: sqlite3.Connection, bot: Bot) -> None:
    """Fetch tweets via Apify actor and store them in the database."""
    input_data = {
        "twitterHandles": USERNAMES,
        "maxResults": MAX_TWEETS_PER_USER * len(USERNAMES),
        "end": datetime.datetime.utcnow().strftime("%Y-%m-%d"),
        "start": HISTORICAL_START,
    }
    run = retry_func(client.actor(ACTOR_ID).call, run_input=input_data)
    for item in retry_func(client.dataset(run["defaultDatasetId"]).iterate_items):
        tweet = store_tweet(conn, item)
        # Additional processing such as media analysis could be added here
    logging.info("Tweet ingestion complete")
    monitor_costs(client)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main() -> None:
    conn = init_db()
    client = ApifyClient(APIFY_TOKEN)
    bot = Bot(TELEGRAM_BOT_TOKEN)

    ingest_gas_prices(conn)
    fetch_tweets(client, conn, bot)

    # Example of scheduled periodic ingestion
    scheduler = BackgroundScheduler()
    scheduler.add_job(lambda: fetch_tweets(client, conn, bot), "interval", hours=1)
    scheduler.add_job(lambda: ingest_gas_prices(conn), "interval", hours=6)
    scheduler.start()

    logging.info("Scheduler started. Press Ctrl+C to exit.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.shutdown()
        logging.info("Shutdown complete")


if __name__ == "__main__":
    main()
