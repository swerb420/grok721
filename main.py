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
from utils import compute_vibe

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
HISTORICAL_START = "2017-01-01"
# Minute-level data is only stored for recent history to save space
HISTORICAL_MINUTE_START = "2022-01-01"
MINUTE_VALUABLE_SOURCES = ["eodhd", "twelve_data", "dukascopy", "barchart"]
REQUEST_TIMEOUT = 30  # seconds
DUNE_MAX_POLL = 60  # Maximum poll attempts (~5 minutes)

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
    if func in {
        requests.get,
        requests.post,
        requests.put,
        requests.delete,
        requests.patch,
        requests.head,
        requests.options,
    }:
        kwargs.setdefault("timeout", REQUEST_TIMEOUT)
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
        CREATE TABLE IF NOT EXISTS high_res_prices (
            ticker TEXT,
            date TEXT,
            close REAL,
            volume REAL,
            volatility REAL,
            momentum REAL,
            PRIMARY KEY (ticker, date)
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
    try:
        result = response.json().get("result", [])
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Error parsing Etherscan historical gas data: %s", exc)
        result = []
    for entry in result:
        try:
            timestamp = datetime.datetime.fromtimestamp(int(entry["unixTimeStamp"])).isoformat()
            average_gas = float(entry["gasPrice"])
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.warning("Malformed gas price entry: %s", exc)
            continue
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
    try:
        result = response.json().get("result", {})
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Error parsing Etherscan gas oracle data: %s", exc)
        result = {}
    timestamp = datetime.datetime.utcnow().isoformat()
    try:
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
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.warning("Error storing current gas price: %s", exc)
    else:
        conn.commit()
        logging.info("Ingested current gas prices")

    # Dune endpoint (simplified execution)
    headers = {"x-dune-api-key": DUNE_API_KEY}
    url = f"https://api.dune.com/api/v1/query/{DUNE_QUERY_ID}/execute"
    response = retry_func(requests.post, url, headers=headers)
    try:
        execution_id = response.json().get("execution_id")
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Failed parsing Dune execution response: %s", exc)
        execution_id = None
    if execution_id:
        status_url = f"https://api.dune.com/api/v1/execution/{execution_id}/status"
        for _ in range(DUNE_MAX_POLL):
            try:
                state = retry_func(requests.get, status_url, headers=headers).json().get("state")
            except Exception as exc:  # pragma: no cover - best effort logging
                logging.error("Failed parsing Dune status: %s", exc)
                break
            if state == "QUERY_STATE_COMPLETED":
                break
            time.sleep(5)
        else:
            logging.warning("Dune query status polling timed out")
            return
        results_url = f"https://api.dune.com/api/v1/execution/{execution_id}/results"
        response = retry_func(requests.get, results_url, headers=headers)
        try:
            rows = response.json().get("rows", [])
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.error("Failed parsing Dune results: %s", exc)
            rows = []
        for row in rows:
            try:
                timestamp = row.get("day", datetime.datetime.utcnow().isoformat())
                average_gas = row.get("avg_gas_gwei", 0)
            except Exception as exc:  # pragma: no cover - best effort logging
                logging.warning("Malformed Dune row: %s", exc)
                continue
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
    try:
        dataset_id = run["defaultDatasetId"]
    except Exception as exc:  # pragma: no cover - best effort logging
        logging.error("Error parsing Apify run result: %s", exc)
        return
    for item in retry_func(client.dataset(dataset_id).iterate_items):
        try:
            tweet = store_tweet(conn, item)
        except Exception as exc:  # pragma: no cover - best effort logging
            logging.error("Error storing tweet: %s", exc)
            continue
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
        conn.close()


if __name__ == "__main__":
    main()
