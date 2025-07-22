"""Simplified orchestration script using the pipeline modules."""
from __future__ import annotations

import logging
import sqlite3
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apify_client import ApifyClient
from telegram import Bot

from config import get_config
from pipelines import init_db
from pipelines.gas import ingest_gas_prices
from pipelines.tweets import fetch_tweets
from pipelines.options import ingest_yfinance_options

APIFY_TOKEN = get_config("APIFY_TOKEN", "apify_api_xxxxxxxxxx")
TELEGRAM_BOT_TOKEN = get_config("TELEGRAM_BOT_TOKEN", "xxxxxxxxxx:xxxxxxxxxx")
DB_FILE = get_config("DB_FILE", "super_db.db")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def main() -> None:
    """Entry point for running the basic pipeline."""
    with sqlite3.connect(DB_FILE, check_same_thread=False) as conn:
        init_db(conn)
        client = ApifyClient(APIFY_TOKEN)
        bot = Bot(TELEGRAM_BOT_TOKEN)

        ingest_gas_prices(conn)
        fetch_tweets(client, conn, bot)
        try:
            ingest_yfinance_options(conn)
        except Exception as exc:
            logging.warning("Options ingestion failed: %s", exc)

        scheduler = BackgroundScheduler()
        scheduler.add_job(lambda: fetch_tweets(client, conn, bot), "interval", hours=1)
        scheduler.add_job(lambda: ingest_gas_prices(conn), "interval", hours=6)
        scheduler.add_job(lambda: ingest_yfinance_options(conn), "interval", hours=24)
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
