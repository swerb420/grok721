import os
import json
import time
import datetime
import sqlite3
import threading
import random
from apify_client import ApifyClient
from transformers import pipeline, AutoTokenizer, AutoModelForSequenceClassification, Trainer, TrainingArguments
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Updater, CallbackQueryHandler, CommandHandler
import logging
import requests
import pandas as pd
import numpy as np
from scipy.stats import pearsonr, spearmanr
from statsmodels.tsa.stattools import grangercausalitytests
from statsmodels.tsa.vector_ar.var_model import VAR
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
from apscheduler.schedulers.background import BackgroundScheduler
from backtrader import Cerebro, Strategy, indicators
from backtrader.feeds import PandasData
from pandas_gbq import read_gbq  # For BigQuery; pip install pandas-gbq
import ccxt  # For order books
import plotly.express as px  # For interactive dashboard; pip install plotly
from catboost import CatBoostRegressor  # For ensembles; pip install catboost xgboost lightgbm
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor
from sklearn.ensemble import VotingRegressor
import re  # For wallet extraction
from concurrent.futures import ThreadPoolExecutor, as_completed  # For concurrency

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Config - Replace with your actual keys/tokens
APIFY_TOKEN = "YOUR_APIFY_TOKEN_HERE"
TELEGRAM_BOT_TOKEN = "YOUR_TELEGRAM_BOT_TOKEN_HERE"
TELEGRAM_CHAT_ID = "YOUR_TELEGRAM_CHAT_ID_HERE"
ALPHA_VANTAGE_KEY = "YOUR_ALPHA_VANTAGE_KEY_HERE"  # Free at alphavantage.co
COINGLASS_KEY = "YOUR_COINGLASS_KEY_HERE"  # From coinglass.com/api
ETHERSCAN_KEY = "YOUR_ETHERSCAN_KEY_HERE"  # From etherscan.io
DUNE_API_KEY = "YOUR_DUNE_API_KEY_HERE"  # From dune.com
DUNE_QUERY_ID = "YOUR_DUNE_QUERY_ID_HERE"  # Create query on Dune UI for wallet tx, get ID
STOCKTWITS_TOKEN = "YOUR_STOCKTWITS_TOKEN_HERE"  # Free signup at developers.stocktwits.com
GOOGLE_CLOUD_PROJECT = "YOUR_GOOGLE_PROJECT_ID"  # For BigQuery
NEWSAPI_KEY = "YOUR_NEWSAPI_KEY_HERE"  # From newsapi.org
QUANDL_KEY = "YOUR_QUANDL_KEY_HERE"  # From nasdaq.com/data-link (free tier)
FRED_API_KEY = "YOUR_FRED_API_KEY_HERE"  # From api.stlouisfed.org
WORLD_BANK_API = "https://api.worldbank.org/v2"  # No key needed
DB_FILE = "super_db.db"
ACTOR_ID = "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
USERNAMES = ["onchainlens", "unipcs", "stalkchain", "elonmusk", "example2"]  # Include tracking accounts
SELECT_ACCOUNTS = []  # Select accounts for visual analysis, managed via Telegram
MAX_TWEETS_PER_USER = 10000
MAX_RETRIES = 5
BASE_BACKOFF = 1
INCREMENTAL = False  # For max data, fetch historical
HISTORICAL_START = "2020-01-01"  # Deeper for patterns
CREDIT_THRESHOLD = 0.8
MONTHLY_CREDITS = 49.0
WALLETS = ['0xexample_whale1', '0xexample_whale2']  # Add tracked wallets, dynamically expanded
PERP_SYMBOLS = ['BTC', 'ETH']  # For funding/OI
EXCHANGES = ['binance', 'bybit', 'okx', 'uniswap_v3']  # For order books
TRACKING_ACCOUNTS = ['onchainlens', 'unipcs', 'stalkchain']  # Accounts to extract wallets from tweets
ECONOMIC_SERIES = ['GDP', 'UNRATE', 'CPIAUCSL']  # FRED series: GDP, Unemployment, CPI
NEWS_KEYWORDS = ['bitcoin', 'ethereum', 'crypto regulation']  # For NewsAPI
QUANDL_DATASETS = ['USTREASURY/YIELD', 'FRED/GDP']  # Free datasets
WORLD_BANK_INDICATORS = ['NY.GDP.MKTP.CD', 'SL.UEM.TOTL.ZS']  # GDP, Unemployment

# ML Models
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
lda_vectorizer = CountVectorizer(max_df=0.95, min_df=2, stop_words='english')
lda_model = LatentDirichletAllocation(n_components=20, random_state=42, n_jobs=-1)  # Parallel jobs
fintwit_tokenizer = AutoTokenizer.from_pretrained("StephanAkkerman/FinTwitBERT")
fintwit_model = AutoModelForSequenceClassification.from_pretrained("StephanAkkerman/FinTwitBERT-sentiment", num_labels=3)  # For sentiment; adjust for regression

# Lock for DB concurrency
db_lock = threading.Lock()

def compute_vibe(sentiment_label, sentiment_score, likes, retweets, replies):
    if likes is None or retweets is None or replies is None:
        logging.warning("Missing engagement metrics, using defaults")
        likes = retweets = replies = 0
    engagement = (likes + retweets * 2 + replies) / 1000.0
    base_score = sentiment_score if sentiment_label == "POSITIVE" else -sentiment_score
    vibe_score = (base_score + engagement) * 5
    vibe_score = min(max(vibe_score, 0), 10)
    if vibe_score > 7: vibe_label = "Hype/Positive Impact"
    elif vibe_score > 5: vibe_label = "Engaging/Neutral"
    elif vibe_score > 3: vibe_label = "Controversial/Mixed"
    else: vibe_label = "Negative/Low Engagement"
    return vibe_score, vibe_label

def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=30)  # Increased timeout for concurrency
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL;')  # WAL for better read/write concurrency
    cur.execute('PRAGMA synchronous = NORMAL;')  # Balance safety/performance
    cur.execute('PRAGMA cache_size = -64000;')  # 64MB cache for faster queries
    # All previous tables
    cur.execute('''
        CREATE TABLE IF NOT EXISTS tweets (
            id TEXT PRIMARY KEY,
            username TEXT,
            created_at TEXT,
            fetch_time TEXT,
            text TEXT,
            likes INTEGER,
            retweets INTEGER,
            replies INTEGER,
            media JSON,
            sentiment_label TEXT,
            sentiment_score FLOAT,
            vibe_score FLOAT,
            vibe_label TEXT,
            analysis JSON,
            approved BOOLEAN,
            source TEXT DEFAULT 'twitter'
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tweets_date ON tweets (created_at);')  # Index for time queries
    # Similar for other tables...
    cur.execute('''
        CREATE TABLE IF NOT EXISTS economic_indicators (
            series TEXT,
            date TEXT,
            value REAL,
            source TEXT,
            PRIMARY KEY (series, date)
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_econ_date ON economic_indicators (date);')
    # Other tables as before
    conn.commit()
    return conn

def ingest_free_datasets(conn):
    # Expanded with concurrency
    def ingest_csv(path):
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, low_memory=False, on_bad_lines='warn')
                if 'text' in df.columns:
                    df['sentiment_label'], df['sentiment_score'] = zip(*df['text'].apply(compute_sentiment))
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute("INSERT OR IGNORE INTO tweets (id, created_at, text, sentiment_label, sentiment_score, source) VALUES (?, ?, ?, ?, ?, ?)",
                                    (str(random.randint(1,1000000)), row.get('date', datetime.datetime.now().isoformat()), row.get('text', ''), row.get('sentiment_label', 'NEUTRAL'), row.get('sentiment_score', 0.5), 'kaggle'))
                    conn.commit()
                logging.info(f"Ingested {len(df)} from {path}")
            except Exception as e:
                logging.error(f"Error ingesting {path}: {e}")

    kaggle_paths = ["path/to/sentiment140.csv", "path/to/crypto_tweets.csv", "path/to/reddit_crypto.csv", "path/to/perpetuals-funding-rates.csv", "path/to/bitcoin-blockchain.csv"]  # Add yours
    with ThreadPoolExecutor(max_workers=4) as executor:  # Concurrent ingestion
        futures = [executor.submit(ingest_csv, path) for path in kaggle_paths]
        for future in as_completed(futures):
            future.result()

    # BigQuery, StockTwits as before, with try-except

def ingest_alpha_vantage_economic(conn):
    series = ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS']  # Expanded
    for s in series:
        url = f"https://www.alphavantage.co/query?function={s}&apikey={ALPHA_VANTAGE_KEY}"
        response = retry_func(requests.get, url).json()
        data = response.get('data', [])
        with db_lock:
            cur = conn.cursor()
            for entry in data:
                cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source) VALUES (?, ?, ?, ?)",
                            (s, entry.get('date'), float(entry.get('value', 0)), 'alpha_vantage'))
            conn.commit()
        logging.info(f"Ingested {len(data)} for Alpha Vantage series {s}")
        time.sleep(12)  # Rate limit 5/min

def ingest_coingecko_crypto(conn):
    cryptos = ['bitcoin', 'ethereum', 'solana', 'ripple']  # Expanded
    for crypto in cryptos:
        url = f"{COINGECKO_API}/coins/{crypto}/market_chart/range?vs_currency=usd&from=1262304000&to={int(time.time())}"  # From 2010
        response = retry_func(requests.get, url).json()
        prices = response.get('prices', [])
        volumes = response.get('total_volumes', [])
        with db_lock:
            cur = conn.cursor()
            for i, (ts, price) in enumerate(prices):
                date = datetime.datetime.fromtimestamp(ts/1000).isoformat()
                volume = volumes[i][1] if i < len(volumes) else 0
                cur.execute("INSERT OR REPLACE INTO prices (ticker, date, close, volume, type) VALUES (?, ?, ?, ?, ?)",
                            (crypto.upper(), date, price, volume, 'crypto'))
            conn.commit()
        logging.info(f"Ingested {len(prices)} data points for CoinGecko {crypto}")
        time.sleep(2)  # Rate limit

def ingest_fred_economic(conn):
    series = ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'INDPRO']  # Expanded
    for s in series:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={s}&api_key={FRED_API_KEY}&file_type=json&observation_start=1960-01-01"
        response = retry_func(requests.get, url).json()
        observations = response.get('observations', [])
        with db_lock:
            cur = conn.cursor()
            for obs in observations:
                cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source) VALUES (?, ?, ?, ?)",
                            (s, obs.get('date'), float(obs.get('value', '0').replace('.', '0')), 'fred'))
            conn.commit()
        logging.info(f"Ingested {len(observations)} for FRED series {s}")
        time.sleep(1)  # Gentle rate

def ingest_newsapi(conn):
    for keyword in NEWS_KEYWORDS:
        url = f"https://newsapi.org/v2/everything?q={keyword}&from={HISTORICAL_START}&sortBy=publishedAt&apiKey={NEWSAPI_KEY}"
        response = retry_func(requests.get, url).json()
        articles = response.get('articles', [])
        with db_lock:
            cur = conn.cursor()
            for art in articles:
                text = art.get('title', '') + ' ' + art.get('description', '')
                sentiment_label, sentiment_score = compute_sentiment(text)
                cur.execute("INSERT OR IGNORE INTO tweets (id, created_at, text, sentiment_label, sentiment_score, source) VALUES (?, ?, ?, ?, ?, ?)",
                            (art.get('url'), art.get('publishedAt'), text, sentiment_label, sentiment_score, 'newsapi'))
            conn.commit()
        logging.info(f"Ingested {len(articles)} news for {keyword}")
        time.sleep(1)  # Rate limit

def ingest_quandl(conn):
    for dataset in QUANDL_DATASETS:
        url = f"https://data.nasdaq.com/api/v3/datasets/{dataset}.json?api_key={QUANDL_KEY}"
        response = retry_func(requests.get, url).json()
        data = response.get('dataset', {}).get('data', [])
        with db_lock:
            cur = conn.cursor()
            for row in data:
                date = row[0]
                value = float(row[1]) if len(row) > 1 else 0
                cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source) VALUES (?, ?, ?, ?)",
                            (dataset, date, value, 'quandl'))
            conn.commit()
        logging.info(f"Ingested {len(data)} for Quandl dataset {dataset}")
        time.sleep(2)  # Rate limit

def ingest_world_bank(conn):
    for indicator in WORLD_BANK_INDICATORS:
        url = f"{WORLD_BANK_API}/country/all/indicator/{indicator}?date=1960:2025&format=json&per_page=10000"
        response = retry_func(requests.get, url).json()
        data = response[1] if len(response) > 1 else []
        with db_lock:
            cur = conn.cursor()
            for entry in data:
                date = entry.get('date')
                value = entry.get('value')
                if value is not None:
                    cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source) VALUES (?, ?, ?, ?)",
                                (indicator, date, float(value), 'world_bank'))
            conn.commit()
        logging.info(f"Ingested {len(data)} for World Bank indicator {indicator}")
        time.sleep(1)  # Gentle rate

def main():
    client = ApifyClient(APIFY_TOKEN)
    conn = init_db()
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    updater = Updater(token=TELEGRAM_BOT_TOKEN, use_context=True)
    dispatcher = updater.dispatcher
    dispatcher.add_handler(CallbackQueryHandler(approval_handler))
    dispatcher.add_handler(CommandHandler('add', add_account))
    dispatcher.add_handler(CommandHandler('remove', remove_account))
    dispatcher.add_handler(CommandHandler('list', list_accounts))
    updater.start_polling()
    
    # Concurrent ingests for speed/memory
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = [
            executor.submit(ingest_free_datasets, conn),
            executor.submit(ingest_wallets, conn),
            executor.submit(ingest_perps, conn),
            executor.submit(ingest_order_books, conn),
            executor.submit(ingest_gas_prices, conn),
            executor.submit(ingest_alpha_vantage_economic, conn),
            executor.submit(ingest_coingecko_crypto, conn),
            executor.submit(ingest_fred_economic, conn),
            executor.submit(ingest_newsapi, conn),
            executor.submit(ingest_quandl, conn),
            executor.submit(ingest_world_bank, conn)
        ]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Ingest thread error: {e}")
    
    fetch_tweets(client, conn, bot)
    analyze_patterns(conn)
    ensemble_prediction(conn)
    generate_dashboard(conn)
    time_series_plots(conn)
    export_for_finetuning(conn)
    backtest_strategies(conn)
    
    scheduler = BackgroundScheduler(max_workers=10)  # More workers
    scheduler.add_job(lambda: fetch_tweets(client, conn, bot), 'cron', hour=1, misfire_grace_time=3600)  # Grace for delays
    scheduler.add_job(lambda: ingest_wallets(conn), 'cron', hour=3)
    scheduler.add_job(lambda: ingest_perps(conn), 'cron', hour=4)
    scheduler.add_job(lambda: ingest_order_books(conn), 'cron', hour=5)
    scheduler.add_job(lambda: ingest_gas_prices(conn), 'cron', hour=6)
    scheduler.add_job(lambda: ingest_alpha_vantage_economic(conn), 'cron', hour=7)
    scheduler.add_job(lambda: ingest_coingecko_crypto(conn), 'cron', hour=8)
    scheduler.add_job(lambda: ingest_fred_economic(conn), 'cron', hour=9)
    scheduler.add_job(lambda: ingest_newsapi(conn), 'cron', hour=10)
    scheduler.add_job(lambda: ingest_quandl(conn), 'cron', hour=11)
    scheduler.add_job(lambda: ingest_world_bank(conn), 'cron', hour=12)
    scheduler.add_job(lambda: analyze_patterns(conn), 'cron', hour=2)
    scheduler.add_job(lambda: ensemble_prediction(conn), 'cron', hour=13)
    scheduler.add_job(lambda: generate_dashboard(conn), 'cron', hour=14)
    scheduler.add_job(lambda: time_series_plots(conn), 'cron', hour=15)
    scheduler.start()
    
    updater.idle()
    conn.close()

if __name__ == "__main__":
    main()
