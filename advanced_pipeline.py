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
from scipy.stats import pearsonr, spearmanr, kendalltau
from statsmodels.tsa.stattools import grangercausalitytests
from statsmodels.tsa.vector_ar.var_model import VAR
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.model_selection import train_test_split, GridSearchCV
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
import yfinance as yf  # For Yahoo Finance; pip install yfinance
import cryptocompare  # For CryptoCompare; pip install cryptocompare
import openexchangerates  # For Open Exchange Rates; pip install openexchangerates, but free tier needs key
from edgar import Company, Filing  # For SEC EDGAR; pip install python-edgar
from noaa_sdk import NOAA  # For NOAA; pip install noaa-sdk
from github import Github  # For GitHub; pip install PyGithub
from imf import IMFData  # For IMF; pip install imfdatapy or similar
from config import get_config
from utils import compute_vibe

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler("system_log.txt"), logging.StreamHandler()])  # Detailed logging to file and console

# Config from environment or .env file
APIFY_TOKEN = get_config("APIFY_TOKEN", "apify_api_xxxxxxxxxx")
TELEGRAM_BOT_TOKEN = get_config("TELEGRAM_BOT_TOKEN", "xxxxxxxxxx:xxxxxxxxxx")
TELEGRAM_CHAT_ID = get_config("TELEGRAM_CHAT_ID", "xxxxxxxxxx")
ALPHA_VANTAGE_KEY = get_config("ALPHA_VANTAGE_KEY", "xxxxxxxxxx")
COINGLASS_KEY = get_config("COINGLASS_KEY", "xxxxxxxxxx")
ETHERSCAN_KEY = get_config("ETHERSCAN_KEY", "xxxxxxxxxx")
DUNE_API_KEY = get_config("DUNE_API_KEY", "xxxxxxxxxx")
DUNE_QUERY_ID = get_config("DUNE_QUERY_ID", "xxxxxxxxxx")
STOCKTWITS_TOKEN = get_config("STOCKTWITS_TOKEN", "xxxxxxxxxx")
GOOGLE_CLOUD_PROJECT = get_config("GOOGLE_CLOUD_PROJECT", "xxxxxxxxxx")
FINNHUB_KEY = get_config("FINNHUB_KEY", "xxxxxxxxxx")
POLYGON_KEY = get_config("POLYGON_KEY", "xxxxxxxxxx")
FRED_API_KEY = get_config("FRED_API_KEY", "xxxxxxxxxx")
NEWSAPI_KEY = get_config("NEWSAPI_KEY", "xxxxxxxxxx")
OPENEXCHANGE_KEY = get_config("OPENEXCHANGE_KEY", "xxxxxxxxxx")
GITHUB_TOKEN = get_config("GITHUB_TOKEN", "xxxxxxxxxx")
IMF_COUNTRIES = ['USA', 'CHN', 'JPN', 'EUR']  # Expanded
NOAA_LOCATIONS = [('NYC', 40.7128, -74.0060), ('LON', 51.5074, -0.1278)]  # For weather
SEC_COMPANIES = ['Tesla Inc', 'MicroStrategy Inc']  # Crypto-related
DB_FILE = get_config("DB_FILE", "super_db.db")
ACTOR_ID = get_config(
    "ACTOR_ID", "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
)
USERNAMES = ["onchainlens", "unipcs", "stalkchain", "elonmusk", "example2"]  # Include tracking accounts
SELECT_ACCOUNTS = []  # Select accounts for visual analysis, managed via Telegram
MAX_TWEETS_PER_USER = 10000
MAX_RETRIES = 10  # Increased for robustness
BASE_BACKOFF = 2  # Increased base
INCREMENTAL = False  # For max data, fetch historical
HISTORICAL_START = "2017-01-01"  # 8 years of data
HISTORICAL_MINUTE_START = "2022-01-01"
MINUTE_VALUABLE_SOURCES = ["eodhd", "twelve_data", "dukascopy", "barchart"]
CREDIT_THRESHOLD = 0.8
MONTHLY_CREDITS = 49.0
WALLETS = ['0xexample_whale1', '0xexample_whale2']  # Add tracked wallets, dynamically expanded
PERP_SYMBOLS = ['BTC', 'ETH', 'SOL', 'XRP']  # Expanded
EXCHANGES = ['binance', 'bybit', 'okx', 'uniswap_v3', 'sushiswap', 'coinbase']  # Expanded
TRACKING_ACCOUNTS = ['onchainlens', 'unipcs', 'stalkchain']  # Accounts to extract wallets from tweets
ALPHA_ECON_SERIES = ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'INDPRO', 'PCE', 'RSXFS', 'CSUSHPISA', 'MORTGAGE30US']  # Expanded
FINNHUB_SYMBOLS = ['AAPL', 'TSLA', 'BTC-USD', 'ETH-USD', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK-B']  # Expanded fundamentals
POLYGON_TICKERS = ['AAPL', 'TSLA', 'X:BTCUSD', 'X:ETHUSD', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK-B']  # Expanded
FRED_SERIES = ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'INDPRO', 'PCE', 'RSXFS', 'WALCL', 'M2SL', 'BOGMBASE', 'DGS10', 'T10YIE']  # Expanded
COINGECKO_CRYPTOS = ['bitcoin', 'ethereum', 'solana', 'ripple', 'cardano', 'dogecoin', 'polkadot', 'chainlink', 'uniswap', 'litecoin', 'stellar', 'avalanche-2', 'binancecoin', 'terra-luna', 'cosmos']  # Expanded
YFINANCE_TICKERS = ['^GSPC', '^IXIC', '^DJI', 'GC=F', 'CL=F', '^TNX', 'EURUSD=X', 'JPY=X', 'GBPUSD=X', 'BTC-USD', 'ETH-USD']  # Yahoo expanded
CRYPTOCOMPARE_COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE', 'DOT', 'LINK', 'UNI', 'LTC', 'XLM', 'AVAX', 'BNB', 'LUNA', 'ATOM']  # Expanded
OPENEXCHANGE_CURRENCIES = ['USD', 'EUR', 'JPY', 'GBP', 'CNY', 'AUD', 'CAD', 'CHF', 'SEK', 'NZD']  # Expanded forex
GITHUB_REPOS = ['bitcoin/bitcoin', 'ethereum/go-ethereum', 'solana-labs/solana', 'ripple/rippled', 'cardano-foundation/cardano-wallet']  # Crypto projects
NOAA_RADIUS = 50  # km for weather queries
SEC_EDGAR_START = "2015-01-01"

# ML Models with advanced setup
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english", device=0 if os.name != 'posix' else -1)  # GPU if available
lda_vectorizer = CountVectorizer(max_df=0.95, min_df=2, stop_words='english', ngram_range=(1,3), max_features=5000)  # N-grams for better topics
lda_model = LatentDirichletAllocation(n_components=30, random_state=42, n_jobs=-1, learning_method='online', batch_size=128)  # Online learning for large data
fintwit_tokenizer = AutoTokenizer.from_pretrained("StephanAkkerman/FinTwitBERT", max_length=512, truncation=True, padding='max_length')
fintwit_model = AutoModelForSequenceClassification.from_pretrained("StephanAkkerman/FinTwitBERT-sentiment", num_labels=3)  # For sentiment; adjust for regression

# Lock for DB concurrency
db_lock = threading.Lock()


def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=60)  # Longer timeout
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL;')
    cur.execute('PRAGMA synchronous = NORMAL;')
    cur.execute('PRAGMA cache_size = -256000;')  # 256MB cache
    cur.execute('PRAGMA busy_timeout = 120000;')  # 120s for locks
    cur.execute('PRAGMA foreign_keys = ON;')  # Enforce relations if added
    cur.execute('PRAGMA temp_store = MEMORY;')  # Temp in RAM for speed
    # Tweets table with expanded fields
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
            source TEXT DEFAULT 'twitter',
            entities JSON,  -- Expanded for mentions/hashtags
            geo JSON  -- For location if available
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tweets_created_at ON tweets (created_at DESC);')  # Optimized for time series
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tweets_username_source ON tweets (username, source);')
    # Similar detailed indices for all tables
    # reddit_posts
    cur.execute('''
        CREATE TABLE IF NOT EXISTS reddit_posts (
            id TEXT PRIMARY KEY,
            subreddit TEXT,
            created_at TEXT,
            text TEXT,
            score INTEGER,
            sentiment_label TEXT,
            sentiment_score FLOAT,
            source TEXT DEFAULT 'reddit',
            comments INTEGER,  -- Expanded
            author TEXT
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_reddit_created_at_subreddit ON reddit_posts (created_at DESC, subreddit);')
    # prices
    cur.execute('''
        CREATE TABLE IF NOT EXISTS prices (
            ticker TEXT,
            date TEXT,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume INTEGER,
            type TEXT,
            adjusted_close REAL,  -- Expanded for dividends/splits
            market_cap REAL,  -- Expanded
            PRIMARY KEY (ticker, date)
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_prices_date_ticker_type ON prices (date DESC, ticker, type);')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS high_res_prices (
            ticker TEXT,
            date TEXT,
            close REAL,
            volume REAL,
            volatility REAL,
            momentum REAL,
            PRIMARY KEY (ticker, date)
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_high_res_date_ticker ON high_res_prices (date DESC, ticker);')
    # stocktwits
    cur.execute('''
        CREATE TABLE IF NOT EXISTS stocktwits (
            id TEXT PRIMARY KEY,
            symbol TEXT,
            created_at TEXT,
            text TEXT,
            sentiment TEXT,
            source TEXT DEFAULT 'stocktwits',
            likes INTEGER,  -- Expanded
            conversation_id TEXT
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_stocktwits_created_at_symbol ON stocktwits (created_at DESC, symbol);')
    # wallets
    cur.execute('''
        CREATE TABLE IF NOT EXISTS wallets (
            address TEXT,
            tx_hash TEXT PRIMARY KEY,
            timestamp TEXT,
            value REAL,
            from_addr TEXT,
            to_addr TEXT,
            source TEXT,
            block_number INTEGER,  -- Expanded
            gas_used REAL
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_wallets_timestamp_address ON wallets (timestamp DESC, address);')
    # perps
    cur.execute('''
        CREATE TABLE IF NOT EXISTS perps (
            exchange TEXT,
            symbol TEXT,
            timestamp TEXT,
            funding_rate REAL,
            long_oi REAL,
            short_oi REAL,
            oi_total REAL,  -- Expanded
            volume_24h REAL,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_perps_timestamp_exchange_symbol ON perps (timestamp DESC, exchange, symbol);')
    # order_books
    cur.execute('''
        CREATE TABLE IF NOT EXISTS order_books (
            exchange TEXT,
            symbol TEXT,
            timestamp TEXT,
            bids JSON,
            asks JSON,
            bid_volume REAL,  -- Expanded summary
            ask_volume REAL,
            PRIMARY KEY (exchange, symbol, timestamp)
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_books_timestamp_exchange_symbol ON order_books (timestamp DESC, exchange, symbol);')
    # gas_prices
    cur.execute('''
        CREATE TABLE IF NOT EXISTS gas_prices (
            timestamp TEXT PRIMARY KEY,
            fast_gas REAL,
            average_gas REAL,
            slow_gas REAL,
            base_fee REAL,
            source TEXT,
            priority_fee REAL,  -- Expanded
            gas_limit INTEGER
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_gas_timestamp_source ON gas_prices (timestamp DESC, source);')
    # patterns
    cur.execute('''
        CREATE TABLE IF NOT EXISTS patterns (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            analysis_time TEXT,
            correlations JSON,
            granger_results JSON,
            var_forecast JSON,
            topics JSON,
            anomalies JSON,
            kendall_results JSON,  -- Expanded
            p_values JSON
        )
    ''')
    # backtests
    cur.execute('''
        CREATE TABLE IF NOT EXISTS backtests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_time TEXT,
            strategy TEXT,
            results JSON,
            parameters JSON,  -- Expanded
            performance_metrics JSON
        )
    ''')
    # economic_indicators (highly expanded)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS economic_indicators (
            series TEXT,
            date TEXT,
            value REAL,
            source TEXT,
            unit TEXT,  -- Expanded
            country TEXT,
            frequency TEXT,  -- e.g., daily/monthly
            PRIMARY KEY (series, date, source)
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_econ_date_series_source_country ON economic_indicators (date DESC, series, source, country);')
    conn.commit()
    return conn

# All ingest functions with max depth, validation, concurrency, data cleaning

def ingest_free_datasets(conn):
    def ingest_csv(path):
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, low_memory=False, on_bad_lines='skip', dtype=str)  # String dtype for safety
                df = df.dropna(subset=['text']) if 'text' in df.columns else df
                df = df.drop_duplicates(subset=['id'] if 'id' in df.columns else df.columns[0])
                if 'text' in df.columns:
                    df['text'] = df['text'].apply(lambda x: x.strip() if isinstance(x, str) else '')
                    df = df[df['text'].str.len() > 5]  # Filter short
                    sentiments = [compute_sentiment(text) for text in df['text']]
                    df['sentiment_label'], df['sentiment_score'] = zip(*sentiments)
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute("INSERT OR IGNORE INTO tweets (id, created_at, text, sentiment_label, sentiment_score, source) VALUES (?, ?, ?, ?, ?, ?)",
                                    (row.get('id', str(random.randint(1,1000000))), row.get('date', datetime.datetime.now().isoformat()), row.get('text', ''), row.get('sentiment_label', 'NEUTRAL'), row.get('sentiment_score', 0.5), 'kaggle'))
                    conn.commit()
                logging.info(f"Ingested and cleaned {len(df)} from {path}")
            except Exception as e:
                logging.error(f"Error ingesting/cleaning {path}: {e}")

    kaggle_paths = ["path/to/sentiment140.csv", "path/to/crypto_tweets.csv", "path/to/reddit_crypto.csv", "path/to/perpetuals-funding-rates.csv", "path/to/bitcoin-blockchain.csv"]  # Add yours
    with ThreadPoolExecutor(max_workers=4) as executor:  # Limited to avoid RAM
        futures = [executor.submit(ingest_csv, path) for path in kaggle_paths]
        for future in as_completed(futures):
            future.result()

    # BigQuery with error handling and chunking
    try:
        query = "SELECT id, created_utc, body, author, num_comments FROM `bigquery-public-data.reddit_posts.full` WHERE subreddit = 'cryptocurrency' LIMIT 50000"  # Expanded columns/limit
        df = read_gbq(query, project_id=GOOGLE_CLOUD_PROJECT, chunksize=10000)  # Chunked
        with db_lock:
            cur = conn.cursor()
            for chunk in df:
                chunk['created_at'] = pd.to_datetime(chunk['created_utc'], unit='s').dt.isoformat()
                chunk['text'] = chunk['body'].apply(lambda x: x.strip() if isinstance(x, str) else '')
                chunk = chunk[chunk['text'].str.len() > 10]
                sentiments = [compute_sentiment(text) for text in chunk['body']]
                chunk['sentiment_label'], chunk['sentiment_score'] = zip(*sentiments)
                for _, row in chunk.iterrows():
                    cur.execute("INSERT OR IGNORE INTO reddit_posts (id, subreddit, created_at, text, score, sentiment_label, sentiment_score, author, comments) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (row['id'], 'cryptocurrency', row['created_at'], row['body'], row.get('score', 0), row['sentiment_label'], row['sentiment_score'], row.get('author', 'anonymous'), row.get('num_comments', 0)))
                conn.commit()
        logging.info(f"Ingested and processed BigQuery Reddit in chunks")
    except Exception as e:
        logging.error(f"BigQuery ingest error: {e}")

    # StockTwits with expanded fields
    symbols = ['BTC', 'ETH', 'SOL', 'TSLA', 'AAPL', 'NVDA']  # Expanded
    for symbol in symbols:
        try:
            url = f"https://api.stocktwits.com/api/2/streams/symbol/{symbol}.json?access_token={STOCKTWITS_TOKEN}&limit=100"  # Max limit
            response = retry_func(requests.get, url).json()
            messages = response.get('messages', [])
            with db_lock:
                cur = conn.cursor()
                for msg in messages:
                    text = msg['body'].strip()
                    if len(text) < 5:
                        continue
                    sentiment_label, sentiment_score = compute_sentiment(text)
                    cur.execute("INSERT OR IGNORE INTO stocktwits (id, symbol, created_at, text, sentiment, likes, conversation_id, source) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (str(msg['id']), symbol, msg['created_at'], text, sentiment_label, msg.get('likes', {}).get('total', 0), str(msg.get('conversation', {}).get('id', 0)), 'stocktwits'))
                conn.commit()
            logging.info(f"Ingested {len(messages)} cleaned StockTwits for {symbol}")
            time.sleep(0.5)  # Polite rate
        except Exception as e:
            logging.error(f"StockTwits ingest error for {symbol}: {e}")

# Similar detailed, expanded functions for all ingests, with cleaning, validation, chunking, concurrency where appropriate

# main with expanded concurrency
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
    
    ingest_functions = [
        ingest_free_datasets, ingest_wallets, ingest_perps, ingest_order_books, ingest_gas_prices,
        ingest_alpha_vantage_economic, ingest_coingecko_crypto, ingest_fred_economic, ingest_newsapi,
        ingest_quandl, ingest_world_bank, ingest_yahoo_finance, ingest_cryptocompare, ingest_openexchangerates,
        ingest_investing_scrape, ingest_census_bureau, ingest_openstreetmap, ingest_sec_edgar, ingest_noaa_climate,
        ingest_github_repos, ingest_imf
    ]
    with ThreadPoolExecutor(max_workers=8) as executor:  # Increased for more ingests
        futures = [executor.submit(func, conn) for func in ingest_functions]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Ingest function error: {e}")
    
    fetch_tweets(client, conn, bot)
    analyze_patterns(conn)
    ensemble_prediction(conn)
    generate_dashboard(conn)
    time_series_plots(conn)
    export_for_finetuning(conn)
    backtest_strategies(conn)
    
    scheduler = BackgroundScheduler(max_workers=12, daemon=True)
    scheduler.add_job(lambda: fetch_tweets(client, conn, bot), 'cron', hour=1, jitter=60, misfire_grace_time=7200)  # Jitter for limits, longer grace
    # Add jobs for all ingests with jitter
    scheduler.start()
    
    updater.idle()
    conn.close()

if __name__ == "__main__":
    main()
