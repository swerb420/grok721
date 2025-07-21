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
from edgar import Company, Filing  # For SEC EDGAR; pip install python-edgar
from noaa_sdk import NOAA  # For NOAA; pip install noaa-sdk
from github import Github  # For GitHub; pip install PyGithub
import eodhd  # For EOD; pip install eodhd
import twelve_data  # For Twelve Data; pip install twelve-data
import dukascopy  # Custom or pydukascopy; assume pip install pydukascopy
import barchart  # For Barchart; pip install barchart-ondemand-client
from fmp_python.fmp import FMP  # For Financial Modeling Prep; pip install fmp-python
from openexchangerates import OpenExchangeRates  # For Open Exchange Rates; pip install openexchangerates

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler("system_log_detailed.txt", mode='a', encoding='utf-8'), logging.StreamHandler()])  # Detailed logging with append

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
FINNHUB_KEY = "YOUR_FINNHUB_KEY_HERE"  # Free at finnhub.io
POLYGON_KEY = "YOUR_POLYGON_KEY_HERE"  # Free at polygon.io
FRED_API_KEY = "YOUR_FRED_API_KEY_HERE"  # From api.stlouisfed.org
NEWSAPI_KEY = "YOUR_NEWSAPI_KEY_HERE"  # From newsapi.org
OPENEXCHANGE_KEY = "YOUR_OPENEXCHANGE_KEY_HERE"  # Free at openexchangerates.org
GITHUB_TOKEN = "YOUR_GITHUB_TOKEN_HERE"  # Optional for higher limits
FMP_KEY = "YOUR_FMP_KEY_HERE"  # Free at financialmodelingprep.com
eodhd_KEY = "YOUR_EODHD_KEY_HERE"  # Free at eodhistoricaldata.com
TWELVE_DATA_KEY = "YOUR_TWELVE_DATA_KEY_HERE"  # Free at twelvedata.com
BARCHART_KEY = "YOUR_BARCHART_KEY_HERE"  # Free at barchart.com/ondemand
DB_FILE = "super_db.db"
ACTOR_ID = "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
USERNAMES = ["onchainlens", "unipcs", "stalkchain", "elonmusk", "example2"]  # Include tracking accounts
SELECT_ACCOUNTS = []  # Select accounts for visual analysis, managed via Telegram
MAX_TWEETS_PER_USER = 10000
MAX_RETRIES = 10  # Increased
BASE_BACKOFF = 1
INCREMENTAL = False  # For max data, fetch historical
HISTORICAL_START = "2015-01-01"  # Deeper
CREDIT_THRESHOLD = 0.8
MONTHLY_CREDITS = 49.0
WALLETS = ['0xexample_whale1', '0xexample_whale2']  # Add tracked wallets, dynamically expanded
PERP_SYMBOLS = ['BTC', 'ETH', 'SOL', 'XRP']  # Expanded
EXCHANGES = ['binance', 'bybit', 'okx', 'uniswap_v3', 'sushiswap', 'coinbase', 'kraken', 'gemini']  # Expanded
TRACKING_ACCOUNTS = ['onchainlens', 'unipcs', 'stalkchain']  # Accounts to extract wallets from tweets
ALPHA_ECON_SERIES = ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'INDPRO', 'PCE', 'RSXFS', 'CSUSHPISA', 'MORTGAGE30US', 'DSPIC96', 'PCEC96', 'PSAVERT']  # Expanded
FINNHUB_SYMBOLS = ['AAPL', 'TSLA', 'BTC-USD', 'ETH-USD', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'V', 'JPM', 'UNH', 'MA', 'HD']  # Expanded
POLYGON_TICKERS = ['AAPL', 'TSLA', 'X:BTCUSD', 'X:ETHUSD', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'V', 'JPM', 'UNH', 'MA', 'HD']  # Expanded
FRED_SERIES = ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'INDPRO', 'PCE', 'RSXFS', 'WALCL', 'M2SL', 'BOGMBASE', 'DGS10', 'T10YIE', 'DSPI', 'PCEPI', 'CPILFESL']  # Expanded
COINGECKO_CRYPTOS = ['bitcoin', 'ethereum', 'solana', 'ripple', 'cardano', 'dogecoin', 'polkadot', 'chainlink', 'uniswap', 'litecoin', 'stellar', 'avalanche-2', 'binancecoin', 'terra-luna', 'cosmos', 'aave', 'maker', 'compound-governance-token', 'yearn-finance', 'synthetix-network-token']  # Expanded
YFINANCE_TICKERS = ['^GSPC', '^IXIC', '^DJI', 'GC=F', 'CL=F', '^TNX', 'EURUSD=X', 'JPY=X', 'GBPUSD=X', 'BTC-USD', 'ETH-USD', '^VIX', '^RUT', 'SI=F', 'HG=F', 'NG=F', 'ZC=F', 'ZS=F', 'ZW=F']  # Expanded Yahoo
CRYPTOCOMPARE_COINS = ['BTC', 'ETH', 'SOL', 'XRP', 'ADA', 'DOGE', 'DOT', 'LINK', 'UNI', 'LTC', 'XLM', 'AVAX', 'BNB', 'LUNA', 'ATOM', 'AAVE', 'MKR', 'COMP', 'YFI', 'SNX']  # Expanded
OPENEXCHANGE_CURRENCIES = ['USD', 'EUR', 'JPY', 'GBP', 'CNY', 'AUD', 'CAD', 'CHF', 'SEK', 'NZD', 'KRW', 'INR', 'BRL', 'RUB', 'ZAR', 'MXN', 'SGD', 'HKD', 'NOK', 'TRY']  # Expanded forex
GITHUB_REPOS = ['bitcoin/bitcoin', 'ethereum/go-ethereum', 'solana-labs/solana', 'ripple/rippled', 'cardano-foundation/cardano-wallet', 'ChainSafe/lodestar', 'polkadot-js/api', 'Chainlink/contracts', 'Uniswap/v3-core', 'litecoin-project/litecoin']  # Expanded crypto projects
NOAA_LOCATIONS = [('NYC', 40.7128, -74.0060, 50), ('LON', 51.5074, -0.1278, 50), ('TOK', 35.6762, 139.6503, 50), ('BER', 52.5200, 13.4050, 50), ('SHA', 31.2304, 121.4737, 50)]  # Expanded locations, radius km
SEC_EDGAR_START = "2010-01-01"  # Deeper
SEC_COMPANIES = ['Tesla Inc', 'MicroStrategy Inc', 'Coinbase Global Inc', 'Riot Blockchain Inc', 'Marathon Digital Holdings Inc', 'Galaxy Digital Holdings Ltd', 'Block Inc', 'PayPal Holdings Inc', 'Visa Inc', 'Mastercard Inc']  # Expanded crypto-related
INVESTING_COMMODITIES = ['gold', 'crude-oil', 'natural-gas', 'silver', 'copper', 'corn', 'soybeans', 'wheat', 'coffee', 'sugar']  # For Investing scrape
CENSUS_SERIES = ['RETAIL', 'HOUSING', 'POP']  # Census endpoints
OSM_QUERIES = ['crypto conference', 'blockchain summit', 'bitcoin meetup']  # For OSM

# ML Models with hyperparam tuning setup
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english", device=0 if os.name != 'posix' else -1)
lda_vectorizer = CountVectorizer(max_df=0.92, min_df=3, stop_words='english', ngram_range=(1,4), max_features=10000, analyzer='word')  # Advanced params
lda_model = LatentDirichletAllocation(n_components=50, random_state=42, n_jobs=-1, learning_method='online', batch_size=256, learning_decay=0.7, max_iter=20)  # Tuned for large datasets
fintwit_tokenizer = AutoTokenizer.from_pretrained("StephanAkkerman/FinTwitBERT", max_length=512, truncation=True, padding='max_length', return_tensors='pt')
fintwit_model = AutoModelForSequenceClassification.from_pretrained("StephanAkkerman/FinTwitBERT-sentiment", num_labels=3, ignore_mismatched_sizes=True)  # Handle mismatches

# Lock for DB concurrency
db_lock = threading.Lock()

def compute_vibe(sentiment_label, sentiment_score, likes, retweets, replies):
    likes, retweets, replies = map(lambda x: x if x is not None and x >= 0 else 0, [likes, retweets, replies])
    engagement = (likes + retweets * 2.5 + replies * 1.5) / 1000.0  # Weighted engagement
    base_score = sentiment_score * 1.2 if sentiment_label == "POSITIVE" else -sentiment_score * 1.1  # Asymmetric weighting
    vibe_score = (base_score + engagement) * 4.5 + np.random.normal(0, 0.05)  # Slight noise for robustness
    vibe_score = min(max(vibe_score, 0), 10)
    thresholds = [7.5, 5.5, 3.5]  # Adjusted thresholds
    labels = ["Hype/Positive Impact", "Engaging/Neutral", "Controversial/Mixed", "Negative/Low Engagement"]
    vibe_label = next((l for t, l in zip(thresholds, labels) if vibe_score > t), labels[-1])
    return vibe_score, vibe_label

def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=120, isolation_level=None)  # Auto-commit, longer timeout
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL;')
    cur.execute('PRAGMA synchronous = NORMAL;')
    cur.execute('PRAGMA cache_size = -512000;')  # 512MB cache for massive queries
    cur.execute('PRAGMA busy_timeout = 300000;')  # 5 min for heavy loads
    cur.execute('PRAGMA foreign_keys = ON;')
    cur.execute('PRAGMA temp_store = MEMORY;')
    cur.execute('PRAGMA mmap_size = 268435456;')  # 256MB mmap for faster reads
    cur.execute('PRAGMA optimize;')  # Optimize on close
    # Tweets table with max expansion
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
            entities JSON,
            geo JSON,
            language TEXT,
            in_reply_to_id TEXT,
            quoted_id TEXT
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tweets_multi ON tweets (created_at DESC, username, source, sentiment_label);')  # Compound index
    # All other tables with similar max expansion and indices
    conn.commit()
    return conn

# ingest functions with max advanced: chunking, cleaning, validation, concurrency, data enrichment

# Example for one; apply to all
def ingest_alpha_vantage_economic(conn):
    def fetch_series(s):
        url = f"https://www.alphavantage.co/query?function={s}&apikey={ALPHA_VANTAGE_KEY}&datatype=json&interval=monthly"  # Monthly for depth
        try:
            response = retry_func(requests.get, url, timeout=30).json()
            data = response.get('data', []) or response.get('Time Series (Monthly)', {})
            if isinstance(data, dict):
                data = [{'date': k, 'value': v} for k, v in data.items()]
            df = pd.DataFrame(data)
            df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)
            df['date'] = pd.to_datetime(df['date'], errors='coerce').dt.isoformat()
            df = df.dropna(subset=['date'])
            df = df.sort_values('date').drop_duplicates('date')
            with db_lock:
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source, frequency) VALUES (?, ?, ?, ?, ?)",
                                (s, row['date'], row['value'], 'alpha_vantage', 'monthly'))
                conn.commit()
            logging.info(f"Ingested {len(df)} cleaned/validated points for Alpha series {s}")
        except Exception as e:
            logging.error(f"Alpha ingest error for {s}: {e}")

    with ThreadPoolExecutor(max_workers=3) as executor:  # Limited to respect 5/min limit
        futures = [executor.submit(fetch_series, s) for s in ALPHA_ECON_SERIES]
        for future in as_completed(futures):
            future.result()
        time.sleep(60)  # Minute buffer for rate

# Similar for all other ingests, with specific params, cleaning, etc.

# main with max concurrency, error recovery
def main():
    client = ApifyClient(APIFY_TOKEN, max_retries=5, timeout_secs=300)
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
        ingest_github_repos, ingest_imf, ingest_financial_modeling_prep, ingest_eod_historical, ingest_twelve_data,
        ingest_dukascopy, ingest_barchart
    ]
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(func, conn) for func in ingest_functions]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Ingest function error, continuing: {e}")

    fetch_tweets(client, conn, bot)
    analyze_patterns(conn)
    ensemble_prediction(conn)
    generate_dashboard(conn)
    time_series_plots(conn)
    export_for_finetuning(conn)
    backtest_strategies(conn)
    
    scheduler = BackgroundScheduler(max_workers=15, daemon=True)
    scheduler.add_job(lambda: fetch_tweets(client, conn, bot), 'cron', hour=1, jitter=120, misfire_grace_time=10800)  # Increased jitter/grace
    # Add jobs for all ingests
    scheduler.start()
    
    updater.idle()
    conn.execute('PRAGMA optimize;')  # Optimize on exit
    conn.close()

if __name__ == "__main__":
    main()
