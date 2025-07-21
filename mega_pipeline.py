"""Mega pipeline exploring numerous integrations."""

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
from twelvedata import TDClient  # For Twelve Data; pip install twelvedata
import pydukascopy  # For Dukascopy; pip install pydukascopy
import barchart_ondemand  # For Barchart; pip install barchart-ondemand-client-python
from fmp_python.fmp import FMP  # For Financial Modeling Prep; pip install fmp-python
from openexchangerates import OpenExchangeRates  # For Open Exchange Rates; pip install openexchangerates
from config import get_config
from utils import compute_vibe

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler("system_log_detailed.txt", mode='a', encoding='utf-8'), logging.StreamHandler()])  # Detailed logging with append

# Config - values are loaded from the environment
APIFY_TOKEN = get_config("APIFY_TOKEN")
TELEGRAM_BOT_TOKEN = get_config("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_config("TELEGRAM_CHAT_ID")
ALPHA_VANTAGE_KEY = get_config("ALPHA_VANTAGE_KEY")
COINGLASS_KEY = get_config("COINGLASS_KEY")
ETHERSCAN_KEY = get_config("ETHERSCAN_KEY")
DUNE_API_KEY = get_config("DUNE_API_KEY")
DUNE_QUERY_ID = get_config("DUNE_QUERY_ID")
STOCKTWITS_TOKEN = get_config("STOCKTWITS_TOKEN")
GOOGLE_CLOUD_PROJECT = get_config("GOOGLE_CLOUD_PROJECT")
FINNHUB_KEY = get_config("FINNHUB_KEY")
POLYGON_KEY = get_config("POLYGON_KEY")
FRED_API_KEY = get_config("FRED_API_KEY")
NEWSAPI_KEY = get_config("NEWSAPI_KEY")
OPENEXCHANGE_KEY = get_config("OPENEXCHANGE_KEY")
GITHUB_TOKEN = get_config("GITHUB_TOKEN")
FMP_KEY = get_config("FMP_KEY")
EODHD_KEY = get_config("EODHD_KEY")
TWELVE_DATA_KEY = get_config("TWELVE_DATA_KEY")
BARCHART_KEY = get_config("BARCHART_KEY")
DB_FILE = "super_db.db"
ACTOR_ID = "kaitoeasyapi/twitter-x-data-tweet-scraper-pay-per-result-cheapest"
USERNAMES = ["onchainlens", "unipcs", "stalkchain", "elonmusk", "example2"]  # Include tracking accounts
SELECT_ACCOUNTS = []  # Select accounts for visual analysis, managed via Telegram
MAX_TWEETS_PER_USER = 10000
MAX_RETRIES = 10  # Increased
BASE_BACKOFF = 1
INCREMENTAL = False  # For max data, fetch historical
HISTORICAL_START = "2017-01-01"  # 8 years of data
HISTORICAL_MINUTE_START = "2022-01-01"
MINUTE_VALUABLE_SOURCES = ["eodhd", "twelve_data", "dukascopy", "barchart"]
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
FMP_SYMBOLS = ['AAPL', 'TSLA', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'V', 'JPM', 'UNH', 'MA', 'HD', 'PG', 'DIS', 'VZ', 'KO', 'PEP', 'WMT', 'CVX']
EODHD_TICKERS = ['AAPL.US', 'TSLA.US', 'MSFT.US', 'NVDA.US', 'GOOGL.US', 'AMZN.US', 'META.US', 'BRK-B.US', 'V.US', 'JPM.US', 'BTC-USD.CRYPTO', 'ETH-USD.CRYPTO', 'SOL-USD.CRYPTO', 'XRP-USD.CRYPTO', 'ADA-USD.CRYPTO']
TWELVE_DATA_SYMBOLS = ['AAPL', 'TSLA', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK.B', 'V', 'JPM', 'BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'ADA/USD', 'EUR/USD', 'USD/JPY', 'GBP/USD', 'AUD/USD', 'USD/CAD']
DUKASCOPY_PAIRS = ['EURUSD', 'USDJPY', 'GBPUSD', 'AUDUSD', 'USDCAD', 'NZDUSD', 'USDCHF', 'EURGBP', 'EURJPY', 'GBPJPY', 'XAUUSD', 'XAGUSD', 'WTI', 'BRENT', 'NGAS']
BARCHART_SYMBOLS = ['ZC*1', 'ZS*1', 'ZW*1', 'KE*1', 'HE*1', 'LE*1', 'GF*1', 'ES*1', 'NQ*1', 'YM*1', 'GC*1', 'SI*1', 'HG*1', 'CL*1', 'NG*1']

# ML Models with hyperparam tuning setup
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english", device=0 if os.name != 'posix' else -1)
lda_vectorizer = CountVectorizer(max_df=0.92, min_df=3, stop_words='english', ngram_range=(1,4), max_features=10000, analyzer='word')  # Advanced params
lda_model = LatentDirichletAllocation(n_components=50, random_state=42, n_jobs=-1, learning_method='online', batch_size=256, learning_decay=0.7, max_iter=20)  # Tuned for large datasets
fintwit_tokenizer = AutoTokenizer.from_pretrained("StephanAkkerman/FinTwitBERT", max_length=512, truncation=True, padding='max_length', return_tensors='pt')
fintwit_model = AutoModelForSequenceClassification.from_pretrained("StephanAkkerman/FinTwitBERT-sentiment", num_labels=3, ignore_mismatched_sizes=True)  # Handle mismatches

# Lock for DB concurrency
db_lock = threading.Lock()

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


# Advanced functions with max value: detailed fetching, cleaning, enrichment, parallel processing, error recovery

def ingest_financial_modeling_prep(conn):
    fmp = FMP(api_key=FMP_KEY)
    for symbol in FMP_SYMBOLS:
        try:
            historical = fmp.get_historical_price(symbol, start_date=HISTORICAL_START, end_date=datetime.datetime.now().strftime('%Y-%m-%d'))
            fundamentals = fmp.get_financial_statements(symbol, yearly=True)
            earnings = fmp.get_earnings_calendar(symbol)
            df_hist = pd.DataFrame(historical)
            df_fund = pd.DataFrame(fundamentals)
            df_earn = pd.DataFrame(earnings)
            # Clean and enrich
            df_hist['date'] = pd.to_datetime(df_hist['date']).dt.isoformat()
            df_hist = df_hist.dropna(subset=['close', 'volume']).sort_values('date').drop_duplicates('date')
            df_hist['adjusted_close'] = df_hist['close'] * (1 + df_hist['changePercent'] / 100)
            df_hist['market_cap'] = df_hist['close'] * df_hist.get('sharesOutstanding', 1)
            with db_lock:
                cur = conn.cursor()
                for _, row in df_hist.iterrows():
                    cur.execute("INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close, volume, type, adjusted_close, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (symbol, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], 'stock', row['adjusted_close'], row['market_cap']))
            df_fund['date'] = pd.to_datetime(df_fund['date']).dt.isoformat()
            df_fund = df_fund.dropna(subset=['revenue', 'netIncome']).sort_values('date')
            for _, row in df_fund.iterrows():
                cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (f"{symbol}_revenue", row['date'], row['revenue'], 'fmp_fundamentals', 'USD', 'US', 'quarterly'))
                cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (f"{symbol}_net_income", row['date'], row['netIncome'], 'fmp_fundamentals', 'USD', 'US', 'quarterly'))
            df_earn['date'] = pd.to_datetime(df_earn['date']).dt.isoformat()
            df_earn = df_earn.dropna(subset=['epsEstimated', 'eps']).sort_values('date')
            for _, row in df_earn.iterrows():
                cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency) VALUES (?, ?, ?, ?, ?, ?, ?)",
                            (f"{symbol}_eps_surprise", row['date'], row['eps'] - row['epsEstimated'], 'fmp_earnings', 'USD', 'US', 'quarterly'))
            conn.commit()
            logging.info(f"Ingested, cleaned, enriched {len(df_hist)} hist, {len(df_fund)} fund, {len(df_earn)} earnings for FMP {symbol}")
        except Exception as e:
            logging.error(f"FMP ingest error for {symbol}: {e}")
        time.sleep(1)

def ingest_eod_historical(conn):
    eod = eodhd.EODHD(EODHD_KEY)
    minute_enabled = "eodhd" in MINUTE_VALUABLE_SOURCES
    for ticker in EODHD_TICKERS:
        try:
            historical = eod.get_historical_data(
                ticker,
                start=HISTORICAL_START,
                end=datetime.datetime.now().strftime('%Y-%m-%d'),
            )
            fundamentals = eod.get_fundamentals(ticker)
            if minute_enabled:
                intraday = fetch_with_fallback(
                    eod.get_intraday_data,
                    ticker=ticker,
                    count=5000,
                    start_date=HISTORICAL_MINUTE_START,
                )
            else:
                intraday = []  # fallback to daily only
            df_hist = pd.DataFrame(historical)
            df_fund = pd.DataFrame([fundamentals]) if isinstance(fundamentals, dict) else pd.DataFrame(fundamentals)
            df_intra = pd.DataFrame(intraday)
            df_hist['date'] = pd.to_datetime(df_hist['date']).dt.isoformat()
            df_hist = df_hist.dropna(subset=['close', 'volume']).sort_values('date').drop_duplicates('date')
            df_hist['adjusted_close'] = df_hist['close'] * df_hist.get('adjustment_factor', 1)
            df_hist['market_cap'] = df_hist['close'] * df_hist.get('shares_outstanding', 1)
            with db_lock:
                cur = conn.cursor()
                for _, row in df_hist.iterrows():
                    cur.execute("INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close, volume, type, adjusted_close, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (ticker, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], 'mixed', row['adjusted_close'], row['market_cap']))
            for key, value in fundamentals.items():
                if isinstance(value, (int, float)):
                    cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                (f"{ticker}_{key}", datetime.datetime.now().isoformat(), value, 'eod_fundamentals', 'various', 'global', 'current'))
            df_intra['date'] = pd.to_datetime(df_intra['timestamp']).dt.isoformat()
            df_intra = df_intra.dropna(subset=['close']).sort_values('date')
            df_intra['volatility'] = df_intra['close'].rolling(window=60).std()
            df_intra['momentum'] = df_intra['close'].diff()
            for _, row in df_intra.iterrows():
                cur.execute("INSERT OR REPLACE INTO prices (ticker, date, close, volume, type) VALUES (?, ?, ?, ?, ?)",
                            (ticker, row['date'], row['close'], row['volume'], 'intraday'))
                cur.execute("INSERT OR REPLACE INTO high_res_prices (ticker, date, close, volume, volatility, momentum) VALUES (?, ?, ?, ?, ?, ?)",
                            (ticker, row['date'], row['close'], row['volume'], row['volatility'], row['momentum']))
            conn.commit()
            logging.info(f"Ingested, enriched {len(df_hist)} daily, {len(df_fund.columns)} fund, {len(df_intra)} intraday for EOD {ticker}")
        except Exception as e:
            logging.error(f"EOD ingest error for {ticker}: {e}")
        time.sleep(0.5)

def ingest_twelve_data(conn):
    td = TDClient(apikey=TWELVE_DATA_KEY)
    minute_enabled = "twelve_data" in MINUTE_VALUABLE_SOURCES
    for symbol in TWELVE_DATA_SYMBOLS:
        try:
            if minute_enabled:
                time_series = fetch_with_fallback(
                    td.time_series,
                    symbol=symbol,
                    start_date=HISTORICAL_MINUTE_START,
                    end_date=datetime.datetime.now().strftime('%Y-%m-%d'),
                    outputsize=5000,
                )
            else:
                time_series = fetch_with_fallback(
                    td.time_series,
                    symbol=symbol,
                    interval="1h",
                    start_date=HISTORICAL_START,
                    end_date=datetime.datetime.now().strftime('%Y-%m-%d'),
                    outputsize=5000,
                )
            time_series = time_series.as_pandas()
            fundamentals = td.fundamentals(symbol=symbol).as_pandas()
            quote = td.quote(symbol=symbol).as_dict()
            df_ts = time_series.reset_index()
            df_fund = fundamentals.reset_index() if not fundamentals.empty else pd.DataFrame()
            df_ts['date'] = pd.to_datetime(df_ts['datetime']).dt.isoformat()
            df_ts = df_ts.dropna(subset=['close', 'volume']).sort_values('date').drop_duplicates('date')
            df_ts['adjusted_close'] = df_ts['close']
            df_ts['market_cap'] = df_ts['close'] * quote.get('fifty_two_week_high', 1)
            df_ts['volatility'] = df_ts['close'].rolling(window=60).std()
            df_ts['momentum'] = df_ts['close'].diff()
            with db_lock:
                cur = conn.cursor()
                for _, row in df_ts.iterrows():
                    cur.execute("INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close, volume, type, adjusted_close, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (symbol, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], 'mixed', row['close'], row['market_cap']))
                    cur.execute("INSERT OR REPLACE INTO high_res_prices (ticker, date, close, volume, volatility, momentum) VALUES (?, ?, ?, ?, ?, ?)",
                                (symbol, row['date'], row['close'], row['volume'], row['volatility'], row['momentum']))
            if not df_fund.empty:
                df_fund['date'] = pd.to_datetime(df_fund['reportDate']).dt.isoformat() if 'reportDate' in df_fund.columns else datetime.datetime.now().isoformat()
                for _, row in df_fund.iterrows():
                    for key in ['eps', 'revenue', 'netIncome', 'ebitda']:
                        if key in row:
                            cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                        (f"{symbol}_{key}", row['date'], row[key], 'twelve_fundamentals', 'USD', 'global', 'quarterly'))
            conn.commit()
            logging.info(f"Ingested, enriched {len(df_ts)} time series, {len(df_fund)} fundamentals for Twelve Data {symbol}")
        except Exception as e:
            logging.error(f"Twelve Data ingest error for {symbol}: {e}")
        time.sleep(1.5)

def ingest_dukascopy(conn):
    minute_enabled = "dukascopy" in MINUTE_VALUABLE_SOURCES
    for pair in DUKASCOPY_PAIRS:
        try:
            if minute_enabled:
                try:
                    df = pydukascopy.get_historical_data(
                        pair,
                        from_date=HISTORICAL_MINUTE_START,
                        to_date=datetime.datetime.now().strftime('%Y-%m-%d'),
                        timeframe='M1',
                    )
                except Exception:
                    df = pydukascopy.get_historical_data(
                        pair,
                        from_date=HISTORICAL_MINUTE_START,
                        to_date=datetime.datetime.now().strftime('%Y-%m-%d'),
                        timeframe='M5',
                    )
            else:
                df = pydukascopy.get_historical_data(
                    pair,
                    from_date=HISTORICAL_START,
                    to_date=datetime.datetime.now().strftime('%Y-%m-%d'),
                    timeframe='H1',
                )
            df = df.dropna(subset=['Close', 'Volume']).sort_values('Timestamp').drop_duplicates('Timestamp')
            df['date'] = pd.to_datetime(df['Timestamp']).dt.isoformat()
            df['adjusted_close'] = df['Close']
            df['market_cap'] = np.nan
            df['volatility'] = df['Close'].rolling(window=60).std()
            df['momentum'] = df['Close'].diff()
            with db_lock:
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute("INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close, volume, type, adjusted_close, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (pair, row['date'], row['Open'], row['High'], row['Low'], row['Close'], row['Volume'], 'forex', row['adjusted_close'], row['market_cap']))
                    cur.execute("INSERT OR REPLACE INTO high_res_prices (ticker, date, close, volume, volatility, momentum) VALUES (?, ?, ?, ?, ?, ?)",
                                (pair, row['date'], row['Close'], row['Volume'], row['volatility'], row['momentum']))
            conn.commit()
            logging.info(f"Ingested, enriched {len(df)} for Dukascopy {pair}")
        except Exception as e:
            logging.error(f"Dukascopy ingest error for {pair}: {e}")
        time.sleep(3)

def ingest_barchart(conn):
    client = barchart_ondemand.BarchartOnDemandClient(
        api_key=BARCHART_KEY, endpoint='https://ondemand.websol.barchart.com'
    )
    minute_enabled = "barchart" in MINUTE_VALUABLE_SOURCES
    for symbol in BARCHART_SYMBOLS:
        try:
            if minute_enabled:
                try:
                    historical = client.get_history(
                        symbol,
                        type='intraday',
                        start=HISTORICAL_MINUTE_START,
                        maxRecords=10000,
                        order='asc',
                        interval=1,
                    )
                except Exception:
                    historical = client.get_history(
                        symbol,
                        type='intraday',
                        start=HISTORICAL_MINUTE_START,
                        maxRecords=10000,
                        order='asc',
                        interval=5,
                    )
            else:
                historical = client.get_history(
                    symbol,
                    type='intraday',
                    start=HISTORICAL_START,
                    maxRecords=10000,
                    order='asc',
                    interval=60,
                )
            df = pd.DataFrame(historical['results'])
            df = df.dropna(subset=['close', 'volume']).sort_values('timestamp').drop_duplicates('timestamp')
            df['date'] = pd.to_datetime(df['timestamp']).dt.isoformat()
            df['adjusted_close'] = df['close']
            df['market_cap'] = np.nan
            df['volatility'] = df['close'].rolling(window=60).std()
            df['momentum'] = df['close'].diff()
            with db_lock:
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute("INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close, volume, type, adjusted_close, market_cap) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (symbol, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], 'futures', row['adjusted_close'], row['market_cap']))
                    cur.execute("INSERT OR REPLACE INTO high_res_prices (ticker, date, close, volume, volatility, momentum) VALUES (?, ?, ?, ?, ?, ?)",
                                (symbol, row['date'], row['close'], row['volume'], row['volatility'], row['momentum']))
            conn.commit()
            logging.info(f"Ingested, enriched {len(df)} for Barchart {symbol}")
        except Exception as e:
            logging.error(f"Barchart ingest error for {symbol}: {e}")
        time.sleep(2)
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
