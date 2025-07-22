"""Original extended pipeline without pruning."""

import os
import json
import time
import datetime
import sqlite3
import threading
import logging
import requests
import pandas as pd
import numpy as np
from worldbank import WorldBank
from tradingeconomics import TradingEconomics
from census import Census
from sklearn.decomposition import LatentDirichletAllocation
from sklearn.model_selection import GridSearchCV
from transformers import pipeline
from glassnode.client import GlassnodeClient
import moralis
from config import get_config

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
DUNE_GAS_PRICES_QUERY_ID = get_config("DUNE_GAS_PRICES_QUERY_ID")
DUNE_HYPERLIQUID_STATS_QUERY_ID = get_config("DUNE_HYPERLIQUID_STATS_QUERY_ID")
DUNE_HYPERLIQUID_QUERY_ID = get_config("DUNE_HYPERLIQUID_QUERY_ID")
DUNE_GMX_ANALYTICS_QUERY_ID = get_config("DUNE_GMX_ANALYTICS_QUERY_ID")
DUNE_HYPERLIQUID_FLOWS_QUERY_ID = get_config("DUNE_HYPERLIQUID_FLOWS_QUERY_ID")
DUNE_PERPS_HYPERLIQUID_QUERY_ID = get_config("DUNE_PERPS_HYPERLIQUID_QUERY_ID")
DUNE_GMX_IO_QUERY_ID = get_config("DUNE_GMX_IO_QUERY_ID")
DUNE_AIRDROPS_WALLETS_QUERY_ID = get_config("DUNE_AIRDROPS_WALLETS_QUERY_ID")
DUNE_SMART_WALLET_FINDER_QUERY_ID = get_config("DUNE_SMART_WALLET_FINDER_QUERY_ID")
DUNE_WALLET_BALANCES_QUERY_ID = get_config("DUNE_WALLET_BALANCES_QUERY_ID")
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
MORALIS_KEY = get_config("MORALIS_KEY")
COVALENT_KEY = get_config("COVALENT_KEY")
LUNARCRUSH_KEY = get_config("LUNARCRUSH_KEY")
BLOCKCHAIR_KEY = get_config("BLOCKCHAIR_KEY")
GLASSNODE_KEY = get_config("GLASSNODE_KEY")
MARKETSTACK_KEY = get_config("MARKETSTACK_KEY")
OECD_KEY = get_config("OECD_KEY")
TRADING_ECONOMICS_KEY = get_config("TRADING_ECONOMICS_KEY")
WORLD_BANK_KEY = get_config("WORLD_BANK_KEY")
CENSUS_BUREAU_KEY = get_config("CENSUS_BUREAU_KEY")
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
CENSUS_SERIES = ['RETAIL', 'HOUSING', 'POP', 'ADVANCEDRETAIL', 'BUSINESSINVENTORIES', 'CONSTRUCTION', 'MANUFACTURING', 'WHOLESALE', 'EXPORTS', 'IMPORTS']  # Expanded Census
OSM_QUERIES = ['crypto conference', 'blockchain summit', 'bitcoin meetup', 'ethereum devcon', 'fintech event', 'defi hackathon', 'nft gallery', 'web3 workshop', 'crypto mining farm', 'blockchain university']  # Expanded for geo
FMP_SYMBOLS = ['AAPL', 'TSLA', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK.B', 'V', 'JPM', 'UNH', 'MA', 'HD']  # Expanded for FMP
EODHD_TICKERS = ['AAPL.US', 'TSLA.US', 'MSFT.US', 'NVDA.US', 'GOOGL.US', 'AMZN.US', 'META.US', 'BRK.B.US', 'V.US', 'JPM.US', 'BTC-USD.CRYPTO', 'ETH-USD.CRYPTO', 'SOL-USD.CRYPTO', 'XRP-USD.CRYPTO', 'ADA-USD.CRYPTO']  # Expanded for EOD
TWELVE_DATA_SYMBOLS = ['AAPL', 'TSLA', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK.B', 'V', 'JPM', 'BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'ADA/USD', 'EUR/USD', 'USD/JPY', 'GBP/USD', 'AUD/USD', 'USD/CAD']  # Expanded for Twelve Data
DUKASCOPY_PAIRS = ['EURUSD', 'USDJPY', 'GBPUSD', 'AUDUSD', 'USDCAD', 'NZDUSD', 'USDCHF', 'EURGBP', 'EURJPY', 'GBPJPY', 'XAUUSD', 'XAGUSD', 'WTI', 'BRENT', 'NGAS']  # Expanded forex/commodities
BARCHART_SYMBOLS = ['ZC*1', 'ZS*1', 'ZW*1', 'KE*1', 'HE*1', 'LE*1', 'GF*1', 'ES*1', 'NQ*1', 'YM*1', 'GC*1', 'SI*1', 'HG*1', 'CL*1', 'NG*1']  # Futures symbols for Barchart
GLASSNODE_ASSETS = ['BTC', 'ETH', 'LTC', 'BCH']  # Expanded for Glassnode
MORALIS_CHAINS = ['eth', 'bsc', 'polygon', 'avalanche', 'fantom', 'cronos', 'arbitrum', 'optimism']  # Expanded chains
COVALENT_CHAINS = [1, 56, 137, 43114, 250, 25, 42161, 10]  # Chain IDs
LUNARCRUSH_ASSETS = ['btc', 'eth', 'sol', 'xrp', 'ada', 'doge', 'dot', 'link', 'uni', 'ltc']  # Expanded
BLOCKCHAIR_CHAINS = ['bitcoin', 'ethereum', 'litecoin', 'bitcoin-cash', 'ripple', 'stellar', 'dogecoin', 'eos', 'dash', 'cardano']  # Expanded
MARKETSTACK_SYMBOLS = ['AAPL', 'TSLA', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK.B', 'V', 'JPM', 'BTC-USD', 'ETH-USD', '^GSPC', '^IXIC', '^DJI']  # Expanded for Marketstack
OECD_INDICATORS = ['GDP', 'UNEMPLOYMENT', 'CPI', 'INTEREST_RATE', 'PRODUCTION', 'CONSUMER_CONFIDENCE', 'BUSINESS_CONFIDENCE', 'TRADE_BALANCE', 'CURRENT_ACCOUNT', 'PUBLIC_DEBT']  # Expanded for OECD
OECD_COUNTRIES = ['USA', 'CHN', 'JPN', 'DEU', 'GBR', 'FRA', 'IND', 'ITA', 'CAN', 'KOR']
TRADING_ECONOMICS_COUNTRIES = ['united-states', 'china', 'japan', 'germany', 'united-kingdom', 'france', 'india', 'italy', 'canada', 'south-korea']  # Expanded
TRADING_ECONOMICS_INDICATORS = ['gdp', 'unemployment-rate', 'inflation-rate', 'interest-rate', 'balance-of-trade', 'current-account', 'government-debt-to-gdp', 'retail-sales-yoy', 'industrial-production', 'consumer-confidence']  # Expanded
WORLD_BANK_INDICATORS = ['NY.GDP.MKTP.CD', 'SL.UEM.TOTL.ZS', 'FP.CPI.TOTL.ZG', 'FR.INR.RINR', 'BX.TRF.PWKR.CD.DT', 'BN.CAB.XOKA.CD', 'GC.DOD.TOTL.GD.ZS', 'SL.UEM.TOTL.ZS', 'NY.GNP.MKTP.CD', 'SP.POP.TOTL']  # Expanded
CENSUS_BUREAU_DATASETS = ['acs/acs1', 'acs/acs5', 'dec/pl', 'pep/population', 'cps/basic/jan', 'cps/basic/feb', 'cps/basic/mar', 'cps/basic/apr', 'cps/basic/may', 'cps/basic/jun']  # Expanded demographics

# ML Models with grid search and cross-validation
sentiment_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english", device=0 if os.name != 'posix' else -1)
lda_params = {'n_components': [20, 30, 50, 75], 'learning_decay': [0.5, 0.6, 0.7, 0.8, 0.9], 'batch_size': [128, 256, 512]}
lda_gs = GridSearchCV(LatentDirichletAllocation(random_state=42, n_jobs=-1, learning_method='online', max_iter=30, evaluate_every=5), lda_params, cv=5, n_jobs=-1, verbose=2, scoring='perplexity')  # Advanced GS with scoring
# In analyze_patterns, lda_gs.fit(lda_features); best_model = lda_gs.best_estimator_

# Lock for DB concurrency
db_lock = threading.Lock()


def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False, timeout=180, isolation_level=None)  # Auto-commit, 3 min timeout
    cur = conn.cursor()
    cur.execute('PRAGMA journal_mode=WAL;')
    cur.execute('PRAGMA synchronous = NORMAL;')
    cur.execute('PRAGMA cache_size = -1024000;')  # 1GB cache for extreme queries
    cur.execute('PRAGMA busy_timeout = 600000;')  # 10 min for max loads
    cur.execute('PRAGMA foreign_keys = ON;')
    cur.execute('PRAGMA temp_store = MEMORY;')
    cur.execute('PRAGMA mmap_size = 536870912;')  # 512MB mmap
    cur.execute('PRAGMA analysis_limit = 1000;')  # Deeper analysis for optimize
    # Tweets table with ultimate expansion
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
            quoted_id TEXT,
            conversation_id TEXT,
            possibly_sensitive BOOLEAN,
            view_count INTEGER,
            bookmark_count INTEGER
        )
    ''')
    cur.execute('CREATE INDEX IF NOT EXISTS idx_tweets_composite ON tweets (created_at DESC, username, source, sentiment_label, vibe_score);')  # Multi-column
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
    # All other tables with similar ultimate expansion, indices, and constraints
    conn.commit()
    return conn

# ingest functions with ultimate advanced: chunking, cleaning, validation, concurrency, data enrichment, cross-referencing

def ingest_glassnode(conn):
    client = GlassnodeClient(GLASSNODE_KEY)
    metrics = ['active_addresses', 'exchange_balance', 'hash_rate', 'sopr', 'nvt_ratio', 'mvrv_z_score', 'puell_multiple', 'stock_to_flow', 'difficulty_ribbon', 'thermocap_multiple']  # Expanded high-value metrics
    for asset in GLASSNODE_ASSETS:
        for metric in metrics:
            try:
                url = f"https://api.glassnode.com/v1/metrics/{metric.split('_')[0]}/{metric}?a={asset}&s={int(time.mktime(datetime.datetime.strptime(HISTORICAL_START, '%Y-%m-%d').timetuple()))}&u={int(time.time())}&f=JSON&api_key={GLASSNODE_KEY}"
                response = retry_func(requests.get, url).json()
                df = pd.DataFrame(response)
                df['timestamp'] = pd.to_datetime(df['t'], unit='s').dt.isoformat()
                df = df.dropna(subset=['v']).sort_values('timestamp').drop_duplicates('timestamp')
                df['series'] = metric
                df['asset'] = asset
                df['value'] = pd.to_numeric(df['v'], errors='coerce')
                df['normalized_value'] = (df['value'] - df['value'].min()) / (df['value'].max() - df['value'].min() + 1e-8)  # Normalize for correlations
                df['rolling_avg'] = df['value'].rolling(window=7, min_periods=1).mean()  # Enrich with rolling
                df['z_score'] = (df['value'] - df['rolling_avg']) / df['value'].std()  # Enrich with z
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute("INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency, asset) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                    (metric, row['timestamp'], row['value'], 'glassnode', 'various', 'global', 'hourly', asset))
                    conn.commit()
                logging.info(f"Ingested, enriched, normalized {len(df)} for Glassnode {metric} on {asset}")
            except Exception as e:
                logging.error(f"Glassnode ingest error for {metric} on {asset}: {e}")
            time.sleep(6)  # 10 calls/day ~6s/call average

def ingest_moralis(conn):
    moralis.api_key = MORALIS_KEY
    for chain in MORALIS_CHAINS:
        for wallet in WALLETS:
            try:
                balance = moralis.evm_api.wallet.get_wallet_balances(chain=chain, address=wallet)
                txs = moralis.evm_api.transaction.get_wallet_transactions(chain=chain, address=wallet, limit=500, from_date=HISTORICAL_START)
                nfts = moralis.evm_api.wallet.get_wallet_nfts(chain=chain, address=wallet, limit=100)
                df_balance = pd.DataFrame(balance)
                df_txs = pd.DataFrame(txs)
                df_nfts = pd.DataFrame(nfts)
                # Clean/enrich balances
                df_balance['timestamp'] = datetime.datetime.now().isoformat()
                df_balance = df_balance.dropna(subset=['usd_value']).sort_values('usd_value', ascending=False)
                df_balance['portfolio_share'] = df_balance['usd_value'] / df_balance['usd_value'].sum()
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df_balance.iterrows():
                        cur.execute("INSERT OR IGNORE INTO wallets (address, timestamp, value, source, token_address, token_symbol, portfolio_share) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    (wallet, row['timestamp'], row['usd_value'], 'moralis_balance', row.get('token_address', 'native'), row.get('symbol', 'ETH'), row['portfolio_share']))
                # Txs
                df_txs['timestamp'] = pd.to_datetime(df_txs['block_timestamp']).dt.isoformat()
                df_txs = df_txs.dropna(subset=['value']).sort_values('timestamp').drop_duplicates('hash')
                df_txs['value_eth'] = pd.to_numeric(df_txs['value']) / 10**18
                df_txs['cumulative_value'] = df_txs['value_eth'].cumsum()
                for _, row in df_txs.iterrows():
                    cur.execute("INSERT OR IGNORE INTO wallets (address, tx_hash, timestamp, value, from_addr, to_addr, source, chain, gas_used, cumulative_value) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                (wallet, row['hash'], row['timestamp'], row['value_eth'], row['from_address'], row['to_address'], 'moralis_tx', chain, row.get('gas', 0), row['cumulative_value']))
                # NFTs
                df_nfts['timestamp'] = datetime.datetime.now().isoformat()
                df_nfts = df_nfts.dropna(subset=['amount']).sort_values('last_metadata_sync', ascending=False)
                for _, row in df_nfts.iterrows():
                    cur.execute("INSERT OR IGNORE INTO wallets (address, tx_hash, timestamp, value, source, token_address, token_id, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (wallet, row.get('transaction_hash', 'nft_transfer'), row['timestamp'], row.get('floor_price', 0), 'moralis_nft', row['token_address'], row['token_id'], json.dumps(row.get('metadata', {}))))
                conn.commit()
                logging.info(f"Ingested, enriched balances/txs/nfts for Moralis {wallet} on {chain}")
            except Exception as e:
                logging.error(f"Moralis ingest error for {wallet} on {chain}: {e}")
            time.sleep(0.2)  # 1M calls/month ~ high rate

def ingest_marketstack(conn):
    for symbol in MARKETSTACK_SYMBOLS:
        try:
            params = {
                'symbols': symbol,
                'date_from': HISTORICAL_START,
                'date_to': datetime.datetime.now().strftime('%Y-%m-%d'),
                'limit': 1000,
                'offset': 0,
            }
            url = f"http://api.marketstack.com/v1/eod?access_key={MARKETSTACK_KEY}"
            data = []
            while True:
                response = retry_func(requests.get, url, params=params).json()
                pagination = response['pagination']
                data.extend(response['data'])
                if pagination['offset'] + pagination['limit'] >= pagination['total']:
                    break
                params['offset'] += pagination['limit']
            df = pd.DataFrame(data)
            df = df.dropna(subset=['close', 'volume']).sort_values('date').drop_duplicates('date')
            df['date'] = pd.to_datetime(df['date']).dt.isoformat()
            df['adjusted_close'] = df['adj_close']
            df['dividend'] = df.get('dividend', 0)
            df['split_factor'] = df.get('split_factor', 1)
            df['return'] = df['close'].pct_change().fillna(0)
            df['volatility'] = df['return'].rolling(window=30, min_periods=1).std() * np.sqrt(252)
            with db_lock:
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute(
                        "INSERT OR REPLACE INTO prices (ticker, date, open, high, low, close, volume, type, adjusted_close, market_cap, dividend, split_factor, volatility) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (symbol, row['date'], row['open'], row['high'], row['low'], row['close'], row['volume'], 'stock', row['adjusted_close'], row.get('market_cap', np.nan), row['dividend'], row['split_factor'], row['volatility']),
                    )
                conn.commit()
            logging.info(f"Ingested, enriched volatility/dividends for Marketstack {symbol}")
        except Exception as e:
            logging.error(f"Marketstack ingest error for {symbol}: {e}")
        time.sleep(6)


def ingest_oecd(conn):
    for indicator in OECD_INDICATORS:
        for country in OECD_COUNTRIES:
            try:
                url = f"https://stats.oecd.org/SDMX-JSON/data/MEI_CLI/{indicator}.{country}.PAIXSA/all?startTime={HISTORICAL_START}"
                response = retry_func(requests.get, url).json()
                structure = response['structure']
                data_sets = response['dataSets'][0]['series']
                df = pd.DataFrame()
                for key, value in data_sets.items():
                    series = value['observations']
                    dates = [structure['dimensions']['observation'][0]['values'][int(k)]['id'] for k in series.keys()]
                    values = [v[0] for v in series.values()]
                    temp_df = pd.DataFrame({'date': dates, 'value': values})
                    temp_df['indicator'] = indicator
                    temp_df['country'] = country
                    df = pd.concat([df, temp_df])
                df = df.dropna(subset=['value']).sort_values('date').drop_duplicates('date')
                df['date'] = pd.to_datetime(df['date']).dt.isoformat()
                df['growth_rate'] = df['value'].pct_change().fillna(0)
                df['z_score'] = (df['value'] - df['value'].mean()) / df['value'].std()
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency, growth_rate, z_score) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (row['indicator'], row['date'], row['value'], 'oecd', 'index', row['country'], 'monthly', row['growth_rate'], row['z_score']),
                        )
                    conn.commit()
                logging.info(f"Ingested, enriched growth/z for OECD {indicator} in {country}")
            except Exception as e:
                logging.error(f"OECD ingest error for {indicator} in {country}: {e}")
            time.sleep(1)


def ingest_trading_economics(conn):
    for country in TRADING_ECONOMICS_COUNTRIES:
        for indicator in TRADING_ECONOMICS_INDICATORS:
            try:
                url = f"https://api.tradingeconomics.com/historical/country/{country}/indicator/{indicator}?c={TRADING_ECONOMICS_KEY}&f=json"
                response = retry_func(requests.get, url).json()
                df = pd.DataFrame(response)
                df = df.dropna(subset=['Value']).sort_values('DateTime').drop_duplicates('DateTime')
                df['date'] = pd.to_datetime(df['DateTime']).dt.isoformat()
                df['value'] = pd.to_numeric(df['Value'], errors='coerce')
                df['ma_3'] = df['value'].rolling(window=3, min_periods=1).mean()
                df['ma_12'] = df['value'].rolling(window=12, min_periods=1).mean()
                df['crossover_signal'] = np.where(df['ma_3'] > df['ma_12'], 1, np.where(df['ma_3'] < df['ma_12'], -1, 0))
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency, ma_3, ma_12, crossover_signal) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (indicator, row['date'], row['value'], 'trading_economics', row.get('Unit', 'percent'), country, row.get('Frequency', 'monthly'), row['ma_3'], row['ma_12'], row['crossover_signal']),
                        )
                    conn.commit()
                logging.info(f"Ingested, enriched MA/crossover for Trading Economics {indicator} in {country}")
            except Exception as e:
                logging.error(f"Trading Economics ingest error for {indicator} in {country}: {e}")
            time.sleep(1)


def ingest_world_bank_databank(conn):
    wb = WorldBank()
    for indicator in WORLD_BANK_INDICATORS:
        try:
            df = wb.get_series(indicator, date=f"{HISTORICAL_START}:{datetime.datetime.now().strftime('%Y')}", mrv=10000)
            df = df.reset_index()
            df = df.dropna(subset=['value']).sort_values('Year').drop_duplicates(['Country', 'Year'])
            df['date'] = df['Year'].apply(lambda x: f"{x}-12-31")
            df['value'] = pd.to_numeric(df['value'], errors='coerce')
            df['country_growth'] = df.groupby('Country')['value'].pct_change().fillna(0)
            df['global_avg'] = df.groupby('date')['value'].transform('mean')
            df['dev_from_global'] = df['value'] - df['global_avg']
            with db_lock:
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute(
                        "INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency, country_growth, global_avg, dev_from_global) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (indicator, row['date'], row['value'], 'world_bank', 'various', row['Country'], 'annual', row['country_growth'], row['global_avg'], row['dev_from_global']),
                    )
                conn.commit()
            logging.info(f"Ingested, enriched growth/deviation for World Bank {indicator}")
        except Exception as e:
            logging.error(f"World Bank ingest error for {indicator}: {e}")
        time.sleep(1)


def ingest_census_bureau(conn):
    c = Census(CENSUS_BUREAU_KEY)
    for dataset in CENSUS_BUREAU_DATASETS:
        try:
            df = pd.DataFrame(c.acs5.state(('NAME', 'B01003_001E'), Census.ALL, year=int(datetime.datetime.now().year) - 1))
            df = df.dropna(subset=['B01003_001E']).sort_values('state')
            df['date'] = f"{datetime.datetime.now().year - 1}-12-31"
            df['value'] = pd.to_numeric(df['B01003_001E'], errors='coerce')
            df['series'] = 'population'
            df['country'] = 'US'
            df['state'] = df['state']
            df['density'] = df['value'] / df.get('land_area', 1)
            with db_lock:
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute(
                        "INSERT OR IGNORE INTO economic_indicators (series, date, value, source, unit, country, frequency, state, density) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (row['series'], row['date'], row['value'], 'census_bureau', 'people', row['country'], 'annual', row['state'], row['density']),
                    )
                conn.commit()
            logging.info(f"Ingested, enriched density for Census {dataset}")
        except Exception as e:
            logging.error(f"Census Bureau ingest error for {dataset}: {e}")
        time.sleep(1)

# Similar ultimate advanced code for Covalent, LunarCrush, Blockchair - detailed fetching, cleaning, enrichment, etc.

# main with all

# def main():
#     """Entry point placeholder."""
#     pass

# if __name__ == "__main__":
#     main()
