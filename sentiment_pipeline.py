import logging
import threading
import time
import json
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler
from backtrader import Cerebro, Strategy
import backtrader as bt
from backtrader.feeds import PandasData
from sklearn.cluster import KMeans
from statsmodels.tsa.stattools import grangercausalitytests
import talib

from config import get_config

# Config
DB_FILE = get_config("DB_FILE", "super_db.db")
FINNHUB_KEY = get_config("FINNHUB_KEY", "YOUR_FINNHUB_KEY_HERE")
ALPHA_VANTAGE_KEY = get_config("ALPHA_VANTAGE_KEY", "YOUR_ALPHA_VANTAGE_KEY_HERE")
POLYGON_KEY = get_config("POLYGON_KEY", "YOUR_POLYGON_KEY_HERE")
FMP_KEY = get_config("FMP_KEY", "YOUR_FMP_KEY_HERE")

COINGECKO_CRYPTOS = ['bitcoin', 'ethereum', 'solana', 'ripple', 'cardano', 'dogecoin', 'polkadot', 'chainlink', 'uniswap', 'litecoin', 'stellar', 'avalanche-2', 'binancecoin', 'terra-luna', 'cosmos', 'aave', 'maker', 'compound-governance-token', 'yearn-finance', 'synthetix-network-token']
FINNHUB_SYMBOLS = ['AAPL', 'TSLA', 'BTC-USD', 'ETH-USD', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'V', 'JPM', 'UNH', 'MA', 'HD']
POLYGON_TICKERS = ['AAPL', 'TSLA', 'X:BTCUSD', 'X:ETHUSD', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK-B', 'V', 'JPM', 'UNH', 'MA', 'HD']
FMP_SYMBOLS = ['AAPL', 'TSLA', 'MSFT', 'NVDA', 'GOOGL', 'AMZN', 'META', 'BRK.B', 'V', 'JPM', 'UNH', 'MA', 'HD']
HISTORICAL_START = "2010-01-01"

db_lock = threading.Lock()

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("system_log_detailed.txt", mode='a', encoding='utf-8'),
                              logging.StreamHandler()])


def ingest_finnhub(conn):
    def fetch_finnhub_data(symbol, start_date, end_date):
        try:
            url = (
                f"https://finnhub.io/api/v1/stock/candle?symbol={symbol}&resolution=D"
                f"&from={int(time.mktime(datetime.strptime(start_date, '%Y-%m-%d').timetuple()))}"
                f"&to={int(time.mktime(datetime.strptime(end_date, '%Y-%m-%d').timetuple()))}"
                f"&token={FINNHUB_KEY}"
            )
            response = requests.get(url).json()
            if response.get('s') != 'ok':
                return None
            df = pd.DataFrame({
                'timestamp': pd.to_datetime(response['t'], unit='s').astype(str),
                'symbol': symbol,
                'open': response['o'],
                'high': response['h'],
                'low': response['l'],
                'close': response['c'],
                'volume': response['v']
            })
            sent_url = f"https://finnhub.io/api/v1/news-sentiment?symbol={symbol}&token={FINNHUB_KEY}"
            sent_response = requests.get(sent_url).json()
            sentiment_score = sent_response.get('sentiment', {}).get('score', 0)
            df['social_score'] = sentiment_score
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'symbol'])
            df['volume_z'] = (
                (df['volume'] - df['volume'].rolling(window=20, min_periods=1).mean()) /
                df['volume'].rolling(window=20, min_periods=1).std()
            )
            df['event_flag'] = np.where((df['volume_z'] > 2) & (abs(df['social_score']) > 0.8), 'big_event', 'normal')
            return df
        except Exception as e:
            logging.error(f"Finnhub fetch error for {symbol}: {e}")
            return None

    start = datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.now()
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [
            executor.submit(
                fetch_finnhub_data,
                symbol,
                (start + timedelta(days=i*180)).strftime('%Y-%m-%d'),
                (start + timedelta(days=(i+1)*180)).strftime('%Y-%m-%d'),
            )
            for symbol in FINNHUB_SYMBOLS
            for i in range(int((end - start).days / 180) + 1)
        ]
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT OR IGNORE INTO prices (timestamp, symbol, open, high, low, close, volume, social_score, volume_z, event_flag)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (row['timestamp'], row['symbol'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['social_score'], row['volume_z'], row['event_flag'])
                        )
                    conn.commit()
                    logging.info(f"Ingested Finnhub for {row['symbol']}")
            time.sleep(1)


def ingest_alpha_vantage_economic(conn):
    def fetch_alpha_data(symbol, endpoint, outputsize='full'):
        try:
            url = f"https://www.alphavantage.co/query?function={endpoint}&symbol={symbol}&apikey={ALPHA_VANTAGE_KEY}&outputsize={outputsize}"
            response = requests.get(url).json()
            if 'Time Series' not in response:
                return None
            data = response['Time Series (Daily)'] if endpoint == 'TIME_SERIES_DAILY' else response['data']
            df = pd.DataFrame.from_dict(data, orient='index').reset_index()
            df['timestamp'] = pd.to_datetime(df['index']).astype(str)
            df['symbol'] = symbol
            df['close'] = df['4. close'].astype(float) if endpoint == 'TIME_SERIES_DAILY' else df['value'].astype(float)
            df = df[['timestamp', 'symbol', 'close']]
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'symbol'])
            df['rsi'] = talib.RSI(df['close'], timeperiod=14)
            df['macd'], df['macd_signal'], _ = talib.MACD(df['close'], fastperiod=12, slowperiod=26, signalperiod=9)
            df['indicator_label'] = np.where(df['rsi'] > 70, 'overbought', np.where(df['rsi'] < 30, 'oversold', 'neutral'))
            df['event_flag'] = np.where((abs(df['macd'] - df['macd_signal']) > df['macd'].std()) & (abs(df['rsi'] - 50) > 20), 'big_event', 'normal')
            return df
        except Exception as e:
            logging.error(f"Alpha Vantage fetch error for {symbol}: {e}")
            return None

    start = datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.now()
    endpoints = ['TIME_SERIES_DAILY', 'REAL_GDP', 'UNEMPLOYMENT', 'CPI']
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(fetch_alpha_data, symbol, endpoint)
            for symbol in FINNHUB_SYMBOLS + ['GDP', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS']
            for endpoint in endpoints
        ]
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT OR IGNORE INTO economic_indicators (timestamp, symbol, value, rsi, macd, macd_signal, indicator_label, event_flag)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (row['timestamp'], row['symbol'], row['close'], row['rsi'], row['macd'], row['macd_signal'], row['indicator_label'], row['event_flag'])
                        )
                    conn.commit()
                    logging.info(f"Ingested Alpha Vantage for {row['symbol']}")
            time.sleep(12)


def ingest_polygon(conn):
    def fetch_polygon_data(ticker, start_date, end_date):
        try:
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start_date}/{end_date}?apiKey={POLYGON_KEY}"
            response = requests.get(url).json()
            if response.get('status') != 'OK':
                return None
            df = pd.DataFrame(response['results'])
            df['timestamp'] = pd.to_datetime(df['t'], unit='ms').astype(str)
            df['symbol'] = ticker
            df = df.rename(columns={'o': 'open', 'h': 'high', 'l': 'low', 'c': 'close', 'v': 'volume'})
            df = df[['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'volume']]
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'symbol'])
            df['tick_volatility'] = (df['high'] - df['low']) / df['close']
            df['volume_z'] = (
                (df['volume'] - df['volume'].rolling(window=20, min_periods=1).mean()) /
                df['volume'].rolling(window=20, min_periods=1).std()
            )
            df['event_flag'] = np.where(df['tick_volatility'] > df['tick_volatility'].quantile(0.95), 'big_event', 'normal')
            return df
        except Exception as e:
            logging.error(f"Polygon fetch error for {ticker}: {e}")
            return None

    start = datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.now()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(
                fetch_polygon_data,
                ticker,
                (start + timedelta(days=i*180)).strftime('%Y-%m-%d'),
                (start + timedelta(days=(i+1)*180)).strftime('%Y-%m-%d'),
            )
            for ticker in POLYGON_TICKERS
            for i in range(int((end - start).days / 180) + 1)
        ]
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT OR IGNORE INTO high_res_prices (timestamp, symbol, open, high, low, close, volume, tick_volatility, event_flag)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (row['timestamp'], row['symbol'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['tick_volatility'], row['event_flag'])
                        )
                    conn.commit()
                    logging.info(f"Ingested Polygon for {row['symbol']}")
            time.sleep(12)


def ingest_financial_modeling_prep(conn):
    def fetch_fmp_data(symbol, start_date, end_date):
        try:
            url = f"https://financialmodelingprep.com/api/v3/historical-price-full/{symbol}?from={start_date}&to={end_date}&apikey={FMP_KEY}"
            response = requests.get(url).json()
            if not response.get('historical'):
                return None
            df = pd.DataFrame(response['historical'])
            df['timestamp'] = pd.to_datetime(df['date']).astype(str)
            df['symbol'] = symbol
            df = df.rename(columns={'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
            sent_url = f"https://financialmodelingprep.com/api/v4/company-outlook?symbol={symbol}&apikey={FMP_KEY}"
            sent_response = requests.get(sent_url).json()
            sentiment_score = sent_response.get('profile', {}).get('ratingRecommendation', 'neutral')
            df['sentiment_fund_label'] = sentiment_score
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'symbol'])
            df['rating_change'] = df['sentiment_fund_label'].ne(df['sentiment_fund_label'].shift()).cumsum()
            df['event_flag'] = np.where((df['volume'] > df['volume'].quantile(0.95)) & (df['rating_change'] > 0), 'big_event', 'normal')
            return df
        except Exception as e:
            logging.error(f"FMP fetch error for {symbol}: {e}")
            return None

    start = datetime.strptime("1985-01-01", '%Y-%m-%d')
    end = datetime.now()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                fetch_fmp_data,
                symbol,
                (start + timedelta(days=i*365)).strftime('%Y-%m-%d'),
                (start + timedelta(days=(i+1)*365)).strftime('%Y-%m-%d'),
            )
            for symbol in FMP_SYMBOLS
            for i in range(int((end - start).days / 365) + 1)
        ]
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT OR IGNORE INTO economic_indicators (timestamp, symbol, open, high, low, close, volume, sentiment_fund_label, rating_change, event_flag)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (row['timestamp'], row['symbol'], row['open'], row['high'], row['low'], row['close'], row['volume'], row['sentiment_fund_label'], row['rating_change'], row['event_flag'])
                        )
                    conn.commit()
                    logging.info(f"Ingested FMP for {row['symbol']}")
            time.sleep(0.4)


def ingest_coingecko_crypto(conn):
    def fetch_coingecko_data(coin, start_date, end_date):
        try:
            start_ts = int(time.mktime(datetime.strptime(start_date, '%Y-%m-%d').timetuple()))
            end_ts = int(time.mktime(datetime.strptime(end_date, '%Y-%m-%d').timetuple()))
            url = f"https://api.coingecko.com/api/v3/coins/{coin}/market_chart/range?vs_currency=usd&from={start_ts}&to={end_ts}"
            response = requests.get(url).json()
            if not response.get('prices'):
                return None
            df = pd.DataFrame({
                'timestamp': [pd.to_datetime(t[0], unit='ms').strftime('%Y-%m-%d %H:%M:%S') for t in response['prices']],
                'symbol': coin,
                'close': [p[1] for p in response['prices']],
                'volume': [v[1] for v in response['total_volumes']]
            })
            social_url = f"https://api.coingecko.com/api/v3/coins/{coin}?localization=false&tickers=false&market_data=false&community_data=true&developer_data=true"
            social_response = requests.get(social_url).json()
            social_score = (
                social_response.get('community_data', {}).get('twitter_followers', 0) +
                social_response.get('developer_data', {}).get('code_additions_deletions_4_weeks', 0)
            ) / 1000
            df['social_score'] = social_score
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'symbol'])
            df['social_growth'] = df['social_score'].pct_change().fillna(0)
            df['volume_z'] = (
                (df['volume'] - df['volume'].rolling(window=20, min_periods=1).mean()) /
                df['volume'].rolling(window=20, min_periods=1).std()
            )
            df['event_flag'] = np.where((df['volume_z'] > 2) & (df['social_growth'] > df['social_growth'].quantile(0.95)), 'big_event', 'normal')
            return df
        except Exception as e:
            logging.error(f"CoinGecko fetch error for {coin}: {e}")
            return None

    start = datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.now()
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [
            executor.submit(
                fetch_coingecko_data,
                coin,
                (start + timedelta(days=i*180)).strftime('%Y-%m-%d'),
                (start + timedelta(days=(i+1)*180)).strftime('%Y-%m-%d'),
            )
            for coin in COINGECKO_CRYPTOS
            for i in range(int((end - start).days / 180) + 1)
        ]
        for future in as_completed(futures):
            df = future.result()
            if df is not None:
                with db_lock:
                    cur = conn.cursor()
                    for _, row in df.iterrows():
                        cur.execute(
                            "INSERT OR IGNORE INTO prices (timestamp, symbol, close, volume, social_score, social_growth, volume_z, event_flag)"
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                            (row['timestamp'], row['symbol'], row['close'], row['volume'], row['social_score'], row['social_growth'], row['volume_z'], row['event_flag'])
                        )
                    conn.commit()
                    logging.info(f"Ingested CoinGecko for {row['symbol']}")
            time.sleep(1.2)


def analyze_patterns(conn):
    cur = conn.cursor()
    cur.execute("SELECT timestamp, symbol, close, volume, social_score, volume_z, event_flag FROM prices WHERE event_flag='big_event'")
    prices = pd.DataFrame(cur.fetchall(), columns=['timestamp', 'symbol', 'close', 'volume', 'social_score', 'volume_z', 'event_flag'])
    cur.execute("SELECT timestamp, symbol, value, rsi, macd, macd_signal, indicator_label, event_flag FROM economic_indicators WHERE event_flag='big_event'")
    econ = pd.DataFrame(cur.fetchall(), columns=['timestamp', 'symbol', 'value', 'rsi', 'macd', 'macd_signal', 'indicator_label', 'event_flag'])
    cur.execute("SELECT timestamp, symbol, close, volume, tick_volatility, event_flag FROM high_res_prices WHERE event_flag='big_event'")
    high_res = pd.DataFrame(cur.fetchall(), columns=['timestamp', 'symbol', 'close', 'volume', 'tick_volatility', 'event_flag'])
    cur.execute("SELECT timestamp, title, sentiment_score FROM news WHERE sentiment_score > 0.5 OR sentiment_score < -0.5")
    news = pd.DataFrame(cur.fetchall(), columns=['timestamp', 'title', 'sentiment_score'])

    prices['timestamp'] = pd.to_datetime(prices['timestamp'])
    econ['timestamp'] = pd.to_datetime(econ['timestamp'])
    high_res['timestamp'] = pd.to_datetime(high_res['timestamp'])
    news['timestamp'] = pd.to_datetime(news['timestamp'])
    merged = prices.merge(econ, on=['timestamp', 'symbol'], how='left', suffixes=('', '_econ')).merge(
        high_res, on=['timestamp', 'symbol'], how='left', suffixes=('', '_high')).merge(news, on='timestamp', how='left')

    features = merged[['social_score', 'volume_z', 'rsi', 'tick_volatility', 'sentiment_score']].fillna(0)
    kmeans = KMeans(n_clusters=3, random_state=42)
    merged['event_cluster'] = kmeans.fit_predict(features)
    for symbol in merged['symbol'].unique():
        df_symbol = merged[merged['symbol'] == symbol][['close', 'social_score', 'sentiment_score']].dropna()
        if len(df_symbol) > 10:
            grangercausalitytests(df_symbol[['close', 'social_score']], maxlag=5, verbose=False)
            grangercausalitytests(df_symbol[['close', 'sentiment_score']], maxlag=5, verbose=False)

    with db_lock:
        for _, row in merged[merged['event_flag'] == 'big_event'].iterrows():
            cur.execute(
                "INSERT OR IGNORE INTO patterns (timestamp, symbol, pattern_type, pattern_value, confidence) VALUES (?, ?, ?, ?, ?)",
                (row['timestamp'], row['symbol'], 'sentiment_event', f"Social: {row['social_score']}, News: {row['sentiment_score']}", row['volume_z'])
            )
        conn.commit()
    logging.info("Analyzed patterns for sentiment-driven events")


class SentimentStrategy(Strategy):
    params = (('leverage', 10), ('size', 1000), ('stop_loss', 0.05), ('take_profit', 0.1))

    def __init__(self):
        self.dataclose = self.datas[0].close
        self.volume_z = self.datas[0].volume_z
        self.social_score = self.datas[0].social_score
        self.event_flag = self.datas[0].event_flag
        self.order = None

    def next(self):
        if self.event_flag[0] == 'big_event' and self.social_score[0] > 0.8:
            self.order = self.buy(size=self.params.size * self.params.leverage)
            self.sell(exectype=bt.Order.Stop, price=self.dataclose[0] * (1 - self.params.stop_loss), size=self.params.size * self.params.leverage)
            self.sell(exectype=bt.Order.Limit, price=self.dataclose[0] * (1 + self.params.take_profit), size=self.params.size * self.params.leverage)
        elif self.event_flag[0] == 'big_event' and self.social_score[0] < -0.8:
            self.order = self.sell(size=self.params.size * self.params.leverage)
            self.buy(exectype=bt.Order.Stop, price=self.dataclose[0] * (1 + self.params.stop_loss), size=self.params.size * self.params.leverage)
            self.buy(exectype=bt.Order.Limit, price=self.dataclose[0] * (1 - self.params.take_profit), size=self.params.size * self.params.leverage)


def backtest_strategies(conn):
    cerebro = Cerebro()
    cerebro.addstrategy(SentimentStrategy)
    cur = conn.cursor()
    for symbol in FINNHUB_SYMBOLS + COINGECKO_CRYPTOS:
        cur.execute(
            "SELECT timestamp, open, high, low, close, volume, social_score, volume_z, event_flag FROM prices WHERE symbol=?",
            (symbol,)
        )
        df = pd.DataFrame(cur.fetchall(), columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'social_score', 'volume_z', 'event_flag'])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df = df.set_index('timestamp')
        data = PandasData(dataname=df)
        cerebro.adddata(data)
    cerebro.broker.set_cash(100000)
    cerebro.broker.setcommission(commission=0.001, leverage=10)
    cerebro.run()
    cerebro.plot()
    logging.info("Backtested sentiment-driven strategies")


def export_for_finetuning(conn, output_file="llm_data.jsonl"):
    cur = conn.cursor()
    cur.execute(
        "SELECT p.timestamp, p.symbol, p.close, p.social_score, p.volume_z, p.event_flag, e.rsi, e.macd, e.indicator_label, h.tick_volatility, n.sentiment_score "
        "FROM prices p LEFT JOIN economic_indicators e ON p.timestamp=e.timestamp AND p.symbol=e.symbol "
        "LEFT JOIN high_res_prices h ON p.timestamp=h.timestamp AND p.symbol=h.symbol "
        "LEFT JOIN news n ON p.timestamp=n.timestamp WHERE p.event_flag='big_event'"
    )
    data = pd.DataFrame(cur.fetchall(), columns=['timestamp', 'symbol', 'close', 'social_score', 'volume_z', 'event_flag', 'rsi', 'macd', 'indicator_label', 'tick_volatility', 'sentiment_score'])

    with open(output_file, 'w') as f:
        for _, row in data.iterrows():
            price_change = (
                (row['close'] - data[data['timestamp'] < row['timestamp']]['close'].tail(1).values[0]) /
                data[data['timestamp'] < row['timestamp']]['close'].tail(1).values[0]
            ) if len(data[data['timestamp'] < row['timestamp']]) > 0 else 0
            jsonl = {
                "timestamp": row['timestamp'],
                "symbol": row['symbol'],
                "event_impact": price_change,
                "social_score": row['social_score'],
                "volume_z": row['volume_z'],
                "event_flag": row['event_flag'],
                "rsi": row['rsi'],
                "macd": row['macd'],
                "indicator_label": row['indicator_label'],
                "tick_volatility": row['tick_volatility'],
                "sentiment_score": row['sentiment_score']
            }
            f.write(json.dumps(jsonl) + '\n')
    logging.info(f"Exported LLM fine-tuning data to {output_file}")


def main():
    conn = sqlite3.connect(DB_FILE)
    ingest_functions = [
        ingest_finnhub,
        ingest_alpha_vantage_economic,
        ingest_polygon,
        ingest_financial_modeling_prep,
        ingest_coingecko_crypto,
    ]
    scheduler = BackgroundScheduler()
    for func in ingest_functions:
        scheduler.add_job(func, 'interval', minutes=60, args=[conn])
    scheduler.add_job(analyze_patterns, 'interval', hours=1, args=[conn])
    scheduler.add_job(backtest_strategies, 'interval', days=1, args=[conn])
    scheduler.add_job(export_for_finetuning, 'interval', days=1, args=[conn])
    scheduler.start()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
        conn.close()


if __name__ == "__main__":
    main()
