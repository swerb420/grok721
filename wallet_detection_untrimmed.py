"""Full wallet detection pipeline with network integrations.

This script corresponds to the original untrimmed version referenced in
``wallet_detection_pipeline.py``.  It includes extended Solana wallet
monitoring functions that make heavy use of external APIs.  The code is
provided for reference only and is not intended for production use.
"""

import pandas as pd
import numpy as np
import requests
import time
import datetime
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json
from solana.rpc.api import Client
import grpc
import networkx as nx  # pip install networkx
from sklearn.cluster import KMeans  # pip install scikit-learn
import subprocess
import os
from web3 import Web3  # For QuickNode; pip install web3
import stalk_pb2  # Generated from StalkChain proto; pip install grpcio grpcio-tools
import stalk_pb2_grpc
from gql import gql, Client as GQLClient  # For Bitquery; pip install python-graphql-client
from yellowstone_grpc.yellowstone_pb2 import SubscribeRequest  # pip install yellowstone-grpc
from yellowstone_grpc.yellowstone_pb2_grpc import YellowstoneServiceStub

# Config (assumes your existing DB_FILE, etc.)
DB_FILE = "super_db.db"
QUICKNODE_RPC = "https://example.solana-mainnet.quiknode.pro/000000/"  # Replace with your QuickNode endpoint
HELIUS_KEY = "YOUR_HELIUS_KEY_HERE"  # Free at helius.dev
BITQUERY_KEY = "YOUR_BITQUERY_KEY_HERE"  # Free at bitquery.io
COINGECKO_CRYPTOS = ['bonk', 'wif', 'shiba-inu', 'dogecoin', 'popcat', 'mew', 'book-of-meme', 'jito', 'helium', 'raydium']  # Solana memecoins
HISTORICAL_START = "2021-01-01"  # Bitquery since 2021, Helius since genesis
db_lock = threading.Lock()

# Logging setup
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler("system_log_detailed.txt", mode='a', encoding='utf-8'), logging.StreamHandler()])

# 1. QuickNode Solana Wallet Alerts Add-On
def ingest_quicknode_wallet_alerts(conn):
    solana_client = Client(QUICKNODE_RPC)
    def fetch_top_holders(token_mint):
        try:
            response = solana_client.get_token_largest_accounts(token_mint)
            if not response.value:
                return []
            return [account.address for account in response.value[:10]]  # Top 10 wallets
        except Exception as e:
            logging.error(f"QuickNode top holders error for {token_mint}: {e}")
            return []

    def fetch_wallet_txs(wallet, start_date, end_date):
        try:
            url = f"{QUICKNODE_RPC}"
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": [wallet, {"limit": 1000}]
            }
            response = requests.post(url, json=payload).json()
            if 'result' not in response:
                return None
            df = pd.DataFrame(response['result'])
            df['timestamp'] = pd.to_datetime(df['blockTime'], unit='s').astype(str)
            df['wallet'] = wallet
            df['amount'] = df.get('lamports', 0) / 1e9  # Convert to SOL
            # Enrich with inflow/outflow and hop detection
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'wallet'])
            G = nx.DiGraph()
            for _, row in df.iterrows():
                if 'fromAddress' in row and 'toAddress' in row:
                    G.add_edge(row['fromAddress'], row['toAddress'], amount=row['amount'], timestamp=row['timestamp'])
            df['hop_depth'] = df['wallet'].apply(lambda x: nx.shortest_path_length(G, source=x) if x in G else 0)
            df['inflow'] = df['amount'].where(df['toAddress'] == wallet, 0)
            df['outflow'] = df['amount'].where(df['fromAddress'] == wallet, 0)
            # Coordinated buy detection
            features = df[['inflow', 'outflow', 'blockTime']].fillna(0)
            if len(features) > 5:
                kmeans = KMeans(n_clusters=3, random_state=42)
                df['coordination_score'] = kmeans.fit_predict(features)
            else:
                df['coordination_score'] = 0
            return df
        except Exception as e:
            logging.error(f"QuickNode wallet tx error for {wallet}: {e}")
            return None

    start = datetime.datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.datetime.now()
    for coin in COINGECKO_CRYPTOS:
        token_mint = f"{coin}_mint_address"  # Replace with actual mint addresses
        wallets = fetch_top_holders(token_mint)
        with ThreadPoolExecutor(max_workers=4) as executor:  # Limited for M2 8GB
            futures = [executor.submit(fetch_wallet_txs, wallet, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')) for wallet in wallets]
            for future in as_completed(futures):
                df = future.result()
                if df is not None:
                    with db_lock:
                        cur = conn.cursor()
                        for _, row in df.iterrows():
                            cur.execute("INSERT OR IGNORE INTO wallets (timestamp, wallet, token, inflow, outflow, hop_depth, coordination_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                        (row['timestamp'], row['wallet'], coin, row['inflow'], row['outflow'], row['hop_depth'], row['coordination_score']))
                        conn.commit()
                        logging.info(f"Ingested QuickNode wallet alerts for {coin}")
        time.sleep(0.2)  # Increased sleep for RAM recovery on M2

# 2. Helius Solana Token/Wallet API
def ingest_helius_wallet(conn):
    def fetch_helius_txs(wallet, start_date, end_date):
        try:
            url = f"https://api.helius.dev/v0/addresses/{wallet}/transactions?api-key={HELIUS_KEY}&limit=1000"
            response = requests.get(url).json()
            if not response:
                return None
            df = pd.DataFrame(response)
            df['timestamp'] = pd.to_datetime(df['timestamp']).astype(str)
            df['wallet'] = wallet
            df['amount'] = df.get('amount', 0) / 1e9  # Convert to SOL
            # Enrich with inflow/outflow and hop detection
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'wallet'])
            G = nx.DiGraph()
            for _, row in df.iterrows():
                if 'to' in row and 'from' in row:
                    G.add_edge(row['from'], row['to'], amount=row['amount'], timestamp=row['timestamp'])
            df['hop_depth'] = df['wallet'].apply(lambda x: nx.shortest_path_length(G, source=x) if x in G else 0)
            df['inflow'] = df['amount'].where(df['to'] == wallet, 0)
            df['outflow'] = df['amount'].where(df['from'] == wallet, 0)
            df['tx_count'] = df.groupby('wallet').cumcount() + 1
            df['fresh_wallet'] = np.where(df['tx_count'] < 10, 1, 0)
            # Coordinated buy detection
            features = df[['inflow', 'outflow', 'timestamp']].fillna(0)
            if len(features) > 5:
                kmeans = KMeans(n_clusters=3, random_state=42)
                df['coordination_score'] = kmeans.fit_predict(features)
            else:
                df['coordination_score'] = 0
            return df
        except Exception as e:
            logging.error(f"Helius wallet tx error for {wallet}: {e}")
            return None

    start = datetime.datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.datetime.now()
    for coin in COINGECKO_CRYPTOS:
        token_mint = f"{coin}_mint_address"  # Replace with actual mint addresses
        wallets = solana_client.get_token_largest_accounts(token_mint).value[:10]
        wallets = [account.address for account in wallets]
        with ThreadPoolExecutor(max_workers=4) as executor:  # Limited for M2 8GB
            futures = [executor.submit(fetch_helius_txs, wallet, start.strftime('%Y-%m-%d'), end.strftime('%Y-%m-%d')) for wallet in wallets]
            for future in as_completed(futures):
                df = future.result()
                if df is not None:
                    with db_lock:
                        cur = conn.cursor()
                        for _, row in df.iterrows():
                            cur.execute("INSERT OR IGNORE INTO wallets (timestamp, wallet, token, inflow, outflow, hop_depth, coordination_score, fresh_wallet) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                        (row['timestamp'], row['wallet'], coin, row['inflow'], row['outflow'], row['hop_depth'], row['coordination_score'], row['fresh_wallet']))
                        conn.commit()
                        logging.info(f"Ingested Helius wallet for {coin}")
        time.sleep(0.2)  # Increased sleep for RAM recovery on M2

# 3. Bitquery Solana Transfers API
def ingest_bitquery_transfers(conn):
    client = GQLClient(endpoint="https://graphql.bitquery.io", headers={"X-API-KEY": BITQUERY_KEY})
    query = gql("""
    query($wallets: [String!], $from: ISO8601DateTime, $till: ISO8601DateTime) {
      solana {
        transfers(
          where: {senderAddress: {in: $wallets}, receiverAddress: {in: $wallets}, date: {since: $from, till: $till}}
        ) {
          block { timestamp { iso8601 } }
          senderAddress
          receiverAddress
          amount
        }
      }
    }
    """)
    def fetch_bitquery_txs(wallets, start_date, end_date):
        try:
            result = client.execute(query, variable_values={"wallets": wallets, "from": start_date, "till": end_date})
            transfers = result['solana']['transfers']
            df = pd.DataFrame(transfers)
            df['timestamp'] = pd.to_datetime(df['block'].apply(lambda x: x['timestamp']['iso8601'])).astype(str)
            df['amount'] = df['amount'].astype(float)
            # Enrich with inflow/outflow and hop detection
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'senderAddress', 'receiverAddress'])
            G = nx.DiGraph()
            for _, row in df.iterrows():
                G.add_edge(row['senderAddress'], row['receiverAddress'], amount=row['amount'], timestamp=row['timestamp'])
            df['hop_depth'] = df['senderAddress'].apply(lambda x: nx.shortest_path_length(G, source=x) if x in G else 0)
            df['inflow'] = df.groupby('receiverAddress')['amount'].transform('sum')
            df['outflow'] = df.groupby('senderAddress')['amount'].transform('sum')
            # Coordinated buy detection
            features = df[['inflow', 'outflow', 'timestamp']].fillna(0)
            if len(features) > 5:
                kmeans = KMeans(n_clusters=3, random_state=42)
                df['coordination_score'] = kmeans.fit_predict(features)
            else:
                df['coordination_score'] = 0
            return df
        except Exception as e:
            logging.error(f"Bitquery transfers error for wallets: {e}")
            return None

    start = datetime.datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.datetime.now()
    for coin in COINGECKO_CRYPTOS:
        token_mint = f"{coin}_mint_address"  # Replace with actual mint addresses
        wallets = solana_client.get_token_largest_accounts(token_mint).value[:10]
        wallets = [account.address for account in wallets]
        with ThreadPoolExecutor(max_workers=4) as executor:  # Limited for M2 8GB
            futures = [executor.submit(fetch_bitquery_txs, wallets, start.strftime('%Y-%m-%dT%H:%M:%SZ'), end.strftime('%Y-%m-%dT%H:%M:%SZ'))]
            for future in as_completed(futures):
                df = future.result()
                if df is not None:
                    with db_lock:
                        cur = conn.cursor()
                        for _, row in df.iterrows():
                            cur.execute("INSERT OR IGNORE INTO wallets (timestamp, wallet, token, inflow, outflow, hop_depth, coordination_score) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                        (row['timestamp'], row['receiverAddress'], coin, row['inflow'], row['outflow'], row['hop_depth'], row['coordination_score']))
                        conn.commit()
                        logging.info(f"Ingested Bitquery transfers for {coin}")
        time.sleep(0.2)  # Increased sleep for RAM recovery on M2

# 4. Solana Wallet Scanner GitHub Integration
def ingest_solana_wallet_scanner(conn):
    def run_scanner(wallet, token_mint):
        try:
            # Run nothingdao/solana-wallet-scanner as subprocess (assume cloned to local path)
            result = subprocess.run(['python', 'solana-wallet-scanner/main.py', '--wallet', wallet, '--mint', token_mint], capture_output=True, text=True)
            data = json.loads(result.stdout)
            df = pd.DataFrame(data['transactions'])
            df['timestamp'] = pd.to_datetime(df['timestamp']).astype(str)
            df['wallet'] = wallet
            df['token'] = token_mint
            # Enrich with scam detection and hop analysis
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'wallet'])
            df['scam_flag'] = df['approvals'].apply(lambda x: 1 if x.get('suspicious', False) else 0)
            G = nx.DiGraph()
            for _, row in df.iterrows():
                if 'from' in row and 'to' in row:
                    G.add_edge(row['from'], row['to'], amount=row.get('amount', 0), timestamp=row['timestamp'])
            df['hop_depth'] = df['wallet'].apply(lambda x: nx.shortest_path_length(G, source=x) if x in G else 0)
            df['inflow'] = df['amount'].where(df['to'] == wallet, 0)
            df['outflow'] = df['amount'].where(df['from'] == wallet, 0)
            # Coordinated buy detection
            features = df[['inflow', 'outflow', 'timestamp']].fillna(0)
            if len(features) > 5:
                kmeans = KMeans(n_clusters=3, random_state=42)
                df['coordination_score'] = kmeans.fit_predict(features)
            else:
                df['coordination_score'] = 0
            return df
        except Exception as e:
            logging.error(f"Solana Wallet Scanner error for {wallet}: {e}")
            return None

    start = datetime.datetime.strptime(HISTORICAL_START, '%Y-%m-%d')
    end = datetime.datetime.now()
    for coin in COINGECKO_CRYPTOS:
        token_mint = f"{coin}_mint_address"  # Replace with actual mint addresses
        wallets = solana_client.get_token_largest_accounts(token_mint).value[:10]
        wallets = [account.address for account in wallets]
        with ThreadPoolExecutor(max_workers=4) as executor:  # Limited for M2 8GB
            futures = [executor.submit(run_scanner, wallet, token_mint) for wallet in wallets]
            for future in as_completed(futures):
                df = future.result()
                if df is not None:
                    with db_lock:
                        cur = conn.cursor()
                        for _, row in df.iterrows():
                            cur.execute("INSERT OR IGNORE INTO wallets (timestamp, wallet, token, inflow, outflow, hop_depth, coordination_score, scam_flag) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                        (row['timestamp'], row['wallet'], row['token'], row['inflow'], row['outflow'], row['hop_depth'], row['coordination_score'], row['scam_flag']))
                        conn.commit()
                        logging.info(f"Ingested Solana Wallet Scanner for {coin}")
        time.sleep(0.2)  # Increased sleep for RAM recovery on M2

# 5. Yellowstone Geyser gRPC for Solana Liquidity Monitoring
def ingest_yellowstone_geyser(conn):
    channel = grpc.insecure_channel('yellowstone:10000')  # Replace with your Yellowstone gRPC server
    stub = YellowstoneServiceStub(channel)
    def fetch_geyser_data(token_mint):
        try:
            request = SubscribeRequest(accounts=[token_mint], transactions=True)
            responses = stub.Subscribe(request)
            data = []
            for response in responses:
                if response.transaction:
                    data.append({
                        'timestamp': pd.to_datetime(response.transaction.block_time, unit='s').strftime('%Y-%m-%d %H:%M:%S'),
                        'token': token_mint,
                        'amount': response.transaction.meta.get('lamports', 0) / 1e9,
                        'from': response.transaction.transaction.message.account_keys[0],
                        'to': response.transaction.transaction.message.account_keys[1]
                    })
                if len(data) >= 1000:
                    break
            df = pd.DataFrame(data)
            # Enrich with inflow/outflow and liquidity hop detection
            df = df.dropna().sort_values('timestamp').drop_duplicates(['timestamp', 'token'])
            G = nx.DiGraph()
            for _, row in df.iterrows():
                G.add_edge(row['from'], row['to'], amount=row['amount'], timestamp=row['timestamp'])
            df['hop_depth'] = df['to'].apply(lambda x: nx.shortest_path_length(G, source=x) if x in G else 0)
            df['inflow'] = df.groupby('to')['amount'].transform('sum')
            df['outflow'] = df.groupby('from')['amount'].transform('sum')
            df['liquidity_spike'] = np.where(df['inflow'] > df['inflow'].quantile(0.95), 1, 0)
            # Coordinated buy detection
            features = df[['inflow', 'outflow', 'timestamp']].fillna(0)
            if len(features) > 5:
                kmeans = KMeans(n_clusters=3, random_state=42)
                df['coordination_score'] = kmeans.fit_predict(features)
            else:
                df['coordination_score'] = 0
            return df
        except Exception as e:
            logging.error(f"Yellowstone Geyser error for {token_mint}: {e}")
            return None

    for coin in COINGECKO_CRYPTOS:
        token_mint = f"{coin}_mint_address"  # Replace with actual mint addresses
        df = fetch_geyser_data(token_mint)
        if df is not None:
            with db_lock:
                cur = conn.cursor()
                for _, row in df.iterrows():
                    cur.execute("INSERT OR IGNORE INTO wallets (timestamp, wallet, token, inflow, outflow, hop_depth, coordination_score, liquidity_spike) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                                (row['timestamp'], row['to'], row['token'], row['inflow'], row['outflow'], row['hop_depth'], row['coordination_score'], row['liquidity_spike']))
                conn.commit()
                logging.info(f"Ingested Yellowstone Geyser for {coin}")
        time.sleep(0.2)  # Increased sleep for RAM recovery on M2
