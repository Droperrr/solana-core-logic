import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import config

import sqlite3
import requests
import time
from typing import List, Dict, Any
import logging
from datetime import datetime, timedelta

DB_PATH = "db/solana_db.sqlite"
TOKENS_PATH = "data/group_b_tokens.txt"
HISTORICAL_PRICES_TABLE = "historical_prices"

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

MAX_RETRIES = 5
INITIAL_BACKOFF_SECONDS = 1


def create_prices_table(conn):
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {HISTORICAL_PRICES_TABLE} (
            token_address TEXT NOT NULL,
            timestamp INTEGER NOT NULL,
            open REAL,
            high REAL,
            low REAL,
            close REAL,
            volume REAL,
            PRIMARY KEY (token_address, timestamp)
        )
    ''')
    conn.commit()


def read_token_list(path: str) -> List[str]:
    with open(path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip() and not line.startswith('#')]


def get_token_activity_range(conn, token_address: str) -> (int, int):
    cursor = conn.cursor()
    # Определяем диапазон дат по активности токена (по полю block_time в transactions)
    cursor.execute('''
        SELECT MIN(block_time), MAX(block_time) FROM transactions
        WHERE source_query_address = ?
    ''', (token_address,))
    row = cursor.fetchone()
    if row and row[0] and row[1]:
        return int(row[0]), int(row[1])
    # Если нет активности — по умолчанию последние 30 дней
    end_time = int(time.time())
    start_time = end_time - 30 * 24 * 60 * 60
    return start_time, end_time


def fetch_prices(token_address: str, start_time: int, end_time: int) -> List[Dict[str, Any]]:
    # Сначала пробуем Jupiter
    params = {
        "ids": token_address,
        "mint": token_address,
        "startTime": start_time,
        "endTime": end_time,
        "interval": "1m"
    }
    for attempt in range(MAX_RETRIES):
        try:
            resp = requests.get(config.JUPITER_PRICE_API_URL, params=params, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                bars = []
                for bar in data:
                    bars.append({
                        "timestamp": int(bar.get("startTime", bar.get("timestamp", 0))),
                        "open": float(bar["open"]),
                        "high": float(bar["high"]),
                        "low": float(bar["low"]),
                        "close": float(bar["close"]),
                        "volume": float(bar.get("volume", 0))
                    })
                if bars:
                    logging.info(f"  Источник цен: Jupiter API")
                    return bars
            else:
                raise requests.RequestException(f"Jupiter API status {resp.status_code}")
        except requests.RequestException as e:
            wait = INITIAL_BACKOFF_SECONDS * (2 ** attempt)
            logging.warning(f"Ошибка запроса к Jupiter API для {token_address} (попытка {attempt+1}/{MAX_RETRIES}): {e}. Жду {wait} сек...")
            time.sleep(wait)
        except Exception as e:
            logging.error(f"Неожиданная ошибка при запросе цен для {token_address}: {e}")
            break
    # Fallback: CoinGecko
    try:
        cg_params = {
            "vs_currency": "usd",
            "from": start_time,
            "to": end_time
        }
        resp = requests.get(config.COINGECKO_API_URL, params=cg_params, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            bars = _normalize_coingecko_data(data, start_time, end_time)
            if bars:
                logging.info(f"  Источник цен: CoinGecko API")
                return bars
        else:
            logging.error(f"CoinGecko API status {resp.status_code}")
    except Exception as e:
        logging.critical(f"Ошибка при запросе CoinGecko для {token_address}: {e}")
    return []


def _normalize_coingecko_data(data: dict, start_time: int, end_time: int) -> List[Dict[str, Any]]:
    # CoinGecko возвращает 'prices': [[timestamp, price], ...]
    # и 'total_volumes': [[timestamp, volume], ...]
    # OHLCV нет, поэтому open/high/low/close = price, volume = volume
    prices = data.get('prices', [])
    volumes = {int(ts/1000): v for ts, v in data.get('total_volumes', [])}
    bars = []
    for ts_ms, price in prices:
        ts = int(ts_ms / 1000)
        if ts < start_time or ts > end_time:
            continue
        bars.append({
            "timestamp": ts,
            "open": price,
            "high": price,
            "low": price,
            "close": price,
            "volume": volumes.get(ts, 0)
        })
    return bars


def upsert_prices_to_db(conn, token_address: str, ohlcv_data: List[Dict[str, Any]]):
    cursor = conn.cursor()
    batch = []
    for bar in ohlcv_data:
        batch.append((
            token_address,
            bar['timestamp'],
            bar['open'],
            bar['high'],
            bar['low'],
            bar['close'],
            bar['volume'],
        ))
    if batch:
        cursor.executemany(f'''
            INSERT OR REPLACE INTO {HISTORICAL_PRICES_TABLE} (token_address, timestamp, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', batch)
        conn.commit()


def backfill_all_tokens():
    conn = sqlite3.connect(DB_PATH)
    create_prices_table(conn)
    tokens = read_token_list(TOKENS_PATH)
    total_tokens = len(tokens)
    tokens_with_prices = 0
    tokens_with_errors = []
    total_price_records = 0
    start_exec = time.time()
    for token in tokens:
        try:
            logging.info(f"Обработка токена: {token}")
            start_time, end_time = get_token_activity_range(conn, token)
            logging.info(f"  Диапазон дат: {datetime.utcfromtimestamp(start_time)} — {datetime.utcfromtimestamp(end_time)}")
            bars = fetch_prices(token, start_time, end_time)
            if bars:
                upsert_prices_to_db(conn, token, bars)
                logging.info(f"  Загружено баров: {len(bars)}")
                tokens_with_prices += 1
                total_price_records += len(bars)
            else:
                logging.warning(f"  Нет данных для токена {token}")
                tokens_with_errors.append(token)
        except Exception as e:
            logging.error(f"Ошибка при обработке токена {token}: {e}")
            tokens_with_errors.append(token)
    conn.close()
    exec_time = time.time() - start_exec
    logging.info("Price data backfill complete.")
    logging.info(f"Total tokens processed: {total_tokens}")
    logging.info(f"Tokens with prices found: {tokens_with_prices}")
    logging.info(f"Tokens with API errors: {len(tokens_with_errors)} ({', '.join(tokens_with_errors)})")
    logging.info(f"Total price records saved: {total_price_records}")
    logging.info(f"Total execution time: {exec_time:.2f} seconds")


if __name__ == "__main__":
    backfill_all_tokens() 