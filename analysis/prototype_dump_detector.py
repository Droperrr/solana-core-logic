import os
import json
import time
from datetime import datetime, timedelta
from collections import defaultdict
import random

# === CONFIG ===
WALLETS_FILE = 'data/wallets.txt'  # по одному адресу на строку
TOKENS_FILE = 'data/tokens.txt'    # по одному mint на строку
HELIUS_API_KEY = os.getenv('HELIUS_API_KEY', None)
HELIUS_ENDPOINT = 'https://api.helius.xyz/v0/addresses/{address}/transactions?api-key={api_key}'

# === MOCK: если нет ключа, используем генератор ===
def mock_helius_transactions(address, token, n=30):
    now = datetime.utcnow()
    txs = []
    for i in range(n):
        ts = now - timedelta(minutes=random.randint(0, 120))
        amount = random.randint(1, 1000)
        direction = random.choice(['in', 'out'])
        txs.append({
            'timestamp': ts.isoformat() + 'Z',
            'token': token,
            'address': address,
            'amount': amount if direction == 'out' else -amount,
            'signature': f'mock_sig_{i}_{address[:4]}'
        })
    return txs

# === LOAD ADDRESSES & TOKENS ===
def load_list(path):
    with open(path, 'r') as f:
        return [line.strip() for line in f if line.strip()]

# === GET TRANSACTIONS ===
def get_transactions(address, token):
    if not HELIUS_API_KEY:
        return mock_helius_transactions(address, token)
    import requests
    url = HELIUS_ENDPOINT.format(address=address, api_key=HELIUS_API_KEY)
    resp = requests.get(url)
    if resp.status_code != 200:
        print(f'Helius error for {address}: {resp.text}')
        return []
    # Здесь нужно распарсить real Helius ответ под нужный формат
    # Для прототипа: возвращаем пусто
    return []

# === AGGREGATE VOLUME BY WINDOW ===
def aggregate_volume(txs, window_minutes=10):
    buckets = defaultdict(list)
    for tx in txs:
        ts = datetime.fromisoformat(tx['timestamp'].replace('Z', ''))
        bucket = ts.replace(second=0, microsecond=0, minute=(ts.minute // window_minutes) * window_minutes)
        buckets[bucket].append(tx)
    return buckets

# === DUMP DETECTOR ===
def detect_dumps(all_txs, token, window_minutes=10, spike_factor=5.0, min_addresses=2):
    # all_txs: list of tx dicts for one token
    # 1. Агрегируем volume по окнам
    buckets = aggregate_volume(all_txs, window_minutes)
    # 2. Считаем средний volume (по абсолюту)
    all_volumes = [abs(tx['amount']) for tx in all_txs]
    avg_volume = sum(all_volumes) / (len(all_volumes) or 1)
    dump_events = []
    for bucket, txs in buckets.items():
        volume = sum(abs(tx['amount']) for tx in txs)
        addresses = set(tx['address'] for tx in txs)
        if volume > spike_factor * avg_volume and len(addresses) >= min_addresses:
            dump_events.append({
                'timestamp': bucket.isoformat() + 'Z',
                'token': token,
                'addresses': list(addresses),
                'volume_before': avg_volume,
                'volume_after': volume,
                'anomaly_type': ['volume_spike', 'wallet_sync'],
                'details': {
                    'window_minutes': window_minutes,
                    'spike_factor': round(volume / (avg_volume or 1), 2)
                }
            })
    return dump_events

# === MAIN ===
def main():
    wallets = load_list(WALLETS_FILE)
    tokens = load_list(TOKENS_FILE)
    report = {'dump_events': []}
    for token in tokens:
        all_txs = []
        for wallet in wallets:
            txs = get_transactions(wallet, token)
            all_txs.extend([tx for tx in txs if tx['token'] == token])
        dump_events = detect_dumps(all_txs, token)
        report['dump_events'].extend(dump_events)
    print(json.dumps(report, indent=2, ensure_ascii=False))

if __name__ == '__main__':
    main() 