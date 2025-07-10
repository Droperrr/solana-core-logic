import sys
import os
# Добавляем корневую директорию проекта в sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

print('START')
import argparse
import logging
import sqlite3
import json
from pathlib import Path
from typing import List
from rpc.client import RPCClient
from process_raw_dump import process_dump_file

DB_PATH = os.path.join(os.path.dirname(__file__), '../db/solana_db.sqlite')
TEMP_RAW_FILE = 'tmp_token_raw.jsonl'
TEMP_ENRICHED_FILE = 'tmp_token_enriched.jsonl'

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s', stream=sys.stdout)
logger = logging.getLogger("force_reload_token")

def delete_token_data(token_address: str):
    """Удаляет все записи по токену из transactions и ml_ready_events."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ml_ready_events WHERE token_a_mint = ? OR token_b_mint = ?", (token_address, token_address))
    ml_count = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM transactions WHERE source_query_address = ?", (token_address,))
    tx_count = cursor.fetchone()[0]
    logger.info(f"Удаляю {ml_count} записей из ml_ready_events и {tx_count} из transactions для токена {token_address}")
    cursor.execute("DELETE FROM ml_ready_events WHERE token_a_mint = ? OR token_b_mint = ?", (token_address, token_address))
    cursor.execute("DELETE FROM transactions WHERE source_query_address = ?", (token_address,))
    conn.commit()
    conn.close()
    logger.info("Удаление завершено.")

def fetch_all_signatures(client: RPCClient, token_address: str) -> List[str]:
    """Получает все сигнатуры для токена через RPC (без лимита)."""
    logger.info(f"Получаю все сигнатуры для токена {token_address} через RPC...")
    signatures = []
    before = None
    while True:
        batch = client.get_signatures_for_address(token_address, before=before, limit=1000)
        if not batch:
            break
        batch_sigs = [x['signature'] for x in batch if 'signature' in x]
        signatures.extend(batch_sigs)
        logger.info(f"Загружено сигнатур: {len(signatures)}")
        if len(batch_sigs) < 1000:
            break
        before = batch_sigs[-1]
    logger.info(f"Итого сигнатур: {len(signatures)}")
    return signatures

def fetch_raw_jsons(client: RPCClient, signatures: List[str]) -> List[dict]:
    """Получает raw_json для каждой сигнатуры."""
    logger.info(f"Получаю raw_json для {len(signatures)} транзакций...")
    raw_txs = []
    for i, sig in enumerate(signatures):
        tx = client.get_transaction(sig)
        if tx:
            tx['signature'] = sig
            raw_txs.append(tx)
        if (i+1) % 100 == 0:
            logger.info(f"Загружено {i+1} транзакций")
    logger.info(f"Итого получено raw_json: {len(raw_txs)}")
    return raw_txs

def save_jsonl(objs: List[dict], path: str):
    with open(path, 'w', encoding='utf-8') as f:
        for obj in objs:
            f.write(json.dumps(obj, ensure_ascii=False) + '\n')

def load_jsonl(path: str) -> List[dict]:
    with open(path, 'r', encoding='utf-8') as f:
        return [json.loads(line) for line in f if line.strip()]

def insert_to_db(token_address: str, enriched: List[dict]):
    """Вставляет обработанные транзакции в БД."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    tx_inserted = 0
    ml_inserted = 0
    for item in enriched:
        sig = item.get('signature')
        block_time = item.get('block_time')
        enriched_data = json.dumps(item.get('enriched', {}), ensure_ascii=False)
        # Вставка в transactions
        cursor.execute("""
            INSERT OR REPLACE INTO transactions (signature, block_time, source_query_address, raw_json, enriched_data)
            VALUES (?, ?, ?, ?, ?)
        """, (sig, block_time, token_address, json.dumps(item, ensure_ascii=False), enriched_data))
        tx_inserted += 1
        # Вставка SWAP-событий в ml_ready_events (если есть)
        if isinstance(item.get('enriched'), list):
            for event in item['enriched']:
                if event.get('event_type') == 'SWAP':
                    cursor.execute("""
                        INSERT INTO ml_ready_events (signature, block_time, event_type, token_a_mint, token_b_mint, from_wallet, to_wallet, from_amount, to_amount, wallet_tag, program_id, instruction_name, event_data_raw)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        sig,
                        block_time,
                        event.get('event_type'),
                        event.get('token_a_mint'),
                        event.get('token_b_mint'),
                        event.get('from_wallet'),
                        event.get('to_wallet'),
                        event.get('from_amount'),
                        event.get('to_amount'),
                        event.get('wallet_tag'),
                        event.get('program_id'),
                        event.get('instruction_name'),
                        json.dumps(event, ensure_ascii=False)
                    ))
                    ml_inserted += 1
    conn.commit()
    conn.close()
    logger.info(f"Вставлено {tx_inserted} транзакций в transactions и {ml_inserted} SWAP-событий в ml_ready_events.")

def count_swaps(token_address: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM ml_ready_events WHERE (token_a_mint = ? OR token_b_mint = ?) AND event_type = 'SWAP'", (token_address, token_address))
    count = cursor.fetchone()[0]
    conn.close()
    return count

def main():
    parser = argparse.ArgumentParser(description="Force reload all data for a token.")
    parser.add_argument("--token_address", required=True, help="Token mint address")
    args = parser.parse_args()

    delete_token_data(args.token_address)
    client = RPCClient()
    signatures = fetch_all_signatures(client, args.token_address)
    raw_txs = fetch_raw_jsons(client, signatures)
    save_jsonl(raw_txs, TEMP_RAW_FILE)
    logger.info("Запускаю enrichment пайплайн...")
    process_dump_file(TEMP_RAW_FILE, TEMP_ENRICHED_FILE, workers=4)
    logger.info("Enrichment завершён. Загружаю в БД...")
    enriched = load_jsonl(TEMP_ENRICHED_FILE)
    insert_to_db(args.token_address, enriched)
    swap_count = count_swaps(args.token_address)
    logger.info(f"SWAP-событий для токена {args.token_address}: {swap_count}")
    os.remove(TEMP_RAW_FILE)
    os.remove(TEMP_ENRICHED_FILE)
    logger.info("Переобработка завершена.")

if __name__ == "__main__":
    main() 