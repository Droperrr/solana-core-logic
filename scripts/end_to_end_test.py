import sys
import os
import logging
import sqlite3
import json

# Добавляем корневую директорию проекта в sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from decoder.decoder import TransactionDecoder
from analysis.dump_detector import DumpDetector
from db.db_writer import save_events_to_db

# --- НАСТРОЙКА ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
logging.getLogger('services.onchain_price_engine').setLevel(logging.INFO)
logging.getLogger('decoder.enrichers.price_enricher').setLevel(logging.INFO)
logging.getLogger('analysis.dump_detector').setLevel(logging.INFO)

# --- ЦЕЛЕВЫЕ ДАННЫЕ ---
LOCAL_TX_FILE = "tests/fixtures/transactions/raydium_2WjgSSiE.raw.json"
TOKEN_ADDRESS = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzL7GATEkLhbaB"

# --- ПАРАМЕТРЫ ДЕТЕКТОРА ---
DUMP_PARAMS = {
    "price_drop_threshold": 0.05,
    "time_window_seconds": 60 * 5,
    "volume_threshold_sol": 1.0 
}

DB_PATH = "db/solana_db.sqlite"


def init_db():
    """Создает необходимые таблицы в БД, если они не существуют."""
    logging.info("Initializing database...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signature TEXT NOT NULL,
                event_type TEXT NOT NULL,
                source TEXT,
                timestamp INTEGER,
                token_a TEXT,
                token_b TEXT,
                details TEXT,
                enrichment_data TEXT
            );
            """)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_signature ON events (signature);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_token_a ON events (token_a);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_events_token_b ON events (token_b);")
            conn.commit()
            logging.info("Database initialized successfully.")
    except sqlite3.Error as e:
        logging.error(f"Database initialization failed: {e}")
        raise

def cleanup_db_for_token(token_address: str):
    """Очищает таблицу events от записей по конкретному токену."""
    logging.info(f"Cleaning up database for token {token_address}...")
    try:
        with sqlite3.connect(DB_PATH) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM events WHERE token_a = ? OR token_b = ?", (token_address, token_address))
            logging.info(f"Deleted {cursor.rowcount} records from 'events' table.")
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Database cleanup failed: {e}")
        raise

def run_test():
    """Выполняет полный цикл теста: очистка, загрузка, обработка, анализ."""
    init_db()
    cleanup_db_for_token(TOKEN_ADDRESS)

    logging.info(f"Starting data ingestion from local file: {LOCAL_TX_FILE}")
    try:
        with open(LOCAL_TX_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Обработка "слипшихся" JSON
        if '}{' in content:
            corrected_content = content.replace('}{', '}\n{')
            raw_txs = [json.loads(line) for line in corrected_content.split('\n') if line.strip()]
        else:
            # Стандартная обработка (один JSON или JSONL)
            try:
                raw_txs = json.loads(content)
            except json.JSONDecodeError:
                raw_txs = [json.loads(line) for line in content.split('\n') if line.strip()]
        
        if not isinstance(raw_txs, list):
            raw_txs = [raw_txs]

    except Exception as e:
        logging.error(f"Failed to read or parse fixture file {LOCAL_TX_FILE}: {e}")
        return

    tx_decoder = TransactionDecoder()
    all_events = []
    for raw_tx in raw_txs:
        sig = raw_tx.get('transaction', {}).get('signatures', ['N/A'])[0]
        logging.info(f"Processing transaction: {sig}")
        try:
            # Правильный вызов метода
            business_events = tx_decoder.decode_tx(raw_tx)
            if business_events:
                all_events.extend(business_events)
        except Exception as e:
            logging.error(f"Error processing transaction {sig}: {e}", exc_info=True)
    
    if all_events:
        logging.info(f"Processed {len(all_events)} business events. Storing to database...")
        save_events_to_db(all_events)
    else:
        logging.warning("No business events were generated.")
        return

    logging.info("--- Starting Dump Analysis ---")
    detector = DumpDetector(db_path=DB_PATH)
    
    found_dumps = detector.find_dumps_for_token(
        token_address=TOKEN_ADDRESS,
        price_drop_threshold=DUMP_PARAMS['price_drop_threshold'],
        time_window_seconds=DUMP_PARAMS['time_window_seconds'],
        volume_threshold_sol=DUMP_PARAMS['volume_threshold_sol']
    )

    logging.info("--- End-to-End Test Report ---")
    print("\n" + "="*50)
    print("E2E VALIDATION REPORT (FROM LOCAL FIXTURE)")
    print("="*50)
    print(f"Token Analyzed: {TOKEN_ADDRESS}")
    print(f"Fixture File: {LOCAL_TX_FILE}")
    print(f"Detector Parameters: {DUMP_PARAMS}")
    print("-"*50)

    if found_dumps:
        print(f"\n✅ SUCCESS: Found {len(found_dumps)} potential dump(s).")
        for dump in found_dumps:
            print("\n--- DETECTED DUMP EVENT ---")
            print(f"  Start Signature: {dump['start_signature']}")
            print(f"  End Signature:   {dump['end_signature']}")
            print(f"  Time Window:     {dump['start_timestamp']} -> {dump['end_timestamp']} ({dump['duration_seconds']}s)")
            print(f"  Price Drop:      {dump['start_price_sol']:.6f} -> {dump['end_price_sol']:.6f} SOL (-{dump['price_drop_percentage']:.2f}%)")
            print(f"  Total Volume:    {dump['cumulative_volume_sol']:.2f} SOL")
            print("---------------------------\n")
    else:
        print("\n❌ FAILURE: No dumps found for the given criteria.")
    
    print("="*50)
    print("Report Finished.")

if __name__ == "__main__":
    run_test() 