import psycopg2
from processing.transaction_processor import process_single_transaction
from dataclasses import asdict, is_dataclass
import json

DB_HOST = "localhost"
DB_PORT = 5432
DB_NAME = "solana_db"
DB_USER = "postgres"
DB_PASSWORD = "Droper80moper!"

# 1. Найти сигнатуры транзакций с enrich_errors
FIND_FAILED_SQL = '''
SELECT signature, transaction_type
FROM transactions
WHERE additional_context ? 'enrich_errors'
LIMIT 100;
'''

# 2. Обновить enrichment-контекст в базе
UPDATE_SQL = '''
UPDATE transactions
SET additional_context = %s
WHERE signature = %s;
'''

def to_serializable(obj):
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [to_serializable(v) for v in obj]
    return obj

if __name__ == "__main__":
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute(FIND_FAILED_SQL)
    rows = cur.fetchall()
    print(f"Найдено {len(rows)} транзакций с enrich_errors для повторного enrichment.")
    for signature, tx_type in rows:
        print(f"Повторный enrichment для: {signature} ({tx_type}) ...")
        result = process_single_transaction(signature, None)
        if not result:
            print(f"[ERROR] enrichment не удался для {signature}")
            continue
        additional_context = result[-1]
        additional_context = to_serializable(additional_context)
        cur.execute(UPDATE_SQL, (json.dumps(additional_context), signature))
        print(f"[OK] enrichment обновлён для {signature}")
    conn.commit()
    cur.close()
    conn.close()
    print("Готово.") 