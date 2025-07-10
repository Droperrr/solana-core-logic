import sqlite3
import sys

def upgrade(conn):
    cursor = conn.cursor()

    # 0. Создаем таблицу transactions, если её нет
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signature TEXT NOT NULL UNIQUE,
            block_time INTEGER,
            slot INTEGER,
            fee_payer TEXT,
            transaction_type TEXT,
            source_query_type TEXT,
            source_query_address TEXT,
            raw_json TEXT,
            enriched_data TEXT,
            parser_version TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # 1. Создаем новую таблицу token_lifecycle, если не существует
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS token_lifecycle (
            token_address TEXT PRIMARY KEY,
            creation_signature TEXT,
            creation_time INTEGER,
            first_dump_signature TEXT,
            first_dump_time INTEGER,
            first_dump_price_drop_percent REAL,
            last_processed_signature TEXT
        );
    """)

    # 2. Добавляем новый столбец on_chain_price_sol в transactions, если его еще нет
    cursor.execute("PRAGMA table_info(transactions);")
    columns = [row[1] for row in cursor.fetchall()]
    if 'on_chain_price_sol' not in columns:
        cursor.execute("ALTER TABLE transactions ADD COLUMN on_chain_price_sol REAL;")
        print("Столбец on_chain_price_sol успешно добавлен.")
    else:
        print("Столбец on_chain_price_sol уже существует.")

    conn.commit()

if __name__ == "__main__":
    # Use command line argument if provided, otherwise default
    db_path = sys.argv[1] if len(sys.argv) > 1 else "solana_db.sqlite"
    print(f"Applying migration to: {db_path}")
    conn = sqlite3.connect(db_path)
    upgrade(conn)
    conn.close() 