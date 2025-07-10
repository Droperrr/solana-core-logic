"""
Миграция: добавление колонки shadow_enriched_data для Shadow Mode.
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

SQL = '''
ALTER TABLE transactions ADD COLUMN IF NOT EXISTS shadow_enriched_data JSONB NULL;
COMMENT ON COLUMN transactions.shadow_enriched_data IS 'Результат работы нового event-based пайплайна в Shadow Mode';
'''

def run_migration(conn):
    with conn.cursor() as cur:
        cur.execute(SQL)
        conn.commit()

def main():
    import os
    conn = psycopg2.connect(
        dbname=os.getenv("DB_NAME", "solana_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "Droper80moper!"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432")
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    run_migration(conn)
    logging.info("Migration 004_add_shadow_mode_column applied successfully.")

if __name__ == "__main__":
    main() 