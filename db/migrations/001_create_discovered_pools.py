import os
import sys
import psycopg2
import logging

# –î–æ–±–∞–≤–ª—è–µ–º –∫–æ—Ä–Ω–µ–≤—É—é –¥–∏—Ä–µ–∫—Ç–æ—Ä–∏—é –≤ –ø—É—Ç—å, —á—Ç–æ–±—ã –∏–º–ø–æ—Ä—Ç–∏—Ä–æ–≤–∞—Ç—å config –∏ db_manager
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import db.db_manager as dbm
import config.config

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS discovered_pools (
    pool_address VARCHAR(44) NOT NULL,
    token_mint_address VARCHAR(44) NOT NULL,
    dex_id VARCHAR(50),
    pool_type VARCHAR(30),
    first_seen_signature TEXT,
    last_seen_signature TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (pool_address, token_mint_address)
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_discovered_pools_dex_id ON discovered_pools(dex_id);
"""

CREATE_DUMP_OPERATIONS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dump_operations (
    id VARCHAR(64) PRIMARY KEY,
    token_id VARCHAR(64) NOT NULL,
    status VARCHAR(32) NOT NULL,
    started_at DOUBLE PRECISION NOT NULL,
    finished_at DOUBLE PRECISION,
    progress DOUBLE PRECISION DEFAULT 0.0,
    stage TEXT,
    result TEXT,
    error_message TEXT,
    progress_percent DOUBLE PRECISION DEFAULT 0.0,
    stage_description TEXT
);
"""

CREATE_OPERATION_LOGS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS operation_logs (
    id SERIAL PRIMARY KEY,
    operation_id VARCHAR(64) NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    level VARCHAR(16) NOT NULL,
    message TEXT NOT NULL
);
"""

def main():
    logging.info("--- –ó–∞–ø—É—Å–∫ –º–∏–≥—Ä–∞—Ü–∏–∏: 001_create_discovered_pools ---")
    conn = None
    try:
        conn = dbm.get_connection()
        if not conn:
            raise Exception("–ù–µ —É–¥–∞–ª–æ—Å—å –ø–æ–ª—É—á–∏—Ç—å —Å–æ–µ–¥–∏–Ω–µ–Ω–∏–µ —Å –ë–î.")
        cursor = conn.cursor()
        logging.info("–í—ã–ø–æ–ª–Ω–µ–Ω–∏–µ CREATE TABLE IF NOT EXISTS –¥–ª—è 'discovered_pools'...")
        cursor.execute(CREATE_TABLE_SQL)
        logging.info("–í—ã–ø–æ–ª–Ω–µ–Ω–∏–µ CREATE INDEX IF NOT EXISTS –¥–ª—è 'discovered_pools'...")
        cursor.execute(CREATE_INDEX_SQL)
        logging.info("Создание таблицы dump_operations...")
        cursor.execute(CREATE_DUMP_OPERATIONS_TABLE_SQL)
        logging.info("Создание таблицы operation_logs...")
        cursor.execute(CREATE_OPERATION_LOGS_TABLE_SQL)
        cursor.close()
        conn.commit()
        logging.info("‚úÖ –ú–∏–≥—Ä–∞—Ü–∏—è 001 —É—Å–ø–µ—à–Ω–æ –∑–∞–≤–µ—Ä—à–µ–Ω–∞.")
    except Exception as e:
        logging.error(f"üí• –û–®–ò–ë–ö–ê: –ú–∏–≥—Ä–∞—Ü–∏—è 001 –Ω–µ —É–¥–∞–ª–∞—Å—å. {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            dbm.release_connection(conn)

if __name__ == "__main__":
    dbm.initialize_engine()
    main()
    dbm.close_engine() 