import os
import sys
import logging

# –î–æ–±–∞–≤–ª—è–µ–º –∫–æ—Ä–µ–Ω—å –ø—Ä–æ–µ–∫—Ç–∞ –≤ sys.path –¥–ª—è –∏–º–ø–æ—Ä—Ç–∞ db_manager
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import db.db_manager as dbm

# –í–µ—Å—å SQL –∏–∑ —Å—Ç–∞—Ä–æ–≥–æ .sql —Ñ–∞–π–ª–∞ —Ç–µ–ø–µ—Ä—å –Ω–∞—Ö–æ–¥–∏—Ç—Å—è –∑–¥–µ—Å—å, –≤–Ω—É—Ç—Ä–∏ Python
REFACTOR_SQL = """
-- –£–¥–∞–ª—è–µ–º —Å—Ç–∞—Ä—ã–π, –Ω–µ–æ–ø—Ç–∏–º–∞–ª—å–Ω—ã–π –∏–Ω–¥–µ–∫—Å, –µ—Å–ª–∏ –æ–Ω —Å—É—â–µ—Å—Ç–≤—É–µ—Ç
DROP INDEX IF EXISTS idx_wallet_links_wallets;

-- –°–æ–∑–¥–∞–µ–º –Ω–æ–≤—ã–π, –ø–æ–∫—Ä—ã–≤–∞—é—â–∏–π –∏–Ω–¥–µ–∫—Å –¥–ª—è –±–æ–ª–µ–µ –±—ã—Å—Ç—Ä—ã—Ö –∑–∞–ø—Ä–æ—Å–æ–≤ –ø–æ –æ–±–æ–∏–º –∫–æ—à–µ–ª—å–∫–∞–º
CREATE INDEX IF NOT EXISTS idx_wallet_links_wallets_combined ON wallet_links (wallet_a, wallet_b);

-- –°–æ–∑–¥–∞–µ–º —Ç–∞–±–ª–∏—Ü—É –¥–ª—è —Ö—Ä–∞–Ω–µ–Ω–∏—è —Å–æ–±—ã—Ç–∏–π, —Å–≤—è–∑–∞–Ω–Ω—ã—Ö —Å–æ —Å–≤—è–∑—è–º–∏
CREATE TABLE IF NOT EXISTS link_events (
    id SERIAL PRIMARY KEY,
    tx_signature TEXT,
    instruction_index INTEGER,
    wallet_a TEXT,
    wallet_b TEXT,
    link_type_classified TEXT,
    confidence_score FLOAT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc')
);
CREATE INDEX IF NOT EXISTS idx_link_events_tx_sig ON link_events(tx_signature);

-- –î–æ–±–∞–≤–ª—è–µ–º –∫–æ–ª–æ–Ω–∫—É –¥–ª—è —Ö—Ä–∞–Ω–µ–Ω–∏—è –¥–æ–ø–æ–ª–Ω–∏—Ç–µ–ª—å–Ω–æ–≥–æ JSON-–∫–æ–Ω—Ç–µ–∫—Å—Ç–∞
ALTER TABLE wallet_links ADD COLUMN IF NOT EXISTS context JSONB;
"""

def main():
    """–û—Å–Ω–æ–≤–Ω–∞—è —Ñ—É–Ω–∫—Ü–∏—è –¥–ª—è –≤—ã–ø–æ–ª–Ω–µ–Ω–∏—è –º–∏–≥—Ä–∞—Ü–∏–∏."""
    logging.info("--- –ü—Ä–∏–º–µ–Ω–µ–Ω–∏–µ –º–∏–≥—Ä–∞—Ü–∏–∏ 006: –†–µ—Ñ–∞–∫—Ç–æ—Ä–∏–Ω–≥ wallet_links –∏ —Å–æ–∑–¥–∞–Ω–∏–µ link_events ---")
    conn = None
    try:
        conn = dbm.get_connection()
        if not conn:
            raise ConnectionError("–ù–µ —É–¥–∞–ª–æ—Å—å –ø–æ–ª—É—á–∏—Ç—å —Å–æ–µ–¥–∏–Ω–µ–Ω–∏–µ —Å –ë–î.")

        with conn.cursor() as cursor:
            logging.info("–í—ã–ø–æ–ª–Ω–µ–Ω–∏–µ SQL –¥–ª—è —Ä–µ—Ñ–∞–∫—Ç–æ—Ä–∏–Ω–≥–∞ wallet_links...")
            cursor.execute(REFACTOR_SQL)
            logging.info("SQL-–∫–æ–º–∞–Ω–¥—ã —É—Å–ø–µ—à–Ω–æ –≤—ã–ø–æ–ª–Ω–µ–Ω—ã.")

        conn.commit()
        logging.info("‚úÖ –ú–∏–≥—Ä–∞—Ü–∏—è 006 —É—Å–ø–µ—à–Ω–æ –∑–∞–≤–µ—Ä—à–µ–Ω–∞ –∏ –∑–∞–∫–æ–º–º–∏—á–µ–Ω–∞.")

    except Exception as e:
        logging.error(f"üí• –û–®–ò–ë–ö–ê: –ú–∏–≥—Ä–∞—Ü–∏—è 006 –Ω–µ —É–¥–∞–ª–∞—Å—å. {e}", exc_info=True)
        if conn:
            logging.warning("–û—Ç–∫–∞—Ç —Ç—Ä–∞–Ω–∑–∞–∫—Ü–∏–∏...")
            conn.rollback()
    finally:
        if conn:
            dbm.release_connection(conn)

# –≠—Ç–æ—Ç –±–ª–æ–∫ –ø–æ–∑–≤–æ–ª—è–µ—Ç –∑–∞–ø—É—Å–∫–∞—Ç—å –º–∏–≥—Ä–∞—Ü–∏—é –∫–∞–∫ –æ—Ç–¥–µ–ª—å–Ω—ã–π —Å–∫—Ä–∏–ø—Ç –¥–ª—è –æ—Ç–ª–∞–¥–∫–∏,
# –Ω–æ –æ–Ω–∞ —Ç–∞–∫–∂–µ –±—É–¥–µ—Ç —Ä–∞–±–æ—Ç–∞—Ç—å –ø—Ä–∏ –∏–º–ø–æ—Ä—Ç–µ —á–µ—Ä–µ–∑ apply_all_migrations.py, –µ—Å–ª–∏ –æ–Ω –≤—ã–∑—ã–≤–∞–µ—Ç main().
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # –£–±–µ–¥–∏–º—Å—è, —á—Ç–æ db_manager –∏–Ω–∏—Ü–∏–∞–ª–∏–∑–∏—Ä–æ–≤–∞–Ω
    dbm.initialize_engine()
    main()
    dbm.close_engine() 