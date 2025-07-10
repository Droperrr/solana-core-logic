import os
import sys
import logging

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import db.db_manager as dbm

CLEANUP_SQL = """
-- –£–¥–∞–ª—è–µ–º –∫–æ–ª–æ–Ω–∫–∏, –∫–æ—Ç–æ—Ä—ã–µ –±–æ–ª—å—à–µ –Ω–µ –Ω—É–∂–Ω—ã –≤ event-driven –º–æ–¥–µ–ª–∏.
-- –í—Å—è —ç—Ç–∞ –∏–Ω—Ñ–æ—Ä–º–∞—Ü–∏—è —Ç–µ–ø–µ—Ä—å —è–≤–ª—è–µ—Ç—Å—è —á–∞—Å—Ç—å—é EnrichedEvent –≤ transactions.enriched_data
ALTER TABLE instructions
    DROP COLUMN IF EXISTS raw,
    DROP COLUMN IF EXISTS is_parsed_by_rpc,
    DROP COLUMN IF EXISTS is_parsed_manually,
    DROP COLUMN IF EXISTS details,
    DROP COLUMN IF EXISTS error_parsing,
    DROP COLUMN IF EXISTS amm_id,
    DROP COLUMN IF EXISTS pool_address,
    DROP COLUMN IF EXISTS dex_id,
    DROP COLUMN IF EXISTS pool_type;
"""

def main():
    logging.info("--- –ü—Ä–∏–º–µ–Ω–µ–Ω–∏–µ –º–∏–≥—Ä–∞—Ü–∏–∏ 008: –û—á–∏—Å—Ç–∫–∞ —Ç–∞–±–ª–∏—Ü—ã instructions ---")
    conn = None
    try:
        conn = dbm.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(CLEANUP_SQL)
        conn.commit()
        logging.info("‚úÖ –ú–∏–≥—Ä–∞—Ü–∏—è 008 —É—Å–ø–µ—à–Ω–æ –∑–∞–≤–µ—Ä—à–µ–Ω–∞.")
    except Exception as e:
        logging.error(f"üí• –û–®–ò–ë–ö–ê: –ú–∏–≥—Ä–∞—Ü–∏—è 008 –Ω–µ —É–¥–∞–ª–∞—Å—å. {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: dbm.release_connection(conn)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dbm.initialize_engine()
    main()
    dbm.close_engine() 