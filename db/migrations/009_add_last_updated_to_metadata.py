import os
import sys
import logging

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import db.db_manager as dbm

ADD_COLUMN_SQL = """
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS last_updated_at TIMESTAMPTZ;
"""

def main():
    logging.info("--- –ü—Ä–∏–º–µ–Ω–µ–Ω–∏–µ –º–∏–≥—Ä–∞—Ü–∏–∏ 009: –î–æ–±–∞–≤–ª–µ–Ω–∏–µ last_updated_at –≤ token_metadata ---")
    conn = None
    try:
        conn = dbm.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(ADD_COLUMN_SQL)
        conn.commit()
        logging.info("‚úÖ –ú–∏–≥—Ä–∞—Ü–∏—è 009 —É—Å–ø–µ—à–Ω–æ –∑–∞–≤–µ—Ä—à–µ–Ω–∞.")
    except Exception as e:
        logging.error(f"üí• –û–®–ò–ë–ö–ê: –ú–∏–≥—Ä–∞—Ü–∏—è 009 –Ω–µ —É–¥–∞–ª–∞—Å—å. {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: dbm.release_connection(conn)

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dbm.initialize_engine()
    main()
    dbm.close_engine() 