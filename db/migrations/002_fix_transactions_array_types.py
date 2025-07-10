import os
import sys
import psycopg2
import logging

# –î–æ–±–∞–≤–ª—è–µ–º –∫–æ—Ä–Ω–µ–≤—É—é –¥–∏—Ä–µ–∫—Ç–æ—Ä–∏—é –≤ –ø—É—Ç—å
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import db.db_manager as dbm

ALTER_TABLE_SQL = """
ALTER TABLE transactions
    ALTER COLUMN detected_patterns TYPE text[] USING detected_patterns::text[],
    ALTER COLUMN involved_platforms TYPE text[] USING involved_platforms::text[];
"""

def main():
    logging.info("--- –ó–∞–ø—É—Å–∫ –º–∏–≥—Ä–∞—Ü–∏–∏: 002_fix_transactions_array_types ---")
    conn = None
    try:
        conn = dbm.get_connection()
        if not conn:
            raise Exception("–ù–µ —É–¥–∞–ª–æ—Å—å –ø–æ–ª—É—á–∏—Ç—å —Å–æ–µ–¥–∏–Ω–µ–Ω–∏–µ —Å –ë–î.")
        with conn.cursor() as cursor:
            logging.info("–í—ã–ø–æ–ª–Ω–µ–Ω–∏–µ ALTER TABLE –¥–ª—è 'transactions'...")
            cursor.execute(ALTER_TABLE_SQL)
            logging.info("–¢–∏–ø—ã —Å—Ç–æ–ª–±—Ü–æ–≤ 'detected_patterns' –∏ 'involved_platforms' —É—Å–ø–µ—à–Ω–æ –∏–∑–º–µ–Ω–µ–Ω—ã –Ω–∞ text[].")
        conn.commit()
        logging.info("‚úÖ –ú–∏–≥—Ä–∞—Ü–∏—è 002 —É—Å–ø–µ—à–Ω–æ –∑–∞–≤–µ—Ä—à–µ–Ω–∞.")
    except Exception as e:
        logging.error(f"üí• –û–®–ò–ë–ö–ê: –ú–∏–≥—Ä–∞—Ü–∏—è 002 –Ω–µ —É–¥–∞–ª–∞—Å—å. {e}", exc_info=True)
        if conn:
            conn.rollback()
    finally:
        if conn:
            dbm.release_connection(conn)

if __name__ == "__main__":
    dbm.initialize_connection_pool()
    main()
    dbm.close_connection_pool() 