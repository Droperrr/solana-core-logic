import logging
import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import db.db_manager as dbm

CONSOLIDATE_SQL = """
-- –®–∞–≥ 1: –î–æ–±–∞–≤–ª—è–µ–º –Ω–µ–¥–æ—Å—Ç–∞—é—â–∏–µ –∫–æ–ª–æ–Ω–∫–∏ –≤ —Ü–µ–ª–µ–≤—É—é —Ç–∞–±–ª–∏—Ü—É token_metadata
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS supply NUMERIC;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS mint_authority TEXT;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS update_authority TEXT;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS freeze_authority TEXT;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS is_mutable BOOLEAN;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS token_standard TEXT;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS description TEXT;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS website TEXT;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS twitter TEXT;
ALTER TABLE token_metadata ADD COLUMN IF NOT EXISTS discord TEXT;

-- –®–∞–≥ 2: –ü–µ—Ä–µ–Ω–æ—Å–∏–º –¥–∞–Ω–Ω—ã–µ –∏–∑ token_asset_info –≤ token_metadata, –æ–±–Ω–æ–≤–ª—è—è —Å—É—â–µ—Å—Ç–≤—É—é—â–∏–µ –∑–∞–ø–∏—Å–∏
INSERT INTO token_metadata (
    mint_address, symbol, name, decimals, logo_uri, supply, mint_authority, update_authority, freeze_authority, is_mutable, token_standard, description, website, twitter, discord, last_updated_at
)
SELECT 
    mint, symbol, name, decimals, NULL, supply, mint_authority, update_authority, freeze_authority, is_mutable, token_standard, description, website, twitter, discord, NOW() AT TIME ZONE 'utc'
FROM token_asset_info
ON CONFLICT (mint_address) DO UPDATE SET
    symbol = COALESCE(EXCLUDED.symbol, token_metadata.symbol),
    name = COALESCE(EXCLUDED.name, token_metadata.name),
    decimals = COALESCE(EXCLUDED.decimals, token_metadata.decimals),
    supply = COALESCE(EXCLUDED.supply, token_metadata.supply),
    mint_authority = COALESCE(EXCLUDED.mint_authority, token_metadata.mint_authority),
    update_authority = COALESCE(EXCLUDED.update_authority, token_metadata.update_authority),
    freeze_authority = COALESCE(EXCLUDED.freeze_authority, token_metadata.freeze_authority),
    is_mutable = COALESCE(EXCLUDED.is_mutable, token_metadata.is_mutable),
    token_standard = COALESCE(EXCLUDED.token_standard, token_metadata.token_standard),
    description = COALESCE(EXCLUDED.description, token_metadata.description),
    website = COALESCE(EXCLUDED.website, token_metadata.website),
    twitter = COALESCE(EXCLUDED.twitter, token_metadata.twitter),
    discord = COALESCE(EXCLUDED.discord, token_metadata.discord),
    last_updated_at = NOW() AT TIME ZONE 'utc';

-- –®–∞–≥ 3: –£–¥–∞–ª—è–µ–º —Å—Ç–∞—Ä—É—é —Ç–∞–±–ª–∏—Ü—É
DROP TABLE IF EXISTS token_asset_info;
"""

def main():
    logging.info("--- –ú–∏–≥—Ä–∞—Ü–∏—è 007: –û–±—ä–µ–¥–∏–Ω–µ–Ω–∏–µ –º–µ—Ç–∞–¥–∞–Ω–Ω—ã—Ö —Ç–æ–∫–µ–Ω–æ–≤ ---")
    conn = None
    try:
        conn = dbm.get_connection()
        with conn.cursor() as cursor:
            cursor.execute(CONSOLIDATE_SQL)
        conn.commit()
        logging.info("‚úÖ –ú–∏–≥—Ä–∞—Ü–∏—è 007 —É—Å–ø–µ—à–Ω–æ –∑–∞–≤–µ—Ä—à–µ–Ω–∞.")
    except Exception as e:
        logging.error(f"üí• –û–®–ò–ë–ö–ê: –ú–∏–≥—Ä–∞—Ü–∏—è 007 –Ω–µ —É–¥–∞–ª–∞—Å—å. {e}", exc_info=True)
        if conn: conn.rollback()
    finally:
        if conn: dbm.release_connection(conn)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    dbm.initialize_engine()
    main()
    dbm.close_engine() 