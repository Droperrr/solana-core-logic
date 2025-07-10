#!/usr/bin/env python3
"""
Script to find a transaction for AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC token
"""

import sqlite3
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def find_al2h_transaction():
    """Find any transaction for AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC token"""
    
    db_path = "db/solana_db.sqlite"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # First, check if we have any transactions for this token
        count_query = """
        SELECT COUNT(*) 
        FROM transactions 
        WHERE source_query_address = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC';
        """
        
        cursor.execute(count_query)
        count = cursor.fetchone()[0]
        logger.info(f"Total transactions for AL2H token: {count}")
        
        if count == 0:
            logger.error("No transactions found for AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC")
            return None
        
        # Query for any transaction for AL2H token
        query = """
        SELECT signature, block_time, enriched_data 
        FROM transactions 
        WHERE source_query_address = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC' 
        ORDER BY block_time DESC 
        LIMIT 1;
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            signature, block_time, enriched_data = result
            logger.info(f"Found transaction:")
            logger.info(f"Signature: {signature}")
            logger.info(f"Block time: {block_time}")
            
            # Parse enriched data to show event details
            try:
                enriched = json.loads(enriched_data)
                if 'events' in enriched:
                    logger.info(f"Events found: {len(enriched['events'])}")
                    for i, event in enumerate(enriched['events']):
                        logger.info(f"Event {i}: {event.get('event_type', 'UNKNOWN')}")
            except json.JSONDecodeError:
                logger.warning("Could not parse enriched_data JSON")
            
            return signature
        else:
            logger.error("No transactions found for AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC")
            return None
            
    except Exception as e:
        logger.error(f"Database error: {e}")
        return None
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    signature = find_al2h_transaction()
    if signature:
        print(f"\n✅ Found transaction signature: {signature}")
        print("Use this signature in debug_price_engine.py")
    else:
        print("\n❌ No suitable transaction found") 