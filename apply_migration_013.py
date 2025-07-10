#!/usr/bin/env python3
"""
Apply migration 013: Add token_collection_progress table
"""

import sys
import os
import sqlite3
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db.migrations.migration_013_add_token_collection_progress import migrate, rollback
from db.db_manager import get_connection

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def main():
    """Apply migration 013"""
    
    if len(sys.argv) > 1 and sys.argv[1] == '--rollback':
        logger.info("Rolling back migration 013")
        conn = get_connection()
        try:
            rollback(conn)
        finally:
            conn.close()
        logger.info("Migration 013 rollback completed")
        return
    
    logger.info("Applying migration 013")
    conn = get_connection()
    try:
        migrate(conn)
    finally:
        conn.close()
    logger.info("Migration 013 applied successfully")

if __name__ == "__main__":
    main() 