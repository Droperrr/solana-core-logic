#!/usr/bin/env python3
"""
Backfill Missing Transactions Script

This script identifies tokens with incomplete transaction history and performs
a full backfill to ensure 100% data completeness for analysis.
"""

import sys
import os
import argparse
import logging
from typing import List, Set, Tuple
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.db_manager import get_connection, check_existing_signatures
from rpc.client import RPCClient
from scripts.processing_utils import process_transaction_signatures
from utils.signature_handler import fetch_signatures_for_token
import config.config as app_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/backfill_missing_transactions.log')
    ]
)

logger = logging.getLogger(__name__)

class TokenBackfillManager:
    """Manages the backfill process for tokens with incomplete data"""
    
    def __init__(self, rpc_client: RPCClient):
        self.rpc_client = rpc_client
        self.conn = get_connection()
        
    def __del__(self):
        """Cleanup database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
    
    def get_token_transaction_count(self, token_address: str) -> int:
        """Get current transaction count for a token in the database"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) as count 
                FROM transactions 
                WHERE source_query_address = ? AND source_query_type = 'token'
            """, (token_address,))
            result = cursor.fetchone()
            return result['count'] if result else 0
        except Exception as e:
            logger.error(f"Error getting transaction count for {token_address}: {e}")
            return 0
    
    def get_token_progress_status(self, token_address: str) -> dict:
        """Get progress status for a token from token_collection_progress table"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT * FROM token_collection_progress 
                WHERE token_address = ?
            """, (token_address,))
            result = cursor.fetchone()
            return result if result else None
        except Exception as e:
            logger.error(f"Error getting progress status for {token_address}: {e}")
            return None
    
    def update_token_progress(self, token_address: str, on_chain_count: int, 
                            db_count: int, status: str = 'PENDING'):
        """Update or insert token progress information"""
        try:
            completeness_ratio = db_count / on_chain_count if on_chain_count > 0 else 0.0
            
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO token_collection_progress 
                (token_address, on_chain_tx_count, db_tx_count, completeness_ratio, 
                 status, last_checked_at, updated_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """, (token_address, on_chain_count, db_count, completeness_ratio, status))
            self.conn.commit()
            
        except Exception as e:
            logger.error(f"Error updating progress for {token_address}: {e}")
    
    def identify_incomplete_tokens(self, token_list: List[str], threshold: int, 
                                 force: bool = False) -> List[str]:
        """Identify tokens that need backfill based on threshold or force flag"""
        incomplete_tokens = []
        
        logger.info(f"Checking {len(token_list)} tokens for completeness...")
        
        for i, token_address in enumerate(token_list, 1):
            logger.info(f"Checking token {i}/{len(token_list)}: {token_address}")
            
            # Check if token is already marked as complete
            progress = self.get_token_progress_status(token_address)
            if progress and progress['status'] == 'COMPLETE' and progress['completeness_ratio'] >= 0.95:
                if not force:
                    logger.info(f"Token {token_address} already marked as complete (ratio: {progress['completeness_ratio']:.2f})")
                    continue
            
            # Get current transaction count
            current_count = self.get_token_transaction_count(token_address)
            logger.info(f"Current transaction count for {token_address}: {current_count}")
            
            if force or current_count < threshold:
                incomplete_tokens.append(token_address)
                logger.info(f"Token {token_address} marked for backfill (count: {current_count}, threshold: {threshold})")
            else:
                logger.info(f"Token {token_address} has sufficient data (count: {current_count}, threshold: {threshold})")
        
        return incomplete_tokens
    
    def fetch_all_signatures_for_token(self, token_address: str) -> List[str]:
        """Fetch all signatures for a token from oldest to newest"""
        logger.info(f"Fetching all signatures for token {token_address}")
        try:
            signatures_info = fetch_signatures_for_token(
                token_mint_address=token_address,
                fetch_limit_per_call=1000,
                total_tx_limit=None,
                direction='b'
            )
            signatures = [sig['signature'] for sig in signatures_info] if signatures_info else []
            logger.info(f"Total signatures fetched for {token_address}: {len(signatures)}")
            return signatures
        except Exception as e:
            logger.error(f"Error fetching signatures for {token_address}: {e}")
            return []
    
    def filter_new_signatures(self, all_signatures: List[str]) -> List[str]:
        """Filter out signatures that already exist in the database"""
        if not all_signatures:
            return []
        
        logger.info(f"Checking {len(all_signatures)} signatures against database...")
        existing_signatures = check_existing_signatures(self.conn, all_signatures)
        new_signatures = [sig for sig in all_signatures if sig not in existing_signatures]
        
        logger.info(f"Found {len(existing_signatures)} existing signatures, {len(new_signatures)} new signatures")
        return new_signatures
    
    def backfill_token(self, token_address: str) -> Tuple[int, int]:
        """Perform full backfill for a single token"""
        logger.info(f"Starting backfill for token {token_address}")
        
        # Fetch all signatures from blockchain
        all_signatures = self.fetch_all_signatures_for_token(token_address)
        if not all_signatures:
            logger.warning(f"No signatures found for token {token_address}")
            return 0, 0
        
        # Filter out existing signatures
        new_signatures = self.filter_new_signatures(all_signatures)
        if not new_signatures:
            logger.info(f"No new signatures to process for token {token_address}")
            return len(all_signatures), 0
        
        # Process new signatures
        logger.info(f"Processing {len(new_signatures)} new signatures for token {token_address}")
        processed_count = process_transaction_signatures(
            signatures=new_signatures,
            rpc_client=self.rpc_client,
            source_query_type='token',
            source_query_address=token_address,
            conn=self.conn
        )
        
        # Update progress
        final_db_count = self.get_token_transaction_count(token_address)
        status = 'COMPLETE' if len(all_signatures) == final_db_count else 'PARTIAL'
        self.update_token_progress(token_address, len(all_signatures), final_db_count, status)
        
        logger.info(f"Backfill completed for {token_address}: {processed_count} new transactions processed")
        return len(all_signatures), processed_count
    
    def run_backfill(self, token_list: List[str], threshold: int, force: bool = False) -> dict:
        """Run the complete backfill process"""
        logger.info(f"Starting backfill process for {len(token_list)} tokens")
        logger.info(f"Threshold: {threshold}, Force: {force}")
        
        # Identify incomplete tokens
        incomplete_tokens = self.identify_incomplete_tokens(token_list, threshold, force)
        
        if not incomplete_tokens:
            logger.info("No tokens require backfill")
            return {
                'total_tokens_checked': len(token_list),
                'tokens_backfilled': 0,
                'total_new_transactions': 0
            }
        
        logger.info(f"Found {len(incomplete_tokens)} tokens requiring backfill")
        
        # Perform backfill for each incomplete token
        total_new_transactions = 0
        
        for i, token_address in enumerate(incomplete_tokens, 1):
            logger.info(f"Processing token {i}/{len(incomplete_tokens)}: {token_address}")
            
            try:
                on_chain_count, new_count = self.backfill_token(token_address)
                total_new_transactions += new_count
                
                logger.info(f"Token {token_address} completed: {new_count} new transactions")
                
            except Exception as e:
                logger.error(f"Error processing token {token_address}: {e}")
                continue
        
        # Final report
        report = {
            'total_tokens_checked': len(token_list),
            'tokens_backfilled': len(incomplete_tokens),
            'total_new_transactions': total_new_transactions
        }
        
        logger.info("Backfill process completed")
        logger.info(f"Final report: {report}")
        
        return report

def load_token_list(token_file: str) -> List[str]:
    """Load token addresses from file"""
    try:
        with open(token_file, 'r') as f:
            tokens = [line.strip() for line in f if line.strip()]
        logger.info(f"Loaded {len(tokens)} tokens from {token_file}")
        return tokens
    except Exception as e:
        logger.error(f"Error loading token list from {token_file}: {e}")
        return []

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(description='Backfill missing transactions for tokens')
    parser.add_argument('--token-file', default='tokens.txt', 
                       help='Path to file with token addresses (default: tokens.txt)')
    parser.add_argument('--token', type=str,
                       help='Process only one specific token address')
    parser.add_argument('--threshold', type=int, default=500,
                       help='Threshold below which tokens are considered incomplete (default: 500)')
    parser.add_argument('--force', action='store_true',
                       help='Force backfill for all tokens, ignoring threshold')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without actually processing')
    
    args = parser.parse_args()
    
    # Validate token file exists
    if not os.path.exists(args.token_file):
        logger.error(f"Token file not found: {args.token_file}")
        sys.exit(1)
    
    # Load token list
    if args.token:
        token_list = [args.token]
        logger.info(f"Processing single token: {args.token}")
    else:
        token_list = load_token_list(args.token_file)
        if not token_list:
            logger.error("No tokens loaded from file")
            sys.exit(1)
    
    # Initialize RPC client
    try:
        rpc_client = RPCClient()
    except Exception as e:
        logger.error(f"Failed to initialize RPC client: {e}")
        sys.exit(1)
    
    # Initialize backfill manager
    manager = TokenBackfillManager(rpc_client)
    
    if args.dry_run:
        logger.info("DRY RUN MODE - No actual processing will be performed")
        incomplete_tokens = manager.identify_incomplete_tokens(token_list, args.threshold, args.force)
        logger.info(f"Would backfill {len(incomplete_tokens)} tokens:")
        for token in incomplete_tokens:
            current_count = manager.get_token_transaction_count(token)
            logger.info(f"  {token} (current: {current_count}, threshold: {args.threshold})")
    else:
        # Run actual backfill
        report = manager.run_backfill(token_list, args.threshold, args.force)
        
        # Print final summary
        print("\n" + "="*60)
        print("BACKFILL SUMMARY")
        print("="*60)
        print(f"Total tokens checked: {report['total_tokens_checked']}")
        print(f"Tokens backfilled: {report['tokens_backfilled']}")
        print(f"Total new transactions added: {report['total_new_transactions']}")
        print("="*60)

if __name__ == "__main__":
    main() 