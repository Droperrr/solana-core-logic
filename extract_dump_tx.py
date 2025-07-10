#!/usr/bin/env python3
"""
Extract raw JSON for dump transaction analysis
"""

import sqlite3
import json

def extract_dump_transaction():
    """Extract raw JSON for the dump transaction"""
    
    signature = "3TZ15bGjHmoUST4eHATy68XiDkjg3P7ct1xmknpSisNv3D5awF4GqHXTjV53Qxue1WLo4YrC5e3UaL4PNx4fuEok"
    
    conn = sqlite3.connect('db/solana_db.sqlite')
    cursor = conn.cursor()
    
    try:
        cursor.execute('SELECT raw_json FROM transactions WHERE signature = ?', (signature,))
        result = cursor.fetchone()
        
        if result:
            print("Transaction found in DB. Attempting to parse raw_json...")
            try:
                raw_json = json.loads(result[0])
                print("raw_json loaded. First 500 chars:")
                print(result[0][:500])
            except Exception as e:
                print(f"Error parsing raw_json: {e}")
                print(f"Raw value (first 500 chars): {result[0][:500]}")
                return
            
            # Save full JSON
            try:
                with open('debug_dump_tx.json', 'w') as f:
                    json.dump(raw_json, f, indent=2)
                print(f"Raw JSON saved to debug_dump_tx.json")
            except Exception as e:
                print(f"Error writing debug_dump_tx.json: {e}")
                return
            
            # Extract key parts for analysis
            meta = raw_json.get('meta', {})
            inner_instructions = meta.get('innerInstructions', [])
            pre_token_balances = meta.get('preTokenBalances', [])
            post_token_balances = meta.get('postTokenBalances', [])
            
            print(f"\n=== TRANSACTION ANALYSIS ===")
            print(f"Signature: {signature}")
            print(f"Inner Instructions Count: {len(inner_instructions)}")
            print(f"Pre Token Balances Count: {len(pre_token_balances)}")
            print(f"Post Token Balances Count: {len(post_token_balances)}")
            
            # Save key parts separately
            try:
                with open('debug_inner_instructions.json', 'w') as f:
                    json.dump(inner_instructions, f, indent=2)
                print(f"Inner instructions saved to debug_inner_instructions.json")
            except Exception as e:
                print(f"Error writing debug_inner_instructions.json: {e}")
            try:
                with open('debug_token_balances.json', 'w') as f:
                    json.dump({
                        'pre': pre_token_balances,
                        'post': post_token_balances
                    }, f, indent=2)
                print(f"Token balances saved to debug_token_balances.json")
            except Exception as e:
                print(f"Error writing debug_token_balances.json: {e}")
            
        else:
            print(f"Transaction {signature} not found in database")
            
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    extract_dump_transaction() 