#!/usr/bin/env python3
"""
Скрипт для поиска SWAP-транзакции в базе данных
"""

import sqlite3
import json

def find_swap_candidate(token_mint: str, db_path: str = "db/solana_db.sqlite"):
    """Находит SWAP-транзакцию для токена"""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Поиск транзакций с SWAP в enriched_data
        cursor.execute('''
            SELECT signature, enriched_data, block_time 
            FROM transactions 
            WHERE source_query_address = ? 
            AND enriched_data LIKE '%"event_type":"SWAP"%'
            ORDER BY block_time DESC 
            LIMIT 1
        ''', (token_mint,))
        
        result = cursor.fetchone()
        
        if result:
            signature, enriched_data, block_time = result
            print(f"Найдена SWAP-транзакция:")
            print(f"  Сигнатура: {signature}")
            print(f"  Block Time: {block_time}")
            print(f"  Enriched Data Preview: {enriched_data[:200]}...")
            return signature
        else:
            print("SWAP-транзакции не найдены")
            return None
            
    finally:
        conn.close()

if __name__ == "__main__":
    token_mint = "AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC"
    signature = find_swap_candidate(token_mint)
    if signature:
        print(f"\nСигнатура для тестирования: {signature}") 