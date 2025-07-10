#!/usr/bin/env python3
"""
Проверка токенов в базе данных
"""

import sqlite3

def check_tokens():
    db_path = "db/solana_db.sqlite"
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Проверяем общее количество транзакций
        cursor.execute("SELECT COUNT(*) FROM transactions")
        total_tx = cursor.fetchone()[0]
        print(f"Общее количество транзакций в базе: {total_tx:,}")
        
        # Проверяем уникальные токены
        cursor.execute("SELECT DISTINCT source_query_address FROM transactions")
        unique_tokens = cursor.fetchall()
        print(f"Количество уникальных токенов: {len(unique_tokens)}")
        
        print("\nУникальные токены в базе:")
        for i, (token,) in enumerate(unique_tokens, 1):
            print(f"{i}. {token}")
        
        # Проверяем количество транзакций для каждого токена
        print("\nКоличество транзакций по токенам:")
        for token, in unique_tokens:
            cursor.execute("SELECT COUNT(*) FROM transactions WHERE source_query_address = ?", (token,))
            count = cursor.fetchone()[0]
            print(f"{token}: {count:,} транзакций")
        
        conn.close()
        
    except Exception as e:
        print(f"Ошибка: {e}")

if __name__ == "__main__":
    check_tokens() 