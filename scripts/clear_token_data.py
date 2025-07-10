#!/usr/bin/env python3
"""
Скрипт для очистки данных токена из базы данных
"""

import sqlite3
import argparse
import sys
from pathlib import Path

def clear_token_data(token_mint: str, db_path: str = "db/solana_db.sqlite"):
    """Очищает все данные токена из базы данных"""
    
    if not Path(db_path).exists():
        print(f"Ошибка: База данных {db_path} не найдена")
        return False
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # Получаем все сигнатуры транзакций токена
        cursor.execute("SELECT signature FROM transactions WHERE source_query_address = ?", (token_mint,))
        sigs = [row[0] for row in cursor.fetchall()]
        print(f"Найдено {len(sigs)} сигнатур для удаления из ml_ready_events.")
        # Удаляем связанные события из ml_ready_events
        if sigs:
            qmarks = ','.join(['?'] * len(sigs))
            cursor.execute(f"DELETE FROM ml_ready_events WHERE signature IN ({qmarks})", sigs)
            ml_events_deleted = cursor.rowcount
        else:
            ml_events_deleted = 0
        # Удаляем транзакции
        cursor.execute("DELETE FROM transactions WHERE source_query_address = ?", (token_mint,))
        transactions_deleted = cursor.rowcount
        
        # Удаляем записи о прогрессе
        try:
            cursor.execute("DELETE FROM token_collection_progress WHERE token = ?", (token_mint,))
            progress_deleted = cursor.rowcount
        except sqlite3.OperationalError:
            progress_deleted = 0
        
        conn.commit()
        
        print(f"Очистка завершена для токена {token_mint}:")
        print(f"  - Удалено транзакций: {transactions_deleted}")
        print(f"  - Удалено записей прогресса: {progress_deleted}")
        print(f"  - Удалено ML событий: {ml_events_deleted}")
        
        return True
        
    except Exception as e:
        print(f"Ошибка при очистке данных: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Очистка данных токена из базы данных")
    parser.add_argument("--token", required=True, help="Адрес токена для очистки")
    parser.add_argument("--db-path", default="db/solana_db.sqlite", help="Путь к базе данных")
    
    args = parser.parse_args()
    
    success = clear_token_data(args.token, args.db_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main() 