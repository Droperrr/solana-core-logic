#!/usr/bin/env python3
"""
Скрипт для анализа координированной активности в транзакциях Solana

Находит временные окна с аномально высокой активностью (>= 3 транзакций в минуту)
и выводит детали по топ-3 самым активным минутам.
"""

import sqlite3
import os
from typing import List, Tuple
from datetime import datetime

def connect_to_db() -> sqlite3.Connection:
    """Подключается к базе данных solana_db.sqlite"""
    db_path = os.path.join('db', 'solana_db.sqlite')
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"База данных не найдена: {db_path}")
    
    return sqlite3.connect(db_path)

def find_coordinated_activity(conn: sqlite3.Connection, threshold: int = 3) -> List[Tuple]:
    """
    Находит минуты с координированной активностью (>= threshold транзакций)
    
    Args:
        conn: Соединение с базой данных
        threshold: Порог количества транзакций для "горячей" минуты
        
    Returns:
        Список кортежей (минута, количество_транзакций)
    """
    query = """
    SELECT 
        strftime('%Y-%m-%d %H:%M', datetime(block_time, 'unixepoch')) as minute,
        COUNT(*) as tx_count
    FROM transactions 
    WHERE block_time IS NOT NULL
    GROUP BY minute
    HAVING tx_count >= ?
    ORDER BY tx_count DESC, minute DESC
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (threshold,))
    return cursor.fetchall()

def get_transactions_for_minute(conn: sqlite3.Connection, minute: str) -> List[str]:
    """
    Получает список сигнатур транзакций для конкретной минуты
    
    Args:
        conn: Соединение с базой данных
        minute: Минута в формате 'YYYY-MM-DD HH:MM'
        
    Returns:
        Список сигнатур транзакций
    """
    query = """
    SELECT signature
    FROM transactions 
    WHERE strftime('%Y-%m-%d %H:%M', datetime(block_time, 'unixepoch')) = ?
    ORDER BY block_time DESC
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (minute,))
    return [row[0] for row in cursor.fetchall()]

def format_minute_display(minute: str) -> str:
    """Форматирует минуту для красивого вывода"""
    try:
        dt = datetime.strptime(minute, '%Y-%m-%d %H:%M')
        return dt.strftime('%Y-%m-%d %H:%M:00')
    except ValueError:
        return minute

def main():
    """Основная функция анализа координированной активности"""
    print("--- Анализ координированной активности ---")
    
    try:
        # Подключаемся к базе данных
        conn = connect_to_db()
        print("✅ Подключение к базе данных установлено")
        
        # Находим координированную активность
        threshold = 3
        hot_minutes = find_coordinated_activity(conn, threshold)
        
        if not hot_minutes:
            print(f"❌ Не найдено минут с активностью >= {threshold} транзакций")
            return
        
        print(f"Найдено {len(hot_minutes)} \"горячих\" минут (>= {threshold} транзакций).")
        print()
        
        # Выводим топ-3 самых активных минут
        print("--- Топ-3 самых активных минут ---")
        print()
        
        for i, (minute, tx_count) in enumerate(hot_minutes[:3], 1):
            formatted_minute = format_minute_display(minute)
            print(f"{i}. Минута: {formatted_minute}")
            print(f"   Транзакций: {tx_count}")
            
            # Получаем сигнатуры для этой минуты
            signatures = get_transactions_for_minute(conn, minute)
            print("   Сигнатуры:")
            
            for sig in signatures:
                # Обрезаем длинные сигнатуры для читаемости
                display_sig = sig[:20] + "..." if len(sig) > 20 else sig
                print(f"     - {display_sig}")
            
            print()
        
        # Выводим общую статистику
        if len(hot_minutes) > 3:
            print(f"... и еще {len(hot_minutes) - 3} минут с высокой активностью")
        
        conn.close()
        print("✅ Анализ завершен")
        
    except FileNotFoundError as e:
        print(f"❌ Ошибка: {e}")
    except sqlite3.Error as e:
        print(f"❌ Ошибка базы данных: {e}")
    except Exception as e:
        print(f"❌ Неожиданная ошибка: {e}")

if __name__ == "__main__":
    main() 