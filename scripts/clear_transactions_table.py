#!/usr/bin/env python3
"""
Скрипт для очистки таблицы транзакций в SQLite базе данных.
Этот скрипт предназначен для использования перед тестированием новых функций обработки транзакций.

Примеры использования:
1. Очистка всех транзакций:
   python scripts/clear_transactions_table.py

2. Подтверждение перед очисткой:
   python scripts/clear_transactions_table.py --confirm
"""
import os
import sys
import sqlite3
import argparse
import logging
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/maintenance.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("db_maintenance")

def get_sqlite_connection(db_path=None):
    """
    Создает и возвращает соединение с базой данных SQLite.
    По умолчанию использует файл db/solana_db.sqlite в директории проекта.
    
    Args:
        db_path (str, optional): Путь к файлу базы данных SQLite.
        
    Returns:
        Connection: Соединение с базой данных SQLite.
    """
    if db_path is None:
        # Используем стандартный путь к базе данных
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')
        
    # Создаем директорию для базы данных, если её нет
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    return conn

def clear_transactions_table(db_path=None, confirm=False):
    """
    Очищает таблицу транзакций в базе данных.
    
    Args:
        db_path (str, optional): Путь к файлу базы данных SQLite.
        confirm (bool): Требовать подтверждения перед удалением.
    
    Returns:
        int: Количество удаленных записей.
    """
    if confirm:
        response = input("Вы действительно хотите очистить таблицу транзакций? [y/N] ")
        if response.lower() != 'y':
            logger.info("Операция отменена пользователем.")
            return 0
    
    conn = get_sqlite_connection(db_path)
    cursor = conn.cursor()
    
    try:
        # Получаем текущее количество записей
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count = cursor.fetchone()[0]
        logger.info(f"Таблица transactions содержит {count} записей")
        
        # Удаляем все записи
        cursor.execute("DELETE FROM transactions")
        conn.commit()
        
        logger.info(f"Удалено {count} записей из таблицы transactions")
        return count
    except sqlite3.Error as e:
        logger.error(f"Ошибка при очистке таблицы: {e}")
        conn.rollback()
        return 0
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(description="Очистка таблицы транзакций в базе данных SQLite")
    parser.add_argument("--db-path", help="Путь к файлу базы данных SQLite")
    parser.add_argument("--confirm", action="store_true", help="Запрашивать подтверждение перед удалением")
    
    args = parser.parse_args()
    
    count = clear_transactions_table(args.db_path, args.confirm)
    print(f"Очищено {count} записей из таблицы транзакций.")

if __name__ == "__main__":
    main() 