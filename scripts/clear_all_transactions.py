#!/usr/bin/env python3
"""
Скрипт для полной очистки таблицы transactions в базе данных SQLite.
"""

import os
import sys
import logging
import sqlite3
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/db_operations.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("db_cleaner")

def get_sqlite_connection():
    """
    Создает и возвращает соединение с базой данных SQLite.
    
    Returns:
        Connection: Соединение с базой данных SQLite.
    """
    # Стандартный путь к базе данных
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')
    
    # Убеждаемся, что директория существует
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Подключение к базе данных: {db_path}")
    conn = sqlite3.connect(db_path)
    return conn

def clear_transactions_table():
    """
    Очищает таблицу transactions в базе данных SQLite.
    """
    conn = get_sqlite_connection()
    cursor = conn.cursor()
    
    try:
        # Проверяем, существует ли таблица
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'")
        if not cursor.fetchone():
            logger.info("Таблица transactions не существует. Нечего очищать.")
            return
            
        # Подсчитываем количество записей перед удалением
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count_before = cursor.fetchone()[0]
        logger.info(f"Количество записей перед очисткой: {count_before}")
        
        # Запрашиваем подтверждение
        confirm = input(f"Вы уверены, что хотите удалить все {count_before} записей из таблицы transactions? (y/n): ")
        if confirm.lower() != 'y':
            logger.info("Операция отменена пользователем.")
            return
        
        # Очищаем таблицу
        cursor.execute("DELETE FROM transactions")
        conn.commit()
        
        # Подсчитываем количество записей после удаления для проверки
        cursor.execute("SELECT COUNT(*) FROM transactions")
        count_after = cursor.fetchone()[0]
        
        logger.info(f"Таблица очищена. Записей удалено: {count_before - count_after}")
        logger.info(f"Количество записей после очистки: {count_after}")
        
    except Exception as e:
        logger.error(f"Ошибка при очистке таблицы transactions: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    logger.info("Начало очистки таблицы transactions...")
    clear_transactions_table()
    logger.info("Очистка таблицы transactions завершена.") 