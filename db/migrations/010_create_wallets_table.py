#!/usr/bin/env python3
"""
Миграция 010: Создание таблицы wallets для отслеживания кошельков и их ролей
"""
import sqlite3
import logging

logger = logging.getLogger("migration_010")

def up(conn):
    """
    Создает таблицу wallets для отслеживания кошельков, их ролей и статистики взаимодействий.
    """
    cursor = conn.cursor()
    
    # Создаем таблицу wallets
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS wallets (
        address TEXT PRIMARY KEY,
        role TEXT DEFAULT 'external',
        first_seen_timestamp INTEGER,
        last_seen_timestamp INTEGER,
        token_interaction_count INTEGER DEFAULT 1,
        notes TEXT
    );
    """)
    
    # Создаем индексы для быстрого поиска
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_role ON wallets(role);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_first_seen ON wallets(first_seen_timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_last_seen ON wallets(last_seen_timestamp);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_wallets_interaction_count ON wallets(token_interaction_count);")
    
    conn.commit()
    logger.info("Таблица wallets успешно создана")

def down(conn):
    """
    Удаляет таблицу wallets (откат миграции).
    """
    cursor = conn.cursor()
    
    # Удаляем индексы
    cursor.execute("DROP INDEX IF EXISTS idx_wallets_role;")
    cursor.execute("DROP INDEX IF EXISTS idx_wallets_first_seen;")
    cursor.execute("DROP INDEX IF EXISTS idx_wallets_last_seen;")
    cursor.execute("DROP INDEX IF EXISTS idx_wallets_interaction_count;")
    
    # Удаляем таблицу
    cursor.execute("DROP TABLE IF EXISTS wallets;")
    
    conn.commit()
    logger.info("Таблица wallets удалена")

if __name__ == "__main__":
    import os
    import sys
    
    # Добавляем путь к корню проекта
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
    
    from analysis.data_provider import get_db_connection
    
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    
    try:
        conn = get_db_connection()
        logger.info("Запуск миграции 010: создание таблицы wallets")
        up(conn)
        logger.info("Миграция 010 успешно выполнена")
    except Exception as e:
        logger.error(f"Ошибка при выполнении миграции 010: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close() 