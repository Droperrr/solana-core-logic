#!/usr/bin/env python3
"""
Миграция 012: Создание таблицы token_collection_progress для отслеживания прогресса сбора данных по токенам
"""
import sqlite3
import logging

logger = logging.getLogger("migration_012")

def up(conn):
    """
    Создает таблицу token_collection_progress для отслеживания прогресса сбора транзакций по токенам.
    """
    cursor = conn.cursor()
    
    # Создаем таблицу token_collection_progress
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS token_collection_progress (
        token_address TEXT PRIMARY KEY,
        on_chain_tx_count INTEGER,
        db_tx_count INTEGER,
        completeness_ratio REAL,
        last_checked_at INTEGER,
        status TEXT DEFAULT 'unknown',
        last_collection_at INTEGER,
        error_message TEXT,
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        updated_at INTEGER DEFAULT (strftime('%s', 'now'))
    );
    """)
    
    # Создаем индексы для быстрого поиска
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_status ON token_collection_progress(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_last_checked ON token_collection_progress(last_checked_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_completeness ON token_collection_progress(completeness_ratio);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_updated ON token_collection_progress(updated_at);")
    
    # Создаем триггер для автоматического обновления updated_at
    cursor.execute("""
    CREATE TRIGGER IF NOT EXISTS update_token_progress_timestamp 
    AFTER UPDATE ON token_collection_progress
    BEGIN
        UPDATE token_collection_progress 
        SET updated_at = strftime('%s', 'now') 
        WHERE token_address = NEW.token_address;
    END;
    """)
    
    conn.commit()
    logger.info("Таблица token_collection_progress успешно создана")

def down(conn):
    """
    Удаляет таблицу token_collection_progress (откат миграции).
    """
    cursor = conn.cursor()
    
    # Удаляем триггер
    cursor.execute("DROP TRIGGER IF EXISTS update_token_progress_timestamp;")
    
    # Удаляем индексы
    cursor.execute("DROP INDEX IF EXISTS idx_progress_status;")
    cursor.execute("DROP INDEX IF EXISTS idx_progress_last_checked;")
    cursor.execute("DROP INDEX IF EXISTS idx_progress_completeness;")
    cursor.execute("DROP INDEX IF EXISTS idx_progress_updated;")
    
    # Удаляем таблицу
    cursor.execute("DROP TABLE IF EXISTS token_collection_progress;")
    
    conn.commit()
    logger.info("Таблица token_collection_progress удалена")

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
        logger.info("Запуск миграции 012: создание таблицы token_collection_progress")
        up(conn)
        logger.info("Миграция 012 успешно выполнена")
    except Exception as e:
        logger.error(f"Ошибка при выполнении миграции 012: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close() 