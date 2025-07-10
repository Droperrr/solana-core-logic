#!/usr/bin/env python3
"""
Миграция 011: Создание таблицы failed_processing_log для Dead-Letter Queue
"""
import logging

logger = logging.getLogger(__name__)

def upgrade(conn):
    """
    Создает таблицу failed_processing_log для логирования сбойных транзакций.
    """
    try:
        cursor = conn.cursor()
        
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS failed_processing_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            signature TEXT NOT NULL,
            failed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reason TEXT NOT NULL,
            payload TEXT
        )
        ''')
        
        # Создаем индексы для быстрого поиска
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_failed_processing_log_signature ON failed_processing_log(signature)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_failed_processing_log_failed_at ON failed_processing_log(failed_at)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_failed_processing_log_reason ON failed_processing_log(reason)')
        
        conn.commit()
        logger.info("Таблица failed_processing_log успешно создана")
        
    except Exception as e:
        logger.error(f"Ошибка при создании таблицы failed_processing_log: {e}")
        conn.rollback()
        raise

def downgrade(conn):
    """
    Удаляет таблицу failed_processing_log.
    """
    try:
        cursor = conn.cursor()
        cursor.execute('DROP TABLE IF EXISTS failed_processing_log')
        conn.commit()
        logger.info("Таблица failed_processing_log успешно удалена")
        
    except Exception as e:
        logger.error(f"Ошибка при удалении таблицы failed_processing_log: {e}")
        conn.rollback()
        raise 