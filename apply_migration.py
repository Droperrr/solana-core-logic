#!/usr/bin/env python3
"""
Скрипт для применения миграции failed_processing_log
"""
import sys
import os
import logging

# Добавляем корневую директорию проекта в sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from scripts.processing_utils import get_sqlite_connection
from db.migrations.migration_011_create_failed_processing_log import upgrade

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Применяет миграцию для создания таблицы failed_processing_log"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Применение миграции: создание таблицы failed_processing_log")
        
        # Получаем соединение с БД
        conn = get_sqlite_connection()
        
        # Применяем миграцию
        upgrade(conn)
        
        logger.info("Миграция успешно применена")
        
        # Проверяем, что таблица создана
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='failed_processing_log'")
        if cursor.fetchone():
            logger.info("✅ Таблица failed_processing_log успешно создана")
        else:
            logger.error("❌ Таблица failed_processing_log не найдена")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при применении миграции: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 