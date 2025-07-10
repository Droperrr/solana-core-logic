#!/usr/bin/env python3
"""
Скрипт для применения миграции 012: добавление retry_count и status
"""
import sys
import os
import logging

# Добавляем корневую директорию проекта в sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from scripts.processing_utils import get_sqlite_connection
from db.migrations.migration_012_add_retry_count_to_failed_log import upgrade

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Применяет миграцию для добавления retry_count и status"""
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Применение миграции 012: добавление retry_count и status")
        
        # Получаем соединение с БД
        conn = get_sqlite_connection()
        
        # Применяем миграцию
        upgrade(conn)
        
        logger.info("Миграция 012 успешно применена")
        
        # Проверяем, что колонки добавлены
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(failed_processing_log)")
        columns = cursor.fetchall()
        
        column_names = [col[1] for col in columns]
        if 'retry_count' in column_names and 'status' in column_names:
            logger.info("✅ Колонки retry_count и status успешно добавлены")
        else:
            logger.error("❌ Колонки не найдены")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Ошибка при применении миграции: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 