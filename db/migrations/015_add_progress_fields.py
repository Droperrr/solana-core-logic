#!/usr/bin/env python3
"""
Миграция 015: Добавление полей прогресса в таблицу group_task_status
Добавляет поля progress_percent и current_step_description для отслеживания детального прогресса фоновых задач
"""

import sqlite3
import sys
import logging

logger = logging.getLogger("migration_015")

def upgrade(conn):
    """
    Добавляет поля прогресса в таблицу group_task_status
    """
    cursor = conn.cursor()
    
    print("🔧 Применение миграции 015: Добавление полей прогресса")
    
    # Проверяем существование полей и добавляем их если нужно
    cursor.execute("PRAGMA table_info(group_task_status)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'progress_percent' not in columns:
        print("📋 Добавление поля progress_percent...")
        cursor.execute("""
            ALTER TABLE group_task_status 
            ADD COLUMN progress_percent REAL DEFAULT 0.0
        """)
        print("✅ Поле progress_percent добавлено")
    else:
        print("✅ Поле progress_percent уже существует")
    
    if 'current_step_description' not in columns:
        print("📋 Добавление поля current_step_description...")
        cursor.execute("""
            ALTER TABLE group_task_status 
            ADD COLUMN current_step_description TEXT
        """)
        print("✅ Поле current_step_description добавлено")
    else:
        print("✅ Поле current_step_description уже существует")
    
    # Создаем индекс для progress_percent для быстрых запросов
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_progress ON group_task_status(progress_percent);")
    print("✅ Индекс для progress_percent создан")
    
    # Проверяем финальную структуру
    print("\n📊 Финальная структура таблицы group_task_status:")
    cursor.execute("PRAGMA table_info(group_task_status)")
    for row in cursor.fetchall():
        print(f"  - {row[1]}: {row[2]} (nullable: {not row[3]})")
    
    conn.commit()
    print("\n🎉 Миграция 015 успешно применена!")

def downgrade(conn):
    """
    Откат миграции (удаление добавленных полей)
    ВНИМАНИЕ: SQLite не поддерживает DROP COLUMN напрямую
    """
    cursor = conn.cursor()
    
    print("🔄 Откат миграции 015...")
    print("⚠️  SQLite не поддерживает DROP COLUMN")
    print("⚠️  Для полного отката потребуется пересоздание таблицы")
    
    # Удаляем только индекс
    cursor.execute("DROP INDEX IF EXISTS idx_group_progress;")
    
    conn.commit()
    print("✅ Частичный откат миграции 015 завершен")

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    
    # Путь к базе данных
    db_path = sys.argv[1] if len(sys.argv) > 1 else "solana_db.sqlite"
    print(f"🎯 Применение миграции к: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        upgrade(conn)
    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении миграции 015: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close() 