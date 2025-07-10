#!/usr/bin/env python3
"""
Миграция 014: Проверка и дополнение структуры контрольных таблиц
Обеспечивает наличие всех необходимых таблиц и полей для дашборда
"""

import sqlite3
import sys
import logging

logger = logging.getLogger("migration_014")

def upgrade(conn):
    """
    Проверяет и создает/дополняет контрольные таблицы для дашборда
    """
    cursor = conn.cursor()
    
    print("🔧 Применение миграции 014: Контрольные таблицы для дашборда")
    
    # 1. Проверяем и создаем таблицу token_lifecycle
    print("📋 Проверка таблицы token_lifecycle...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS token_lifecycle (
            token_address TEXT PRIMARY KEY,
            creation_signature TEXT,
            creation_time INTEGER,
            first_dump_signature TEXT,
            first_dump_time INTEGER,
            first_dump_price_drop_percent REAL,
            last_processed_signature TEXT
        );
    """)
    print("✅ Таблица token_lifecycle готова")
    
    # 2. Проверяем и создаем таблицу token_collection_progress
    print("📋 Проверка таблицы token_collection_progress...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS token_collection_progress (
            token_address TEXT PRIMARY KEY,
            on_chain_tx_count INTEGER,
            db_tx_count INTEGER,
            completeness_ratio REAL,
            status TEXT DEFAULT 'unknown',
            last_checked_at INTEGER,
            last_collection_at INTEGER,
            error_message TEXT,
            created_at INTEGER DEFAULT (strftime('%s', 'now')),
            updated_at INTEGER DEFAULT (strftime('%s', 'now'))
        );
    """)
    print("✅ Таблица token_collection_progress готова")
    
    # 3. Проверяем и создаем таблицу group_task_status
    print("📋 Проверка таблицы group_task_status...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS group_task_status (
            group_name TEXT PRIMARY KEY,
            status TEXT NOT NULL,
            updated_at INTEGER NOT NULL
        );
    """)
    print("✅ Таблица group_task_status готова")
    
    # 4. Создаем индексы для оптимизации запросов
    print("🔗 Создание индексов...")
    
    # Индексы для token_collection_progress
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_status ON token_collection_progress(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_last_checked ON token_collection_progress(last_checked_at);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_completeness ON token_collection_progress(completeness_ratio);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_progress_updated ON token_collection_progress(updated_at);")
    
    # Индексы для token_lifecycle
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_lifecycle_dump_time ON token_lifecycle(first_dump_time);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_lifecycle_creation_time ON token_lifecycle(creation_time);")
    
    # Индексы для group_task_status
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_status ON group_task_status(status);")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_group_updated ON group_task_status(updated_at);")
    
    print("✅ Индексы созданы")
    
    # 5. Создаем триггер для автоматического обновления updated_at в token_collection_progress
    print("⚡ Создание триггеров...")
    cursor.execute("""
        CREATE TRIGGER IF NOT EXISTS update_token_progress_timestamp 
        AFTER UPDATE ON token_collection_progress
        BEGIN
            UPDATE token_collection_progress 
            SET updated_at = strftime('%s', 'now') 
            WHERE token_address = NEW.token_address;
        END;
    """)
    print("✅ Триггеры созданы")
    
    # 6. Включаем режим WAL для безопасного одновременного доступа
    print("🔒 Настройка режима WAL...")
    cursor.execute("PRAGMA journal_mode=WAL;")
    result = cursor.fetchone()
    print(f"✅ Режим журналирования: {result[0] if result else 'неизвестно'}")
    
    # 7. Проверяем финальную структуру
    print("\n📊 Финальная проверка структуры таблиц:")
    
    tables = ['token_lifecycle', 'token_collection_progress', 'group_task_status']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  - {table}: {count} записей")
    
    conn.commit()
    print("\n🎉 Миграция 014 успешно применена!")

def downgrade(conn):
    """
    Откат миграции (удаление созданных элементов)
    """
    cursor = conn.cursor()
    
    print("🔄 Откат миграции 014...")
    
    # Удаляем триггеры
    cursor.execute("DROP TRIGGER IF EXISTS update_token_progress_timestamp;")
    
    # Удаляем индексы
    indexes = [
        'idx_progress_status', 'idx_progress_last_checked', 'idx_progress_completeness', 'idx_progress_updated',
        'idx_lifecycle_dump_time', 'idx_lifecycle_creation_time',
        'idx_group_status', 'idx_group_updated'
    ]
    
    for index in indexes:
        cursor.execute(f"DROP INDEX IF EXISTS {index};")
    
    # ВНИМАНИЕ: Таблицы НЕ удаляем, так как они могут содержать важные данные
    print("⚠️  Таблицы сохранены (содержат данные)")
    
    conn.commit()
    print("✅ Откат миграции 014 завершен")

if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(level=logging.INFO)
    
    # Путь к базе данных
    db_path = sys.argv[1] if len(sys.argv) > 1 else "db/solana_db.sqlite"
    print(f"🎯 Применение миграции к: {db_path}")
    
    try:
        conn = sqlite3.connect(db_path)
        upgrade(conn)
    except Exception as e:
        logger.error(f"❌ Ошибка при выполнении миграции 014: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close() 