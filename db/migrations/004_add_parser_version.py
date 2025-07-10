#!/usr/bin/env python3
"""
Миграция 004: Добавление версионирования парсера
Добавляет столбец parser_version для отслеживания версий обогащения данных
"""

def upgrade(conn):
    """Применить миграцию"""
    cursor = conn.cursor()
    
    # Проверяем, есть ли уже столбец parser_version
    cursor.execute("PRAGMA table_info(transactions)")
    columns = [row[1] for row in cursor.fetchall()]
    
    if 'parser_version' not in columns:
        cursor.execute("""
            ALTER TABLE transactions 
            ADD COLUMN parser_version TEXT DEFAULT '1.0.0'
        """)
        print("✅ Добавлен столбец parser_version")
    else:
        print("ℹ️ Столбец parser_version уже существует")
    
    if 'updated_at' not in columns:
        cursor.execute("""
            ALTER TABLE transactions 
            ADD COLUMN updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        """)
        print("✅ Добавлен столбец updated_at")
    else:
        print("ℹ️ Столбец updated_at уже существует")
    
    # Создаем индекс для быстрого поиска по версии парсера
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_transactions_parser_version 
        ON transactions(parser_version)
    """)
    
    # Обновляем существующие записи значением по умолчанию
    cursor.execute("""
        UPDATE transactions 
        SET parser_version = '1.0.0', updated_at = CURRENT_TIMESTAMP
        WHERE parser_version IS NULL
    """)
    
    conn.commit()
    print("✅ Миграция 004: Добавлены столбцы parser_version и updated_at")

def downgrade(conn):
    """Откатить миграцию"""
    cursor = conn.cursor()
    
    # SQLite не поддерживает DROP COLUMN напрямую
    # Создаем новую таблицу без этих столбцов
    cursor.execute("""
        CREATE TABLE transactions_backup AS 
        SELECT signature, block_time, slot, fee_payer, transaction_type, 
               raw_json, enriched_data, source_query_type, source_query_address
        FROM transactions
    """)
    
    cursor.execute("DROP TABLE transactions")
    cursor.execute("ALTER TABLE transactions_backup RENAME TO transactions")
    
    conn.commit()
    print("✅ Миграция 004: Откат завершен")

if __name__ == "__main__":
    import sqlite3
    import sys
    import os
    
    # Добавляем путь к проекту
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
    
    # Применяем миграцию
    conn = sqlite3.connect('db/solana_db.sqlite')
    try:
        upgrade(conn)
        print("Миграция успешно применена")
    except Exception as e:
        print(f"Ошибка применения миграции: {e}")
        downgrade(conn)
    finally:
        conn.close() 