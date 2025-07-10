import sqlite3

conn = sqlite3.connect('db/solana_db.sqlite')
cursor = conn.cursor()

# Проверяем таблицы
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print('Таблицы в базе:')
for table in tables:
    print(f'  {table[0]}')

# Проверяем содержимое каждой таблицы
for table in tables:
    table_name = table[0]
    print(f'\n--- Таблица: {table_name} ---')
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f'Записей: {count}')
        
        if count > 0:
            cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
            rows = cursor.fetchall()
            print('Первые 3 записи:')
            for row in rows:
                print(f'  {row}')
    except Exception as e:
        print(f'Ошибка при чтении таблицы {table_name}: {e}')

conn.close() 