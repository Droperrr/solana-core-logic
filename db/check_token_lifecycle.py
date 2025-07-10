import sqlite3

DB_PATH = "solana_db.sqlite"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("Таблицы в базе:")
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [row[0] for row in cursor.fetchall()]
print(tables)

if 'token_lifecycle' in tables:
    print("\nСхема таблицы token_lifecycle:")
    cursor.execute("PRAGMA table_info(token_lifecycle);")
    for row in cursor.fetchall():
        print(row)
else:
    print("\nТаблица token_lifecycle НЕ найдена в базе!")

conn.close() 