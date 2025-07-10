import sqlite3

conn = sqlite3.connect('db/solana_db.sqlite')
cursor = conn.cursor()

# Проверим структуру таблицы
cursor.execute("PRAGMA table_info(transactions)")
columns = cursor.fetchall()
print("Структура таблицы transactions:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")

# Проверим все токены
cursor.execute('SELECT DISTINCT token_address FROM transactions LIMIT 10')
tokens = cursor.fetchall()
print(f"\nПервые 10 токенов в базе:")
for token in tokens:
    print(f"  {token[0]}")

# Посчитаем транзакции для нашего токена
cursor.execute('SELECT COUNT(*) FROM transactions WHERE token_address = ?', ('AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC',))
count = cursor.fetchone()[0]
print(f'\nТранзакций для AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC: {count}')

conn.close() 