import sqlite3

# Подключение к базе данных
conn = sqlite3.connect('db/solana_db.sqlite')
cursor = conn.cursor()

# Сначала посмотрим на структуру таблицы transactions
cursor.execute("PRAGMA table_info(transactions)")
columns = cursor.fetchall()
print("Структура таблицы transactions:")
for col in columns:
    print(f"  {col[1]} ({col[2]})")
print()

# Поиск транзакций для нашего токена
token_mint = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC'

# Попробуем найти транзакции, содержащие наш токен в enriched_data
cursor.execute('''
    SELECT signature, transaction_type, enriched_data, block_time 
    FROM transactions 
    WHERE enriched_data LIKE ? 
    ORDER BY block_time DESC 
    LIMIT 10
''', (f'%{token_mint}%',))

results = cursor.fetchall()

print(f"Найдено {len(results)} транзакций, содержащих токен {token_mint}:")
print("-" * 80)

for i, (signature, tx_type, enriched_data, block_time) in enumerate(results, 1):
    print(f"{i}. Signature: {signature}")
    print(f"   Type: {tx_type}")
    print(f"   Block Time: {block_time}")
    
    # Проверяем, есть ли SWAP в типе или обогащенных данных
    is_swap = False
    if tx_type and 'SWAP' in tx_type.upper():
        is_swap = True
    if enriched_data and 'SWAP' in enriched_data.upper():
        is_swap = True
    
    if is_swap:
        print(f"   *** SWAP ТРАНЗАКЦИЯ НАЙДЕНА! ***")
    
    print(f"   Enriched Data Preview: {enriched_data[:200] if enriched_data else 'None'}...")
    print()

conn.close() 