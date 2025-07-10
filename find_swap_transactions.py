import sqlite3
import json

# Подключение к базе данных
conn = sqlite3.connect('db/solana_db.sqlite')
cursor = conn.cursor()

# Поиск всех транзакций, содержащих SWAP
cursor.execute('''
    SELECT signature, transaction_type, enriched_data, block_time, source_query_address
    FROM transactions 
    WHERE (transaction_type LIKE '%SWAP%' OR enriched_data LIKE '%SWAP%')
    ORDER BY block_time DESC 
    LIMIT 20
''')

results = cursor.fetchall()

print(f"Найдено {len(results)} SWAP-транзакций:")
print("-" * 80)

swap_signatures = []

for i, (signature, tx_type, enriched_data, block_time, source_query_address) in enumerate(results, 1):
    print(f"{i}. Signature: {signature}")
    print(f"   Type: {tx_type}")
    print(f"   Block Time: {block_time}")
    print(f"   Source Address: {source_query_address}")
    
    # Проверяем, содержит ли транзакция наш тестовый токен
    token_mint = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC'
    contains_our_token = False
    
    if enriched_data:
        try:
            enriched_json = json.loads(enriched_data)
            # Ищем токен в различных полях
            if 'token_changes' in enriched_json:
                for change in enriched_json['token_changes']:
                    if 'mint' in change and change['mint'] == token_mint:
                        contains_our_token = True
                        break
            if 'swaps' in enriched_json:
                for swap in enriched_json['swaps']:
                    if 'token_in' in swap and swap['token_in'].get('mint') == token_mint:
                        contains_our_token = True
                        break
                    if 'token_out' in swap and swap['token_out'].get('mint') == token_mint:
                        contains_our_token = True
                        break
        except:
            # Если не удалось распарсить JSON, ищем строку
            if token_mint in enriched_data:
                contains_our_token = True
    
    if contains_our_token:
        print(f"   *** НАШ ТОКЕН НАЙДЕН В ЭТОЙ ТРАНЗАКЦИИ! ***")
        swap_signatures.append(signature)
    
    print(f"   Enriched Data Preview: {enriched_data[:300] if enriched_data else 'None'}...")
    print()

if swap_signatures:
    print(f"\n*** НАЙДЕНЫ SWAP-ТРАНЗАКЦИИ С НАШИМ ТОКЕНОМ: {len(swap_signatures)} ***")
    for sig in swap_signatures:
        print(f"  - {sig}")
else:
    print("\n*** SWAP-ТРАНЗАКЦИИ С НАШИМ ТОКЕНОМ НЕ НАЙДЕНЫ ***")
    print("Попробуем найти любые транзакции с нашим токеном...")
    
    # Поиск любых транзакций с нашим токеном
    token_mint = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC'
    cursor.execute('''
        SELECT signature, transaction_type, enriched_data, block_time, source_query_address
        FROM transactions 
        WHERE source_query_address = ?
        ORDER BY block_time DESC 
        LIMIT 10
    ''', (token_mint,))
    
    token_results = cursor.fetchall()
    print(f"\nНайдено {len(token_results)} транзакций для нашего токена:")
    for i, (signature, tx_type, enriched_data, block_time, source_query_address) in enumerate(token_results, 1):
        print(f"{i}. {signature} - {tx_type}")

conn.close() 