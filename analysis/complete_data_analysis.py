#!/usr/bin/env python3
"""
📊 Полный анализ доступных данных о транзакциях
"""

import sqlite3
import json

def analyze_complete_data():
    """Полный анализ всех доступных данных"""
    conn = sqlite3.connect('db/solana_db.sqlite')
    
    print("📊 ПОЛНЫЙ АНАЛИЗ ДАННЫХ О ТРАНЗАКЦИЯХ")
    print("=" * 60)
    
    # 1. Базовые поля транзакций
    print("🔍 1. БАЗОВЫЕ ПОЛЯ TRANSACTIONS:")
    tx_sample = conn.execute('''
        SELECT signature, block_time, slot, fee_payer, transaction_type, 
               source_query_type, source_query_address 
        FROM transactions LIMIT 1
    ''').fetchone()
    
    print(f"  📝 signature: {tx_sample[0][:30]}... (уникальный ID транзакции)")
    print(f"  ⏰ block_time: {tx_sample[1]} (Unix timestamp)")
    print(f"  🎰 slot: {tx_sample[2]} (номер слота Solana)")
    print(f"  👛 fee_payer: {tx_sample[3] or 'None'} (кошелек, платящий комиссию)")
    print(f"  📋 transaction_type: {tx_sample[4]} (тип транзакции)")
    print(f"  🎯 source_query_type: {tx_sample[5]} (тип поискового запроса)")
    print(f"  🎯 source_query_address: {tx_sample[6]} (адрес токена)")
    
    # 2. Raw JSON структура
    print(f"\n🔍 2. RAW_JSON СТРУКТУРА:")
    raw_row = conn.execute('SELECT raw_json FROM transactions LIMIT 1').fetchone()
    raw_data = json.loads(raw_row[0])
    
    print(f"  📄 blockTime: временная метка блока")
    print(f"  📄 meta: метаданные выполнения транзакции")
    print(f"  📄 slot: номер слота")
    print(f"  📄 transaction: детали транзакции")
    print(f"  📄 version: версия транзакции")
    print(f"  📄 rpc_source: источник RPC")
    print(f"  📄 signature: подпись транзакции")
    
    # Анализ transaction структуры
    if 'transaction' in raw_data:
        tx_data = raw_data['transaction']
        print(f"\n  📂 transaction содержит:")
        print(f"    📋 message: {list(tx_data.get('message', {}).keys())}")
        print(f"    📋 signatures: список подписей")
    
    # Анализ meta структуры
    if 'meta' in raw_data:
        meta_data = raw_data['meta']
        print(f"\n  📂 meta содержит:")
        for key in meta_data.keys():
            print(f"    📋 {key}: {type(meta_data[key]).__name__}")
    
    # 3. Enriched Data структура
    print(f"\n🔍 3. ENRICHED_DATA СТРУКТУРА:")
    enriched_row = conn.execute('SELECT enriched_data FROM transactions LIMIT 1').fetchone()
    enriched_data = json.loads(enriched_row[0])
    
    if isinstance(enriched_data, list) and len(enriched_data) > 0:
        first_item = enriched_data[0]
        print(f"  📋 Список из {len(enriched_data)} обогащенных событий")
        print(f"  📋 Первое событие содержит:")
        for key in first_item.keys():
            print(f"    🎭 {key}: {type(first_item[key]).__name__}")
    
    # 4. Детальный анализ instruction data
    print(f"\n🔍 4. ДЕТАЛЬНЫЙ АНАЛИЗ ИНСТРУКЦИЙ:")
    
    # Проверяем, есть ли instructions в raw_data
    if 'transaction' in raw_data and 'message' in raw_data['transaction']:
        message = raw_data['transaction']['message']
        if 'instructions' in message:
            instructions = message['instructions']
            print(f"  📋 Найдено {len(instructions)} инструкций")
            
            for i, instr in enumerate(instructions[:2]):  # Первые 2 инструкции
                print(f"    🔧 Инструкция {i+1}:")
                for key, value in instr.items():
                    if key == 'data':
                        print(f"      📄 {key}: base64 data ({len(str(value))} chars)")
                    else:
                        print(f"      📄 {key}: {type(value).__name__} = {str(value)[:50]}")
    
    # 5. Анализ inner instructions
    if 'meta' in raw_data and 'innerInstructions' in raw_data['meta']:
        inner_instructions = raw_data['meta']['innerInstructions']
        print(f"\n  📋 Inner Instructions: {len(inner_instructions)} групп")
        
        for i, group in enumerate(inner_instructions[:1]):  # Первая группа
            if 'instructions' in group:
                print(f"    🔧 Группа {i+1}: {len(group['instructions'])} инструкций")
    
    # 6. Анализ изменений аккаунтов
    if 'meta' in raw_data:
        meta = raw_data['meta']
        print(f"\n🔍 5. ИЗМЕНЕНИЯ АККАУНТОВ:")
        
        if 'preBalances' in meta:
            print(f"  💰 preBalances: баланы до выполнения ({len(meta['preBalances'])} аккаунтов)")
        if 'postBalances' in meta:
            print(f"  💰 postBalances: балансы после выполнения ({len(meta['postBalances'])} аккаунтов)")
        if 'preTokenBalances' in meta:
            print(f"  🪙 preTokenBalances: токен-балансы до ({len(meta['preTokenBalances'])} токенов)")
        if 'postTokenBalances' in meta:
            print(f"  🪙 postTokenBalances: токен-балансы после ({len(meta['postTokenBalances'])} токенов)")
        if 'logMessages' in meta:
            print(f"  📝 logMessages: логи выполнения ({len(meta['logMessages'])} сообщений)")
    
    # 7. Анализ ML events (если есть данные)
    print(f"\n🔍 6. ML_READY_EVENTS:")
    ml_count = conn.execute('SELECT COUNT(*) FROM ml_ready_events').fetchone()[0]
    print(f"  📊 Всего ML событий: {ml_count}")
    
    if ml_count > 0:
        ml_sample = conn.execute('''
            SELECT event_type, platform, program_id, instruction_name
            FROM ml_ready_events 
            WHERE event_type IS NOT NULL 
            LIMIT 3
        ''').fetchall()
        
        for i, ml_event in enumerate(ml_sample):
            print(f"    🎭 Событие {i+1}: {ml_event[0]} на {ml_event[1]} (program: {ml_event[2]})")
    
    conn.close()

def create_analysis_summary():
    """Создание итогового списка доступных данных"""
    print(f"\n📋 ИТОГОВЫЙ СПИСОК ДОСТУПНЫХ ДАННЫХ:")
    print("=" * 50)
    
    categories = {
        "🔍 ИДЕНТИФИКАЦИЯ ТРАНЗАКЦИИ": [
            "signature - уникальный идентификатор транзакции",
            "slot - номер слота Solana",
            "block_time - точное время выполнения (Unix timestamp)",
            "transaction_type - классификация типа транзакции"
        ],
        
        "👛 ИНФОРМАЦИЯ О КОШЕЛЬКАХ": [
            "fee_payer - кошелек, платящий комиссию (редко заполнен)",
            "message.accountKeys - все задействованные аккаунты",
            "preBalances/postBalances - изменения SOL балансов",
            "preTokenBalances/postTokenBalances - изменения токен-балансов"
        ],
        
        "💰 ФИНАНСОВЫЕ ДАННЫЕ": [
            "meta.fee - размер комиссии",
            "preBalances - балансы SOL до транзакции",
            "postBalances - балансы SOL после транзакции",
            "preTokenBalances - токен-балансы до",
            "postTokenBalances - токен-балансы после",
            "enriched_data - обогащенные события (свапы, трансферы)"
        ],
        
        "🔧 ТЕХНИЧЕСКАЯ ИНФОРМАЦИЯ": [
            "message.instructions - список выполненных инструкций",
            "meta.innerInstructions - вложенные инструкции",
            "meta.logMessages - логи выполнения программ",
            "program_id - идентификаторы вызванных программ",
            "instruction_name - названия инструкций"
        ],
        
        "⏰ ВРЕМЕННЫЕ ДАННЫЕ": [
            "block_time - время выполнения транзакции",
            "slot - последовательность в блокчейне",
            "Возможность вычислить интервалы между транзакциями",
            "Кластеризация по времени для поиска координации"
        ],
        
        "🎯 КОНТЕКСТНЫЕ ДАННЫЕ": [
            "source_query_address - адрес токена",
            "rpc_source - источник данных",
            "enriched_data - семантически обогащенные события",
            "platform - платформа DEX (если определена)"
        ],
        
        "🕵️‍♂️ СЛЕДСТВЕННЫЕ ВОЗМОЖНОСТИ": [
            "Полная цепочка вызовов через innerInstructions",
            "Трассировка изменений балансов",
            "Анализ логов для поиска ошибок",
            "Корреляция между транзакциями по времени",
            "Идентификация программ и их инструкций"
        ]
    }
    
    for category, items in categories.items():
        print(f"\n{category}:")
        for item in items:
            print(f"  • {item}")

def main():
    analyze_complete_data()
    create_analysis_summary()
    
    print(f"\n🎉 АНАЛИЗ ЗАВЕРШЕН")
    print(f"💡 У нас есть очень богатые данные для детального анализа координации!")

if __name__ == "__main__":
    main() 