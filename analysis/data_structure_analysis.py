#!/usr/bin/env python3
"""
📊 Анализ структуры данных о транзакциях
"""

import sqlite3
import json

DB_PATH = 'db/solana_db.sqlite'

def analyze_transactions_structure():
    """Анализ структуры таблицы transactions"""
    conn = sqlite3.connect(DB_PATH)
    
    print("🔍 СТРУКТУРА ТАБЛИЦЫ TRANSACTIONS:")
    print("=" * 50)
    
    # Получаем структуру
    cols = conn.execute('PRAGMA table_info(transactions)').fetchall()
    for col in cols:
        print(f"  📋 {col[1]}: {col[2]}")
    
    print(f"\n📊 ОБРАЗЦЫ ДАННЫХ:")
    print("-" * 30)
    
    # Получаем образец данных
    sample = conn.execute('''
        SELECT signature, block_time, fee_payer, transaction_type, 
               source_query_address, raw_json, enriched_data
        FROM transactions 
        LIMIT 2
    ''').fetchall()
    
    for i, row in enumerate(sample, 1):
        print(f"\n🔍 Транзакция {i}:")
        print(f"  📝 Сигнатура: {row[0][:50]}...")
        print(f"  ⏰ Время блока: {row[1]} ({row[1]})")
        print(f"  👛 Fee payer: {row[2] or 'None'}")
        print(f"  📋 Тип: {row[3]}")
        print(f"  🎯 Адрес запроса: {row[4]}")
        
        # Анализ raw_json
        if row[5]:
            try:
                raw_data = json.loads(row[5])
                print(f"  📄 Raw JSON keys: {list(raw_data.keys())[:5]}...")
            except:
                print(f"  📄 Raw JSON: {str(row[5])[:50]}...")
        
        # Анализ enriched_data
        if row[6]:
            try:
                enriched = json.loads(row[6])
                print(f"  🎭 Enriched keys: {list(enriched.keys())[:5]}...")
            except:
                print(f"  🎭 Enriched: {str(row[6])[:50]}...")
    
    conn.close()

def analyze_ml_events_structure():
    """Анализ структуры таблицы ml_ready_events"""
    conn = sqlite3.connect(DB_PATH)
    
    print(f"\n🔍 СТРУКТУРА ТАБЛИЦЫ ML_READY_EVENTS:")
    print("=" * 50)
    
    # Получаем структуру
    cols = conn.execute('PRAGMA table_info(ml_ready_events)').fetchall()
    for col in cols:
        print(f"  📋 {col[1]}: {col[2]}")
    
    print(f"\n📊 ОБРАЗЦЫ ДАННЫХ:")
    print("-" * 30)
    
    # Получаем образец данных
    sample = conn.execute('''
        SELECT signature, block_time, event_type, from_wallet, to_wallet,
               from_amount, to_amount, token_a_mint, token_b_mint, 
               platform, enriched_data, event_data_raw
        FROM ml_ready_events 
        LIMIT 3
    ''').fetchall()
    
    for i, row in enumerate(sample, 1):
        print(f"\n🎭 ML событие {i}:")
        print(f"  📝 Сигнатура: {row[0][:50]}...")
        print(f"  ⏰ Время: {row[1]}")
        print(f"  🎯 Тип события: {row[2]}")
        print(f"  👛 From wallet: {row[3] or 'None'}")
        print(f"  👛 To wallet: {row[4] or 'None'}")
        print(f"  💰 From amount: {row[5]}")
        print(f"  💰 To amount: {row[6]}")
        print(f"  🪙 Token A: {row[7] or 'None'}")
        print(f"  🪙 Token B: {row[8] or 'None'}")
        print(f"  🏢 Platform: {row[9] or 'None'}")
        
        # Анализ enriched_data
        if row[10]:
            try:
                enriched = json.loads(row[10])
                print(f"  🎭 Enriched keys: {list(enriched.keys())[:3]}...")
            except:
                print(f"  🎭 Enriched: {str(row[10])[:30]}...")
        
        # Анализ event_data_raw
        if row[11]:
            print(f"  📄 Raw event data: {str(row[11])[:50]}...")

def analyze_raw_json_content():
    """Детальный анализ содержимого raw_json"""
    conn = sqlite3.connect(DB_PATH)
    
    print(f"\n🔍 ДЕТАЛЬНЫЙ АНАЛИЗ RAW_JSON:")
    print("=" * 50)
    
    # Получаем одну транзакцию с raw_json
    row = conn.execute('''
        SELECT raw_json, enriched_data 
        FROM transactions 
        WHERE raw_json IS NOT NULL 
        LIMIT 1
    ''').fetchone()
    
    if row and row[0]:
        try:
            raw_data = json.loads(row[0])
            print(f"📄 Структура RAW_JSON:")
            
            def print_structure(data, prefix="  "):
                if isinstance(data, dict):
                    for key, value in list(data.items())[:10]:  # Первые 10 ключей
                        if isinstance(value, (dict, list)):
                            print(f"{prefix}📂 {key}: {type(value).__name__}")
                            if isinstance(value, dict):
                                print_structure(value, prefix + "  ")
                        else:
                            print(f"{prefix}📄 {key}: {type(value).__name__} = {str(value)[:50]}...")
                elif isinstance(data, list) and len(data) > 0:
                    print(f"{prefix}📋 List[{len(data)}] items, first item:")
                    print_structure(data[0], prefix + "  ")
            
            print_structure(raw_data)
            
        except json.JSONDecodeError as e:
            print(f"❌ Ошибка парсинга JSON: {e}")
    
    conn.close()

def analyze_data_coverage():
    """Анализ покрытия данных"""
    conn = sqlite3.connect(DB_PATH)
    
    print(f"\n📊 ПОКРЫТИЕ ДАННЫХ:")
    print("=" * 30)
    
    # Статистика по transactions
    tx_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            COUNT(fee_payer) as has_fee_payer,
            COUNT(raw_json) as has_raw_json,
            COUNT(enriched_data) as has_enriched_data
        FROM transactions
    ''').fetchone()
    
    print(f"🔍 TRANSACTIONS:")
    print(f"  📊 Всего: {tx_stats[0]:,}")
    print(f"  👛 С fee_payer: {tx_stats[1]:,} ({tx_stats[1]/tx_stats[0]*100:.1f}%)")
    print(f"  📄 С raw_json: {tx_stats[2]:,} ({tx_stats[2]/tx_stats[0]*100:.1f}%)")
    print(f"  🎭 С enriched_data: {tx_stats[3]:,} ({tx_stats[3]/tx_stats[0]*100:.1f}%)")
    
    # Статистика по ml_ready_events
    ml_stats = conn.execute('''
        SELECT 
            COUNT(*) as total,
            COUNT(from_wallet) as has_from_wallet,
            COUNT(to_wallet) as has_to_wallet,
            COUNT(from_amount) as has_from_amount,
            COUNT(to_amount) as has_to_amount
        FROM ml_ready_events
    ''').fetchone()
    
    print(f"\n🎭 ML_READY_EVENTS:")
    print(f"  📊 Всего: {ml_stats[0]:,}")
    print(f"  👛 С from_wallet: {ml_stats[1]:,} ({ml_stats[1]/ml_stats[0]*100:.1f}%)")
    print(f"  👛 С to_wallet: {ml_stats[2]:,} ({ml_stats[2]/ml_stats[0]*100:.1f}%)")
    print(f"  💰 С from_amount: {ml_stats[3]:,} ({ml_stats[3]/ml_stats[0]*100:.1f}%)")
    print(f"  💰 С to_amount: {ml_stats[4]:,} ({ml_stats[4]/ml_stats[0]*100:.1f}%)")
    
    conn.close()

def main():
    print("📊 АНАЛИЗ СТРУКТУРЫ ДАННЫХ О ТРАНЗАКЦИЯХ")
    print("=" * 60)
    
    analyze_transactions_structure()
    analyze_ml_events_structure() 
    analyze_raw_json_content()
    analyze_data_coverage()
    
    print(f"\n🎉 АНАЛИЗ ЗАВЕРШЕН")

if __name__ == "__main__":
    main() 