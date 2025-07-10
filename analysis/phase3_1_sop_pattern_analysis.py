#!/usr/bin/env python3
"""
Фаза 1: Анализ стандартных операционных процедур (SOP) 
на семантически обогащенных данных
"""
import sqlite3
import pandas as pd
from collections import Counter, defaultdict
import json

def analyze_sop_patterns():
    """
    Анализирует стандартные операционные процедуры группы
    на основе семантически обогащенных данных
    """
    print("=" * 80)
    print("ФАЗА 1: АНАЛИЗ СТАНДАРТНЫХ ОПЕРАЦИОННЫХ ПРОЦЕДУР (SOP)")
    print("=" * 80)
    
    conn = sqlite3.connect('db/solana_db.sqlite')
    
    # Загружаем семантически обогащенные события
    print("📊 Загружаем семантически обогащенные события...")
    
    query = '''
    SELECT 
        e.signature,
        e.block_time,
        e.semantic_event_type,
        e.transaction_role,
        e.is_infrastructure,
        e.is_trading_related,
        e.program_category,
        e.token_a_mint,
        e.token_b_mint,
        e.from_wallet,
        e.to_wallet,
        t.source_query_address as token_address
    FROM enhanced_ml_events e
    JOIN transactions t ON e.signature = t.signature
    ORDER BY e.block_time, e.id
    '''
    
    df = pd.read_sql_query(query, conn)
    
    print(f"✅ Загружено {len(df):,} семантических событий")
    print(f"📝 Уникальных транзакций: {df['signature'].nunique():,}")
    
    # Анализ по токенам
    print(f"\n🪙 РАСПРЕДЕЛЕНИЕ ПО ТОКЕНАМ:")
    token_stats = df['token_address'].value_counts()
    for token, count in token_stats.items():
        print(f"  {token}: {count:,} событий")
    
    # Создаем последовательности событий для каждой транзакции
    print(f"\n🔍 АНАЛИЗ ПОСЛЕДОВАТЕЛЬНОСТЕЙ ОПЕРАЦИЙ...")
    
    sequences = df.groupby('signature').agg({
        'semantic_event_type': lambda x: ' -> '.join(x),
        'block_time': 'first',
        'token_address': 'first',
        'is_infrastructure': lambda x: sum(x),
        'is_trading_related': lambda x: sum(x)
    }).reset_index()
    
    sequences.columns = ['signature', 'sequence_pattern', 'block_time', 'token_address', 'infra_count', 'trading_count']
    
    print(f"✅ Проанализировано {len(sequences):,} уникальных транзакций")
    
    # Анализ общих паттернов
    print(f"\n📈 ТОП-15 САМЫХ ЧАСТЫХ ОПЕРАЦИОННЫХ ПРОЦЕДУР:")
    print("-" * 70)
    
    sequence_counts = sequences['sequence_pattern'].value_counts()
    total_transactions = len(sequences)
    
    for i, (pattern, count) in enumerate(sequence_counts.head(15).items(), 1):
        percentage = (count / total_transactions) * 100
        
        # Определяем тип операции
        if 'SWAP' in pattern or 'TOKEN_SWAP' in pattern:
            operation_type = "🔄 ТОРГОВЛЯ"
        elif 'TRANSFER' in pattern or 'TOKEN_TRANSFER' in pattern:
            operation_type = "📤 ПЕРЕВОД"
        elif 'INFRASTRUCTURE' in pattern:
            operation_type = "🔧 ИНФРАСТРУКТУРА"
        else:
            operation_type = "❓ СМЕШАННЫЙ"
        
        print(f"{i:2d}. {operation_type} ({count:3d} раз, {percentage:5.1f}%)")
        print(f"    📋 {pattern}")
        print()
    
    # Анализ доминирующего паттерна
    if not sequence_counts.empty:
        most_common_pattern = sequence_counts.index[0]
        most_common_count = sequence_counts.iloc[0]
        dominance_ratio = most_common_count / total_transactions
        
        print(f"🎯 АНАЛИЗ ДОМИНИРУЮЩЕГО ПАТТЕРНА:")
        print("-" * 50)
        print(f"Самая частая SOP: '{most_common_pattern}'")
        print(f"Встречается: {most_common_count} из {total_transactions} транзакций ({dominance_ratio:.1%})")
        
        if dominance_ratio > 0.5:
            print("✅ ВЫВОД: Это доминирующий паттерн - 'нормальное' поведение группы")
        elif dominance_ratio > 0.3:
            print("⚠️  ВЫВОД: Частый паттерн, но есть значительное разнообразие")
        else:
            print("🔍 ВЫВОД: Высокое разнообразие паттернов - нужен детальный анализ")
    
    # Сравнительный анализ по токенам
    print(f"\n🔬 СРАВНИТЕЛЬНЫЙ АНАЛИЗ ПО ТОКЕНАМ:")
    print("-" * 60)
    
    for token in token_stats.index:
        token_sequences = sequences[sequences['token_address'] == token]
        token_patterns = token_sequences['sequence_pattern'].value_counts()
        
        print(f"\n🪙 Токен: {token}")
        print(f"   Транзакций: {len(token_sequences):,}")
        print(f"   Топ-5 паттернов:")
        
        for i, (pattern, count) in enumerate(token_patterns.head(5).items(), 1):
            percentage = (count / len(token_sequences)) * 100
            print(f"   {i}. {pattern} ({count} раз, {percentage:.1f}%)")
    
    # Поиск межтокенных совпадений
    print(f"\n🔄 АНАЛИЗ МЕЖТОКЕННЫХ СОВПАДЕНИЙ (SOP FINGERPRINT):")
    print("-" * 70)
    
    # Получаем топ-5 паттернов для каждого токена
    token_top_patterns = {}
    for token in token_stats.index:
        token_sequences = sequences[sequences['token_address'] == token]
        token_patterns = token_sequences['sequence_pattern'].value_counts().head(5)
        token_top_patterns[token] = set(token_patterns.index)
    
    # Находим пересечения
    if len(token_top_patterns) >= 2:
        tokens = list(token_top_patterns.keys())
        common_patterns = token_top_patterns[tokens[0]]
        
        for token in tokens[1:]:
            common_patterns = common_patterns.intersection(token_top_patterns[token])
        
        print(f"🎯 ОБЩИЕ ПАТТЕРНЫ В ТОП-5 У ВСЕХ ТОКЕНОВ:")
        if common_patterns:
            for pattern in common_patterns:
                print(f"   ✅ {pattern}")
                
                # Показываем статистику по токенам для этого паттерна
                for token in tokens:
                    token_sequences = sequences[sequences['token_address'] == token]
                    pattern_count = (token_sequences['sequence_pattern'] == pattern).sum()
                    total_token_tx = len(token_sequences)
                    percentage = (pattern_count / total_token_tx) * 100
                    print(f"      {token}: {pattern_count} раз ({percentage:.1f}%)")
            
            print(f"\n🚨 КРИТИЧЕСКИЙ ВЫВОД:")
            print(f"   Найдено {len(common_patterns)} общих паттернов в топ-5 у всех токенов!")
            print(f"   Это указывает на СТАНДАРТИЗИРОВАННУЮ ОПЕРАЦИОННУЮ ПРОЦЕДУРУ!")
        else:
            print("   ❌ Общих паттернов в топ-5 не найдено")
    
    # Поиск аномальных транзакций
    print(f"\n🕵️ ПОИСК АНОМАЛЬНЫХ ТРАНЗАКЦИЙ:")
    print("-" * 50)
    
    # Паттерны, встречающиеся только 1-2 раза - потенциальные аномалии
    rare_patterns = sequence_counts[sequence_counts <= 2]
    
    print(f"Найдено {len(rare_patterns)} редких паттернов (≤2 раз):")
    
    for pattern, count in rare_patterns.head(10).items():
        print(f"   {count}x: {pattern}")
        
        # Находим транзакции с этим паттерном
        anomaly_transactions = sequences[sequences['sequence_pattern'] == pattern]
        for _, tx in anomaly_transactions.iterrows():
            print(f"      🔍 {tx['signature']} (токен: {tx['token_address']})")
    
    # Временной анализ
    print(f"\n📅 ВРЕМЕННОЙ АНАЛИЗ ПАТТЕРНОВ:")
    print("-" * 40)
    
    sequences['hour'] = pd.to_datetime(sequences['block_time'], unit='s').dt.hour
    hourly_patterns = sequences.groupby('hour')['sequence_pattern'].value_counts()
    
    print("Активность по часам (топ паттерн):")
    for hour in range(24):
        if hour in hourly_patterns.index.get_level_values(0):
            hour_data = hourly_patterns[hour]
            if not hour_data.empty:
                top_pattern = hour_data.index[0]
                count = hour_data.iloc[0]
                print(f"   {hour:2d}:00 - {top_pattern} ({count} раз)")
    
    # Создаем сводный отчет
    print(f"\n📋 СВОДНЫЙ ОТЧЕТ - ПОИСК SOP:")
    print("=" * 60)
    
    print(f"✅ Общая статистика:")
    print(f"   - Проанализировано транзакций: {total_transactions:,}")
    print(f"   - Уникальных паттернов: {len(sequence_counts):,}")
    print(f"   - Токенов: {len(token_stats)}")
    
    print(f"\n🎯 Ключевые выводы:")
    if dominance_ratio > 0.3:
        print(f"   ✅ Обнаружен доминирующий паттерн ({dominance_ratio:.1%} транзакций)")
        print(f"   ✅ Группа следует стандартизированной процедуре")
    
    if common_patterns:
        print(f"   🚨 Найдено {len(common_patterns)} общих паттернов между токенами")
        print(f"   🚨 ВЫСОКАЯ ВЕРОЯТНОСТЬ КООРДИНИРОВАННЫХ ДЕЙСТВИЙ")
    
    print(f"   📊 Редких паттернов (аномалий): {len(rare_patterns)}")
    
    conn.close()
    
    print(f"\n🎉 ФАЗА 1 ЗАВЕРШЕНА!")
    print(f"📈 Готовы к Фазе 2: Анализ аномалий в SOP")

if __name__ == "__main__":
    analyze_sop_patterns() 