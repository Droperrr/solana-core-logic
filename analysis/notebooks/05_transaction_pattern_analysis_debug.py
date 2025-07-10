#!/usr/bin/env python3
"""
🧠 Анализ паттернов транзакций - отладочная версия
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Конфигурация
DB_PATH = 'db/solana_db.sqlite'
TOKEN_ADDRESSES = [
    'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC',
    'AGmXzgFQw8bLtsJvr5dgnHtyP6rP6GLwmZwgVqjFL6Q8'
]

def test_database_connection():
    """Тест подключения к базе данных"""
    print("🔧 ТЕСТИРОВАНИЕ ПОДКЛЮЧЕНИЯ К БД")
    try:
        conn = sqlite3.connect(DB_PATH)
        
        # Проверяем наличие таблицы
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
        print(f"   ✅ Таблицы в БД: {[t[0] for t in tables]}")
        
        # Проверяем количество записей
        for token in TOKEN_ADDRESSES:
            count = conn.execute("SELECT COUNT(*) FROM transactions WHERE source_query_address = ?", (token,)).fetchone()[0]
            print(f"   📊 {token[:20]}...: {count:,} транзакций")
        
        conn.close()
        return True
    except Exception as e:
        print(f"   ❌ Ошибка подключения: {e}")
        return False

def get_sample_data(token_address, limit=10):
    """Получить образец данных"""
    conn = sqlite3.connect(DB_PATH)
    
    query = """
    SELECT 
        signature,
        block_time,
        fee_payer,
        transaction_type,
        datetime(block_time, 'unixepoch') as readable_time
    FROM transactions 
    WHERE source_query_address = ?
    ORDER BY block_time
    LIMIT ?
    """
    
    df = pd.read_sql_query(query, conn, params=[token_address, limit])
    conn.close()
    return df

def analyze_basic_patterns(token_address):
    """Базовый анализ паттернов"""
    print(f"\n🔍 АНАЛИЗ ТОКЕНА: {token_address[:25]}...")
    
    try:
        # Получаем образец данных
        sample_df = get_sample_data(token_address, 100)
        print(f"   📊 Загружено записей: {len(sample_df)}")
        
        if len(sample_df) == 0:
            print("   ❌ Нет данных")
            return None
        
        # Показываем первые записи
        print(f"   📋 Первые записи:")
        for i, row in sample_df.head(3).iterrows():
            print(f"      {row['readable_time']} | {row['fee_payer'][:15] if row['fee_payer'] else 'Unknown'}... | {row['transaction_type']}")
        
        # Базовая статистика
        unique_wallets = sample_df['fee_payer'].nunique()
        date_range = sample_df['readable_time'].nunique()
        
        print(f"   📈 Статистика (образец):")
        print(f"      Уникальных кошельков: {unique_wallets}")
        print(f"      Временной диапазон: {sample_df['readable_time'].min()} - {sample_df['readable_time'].max()}")
        
        # Поиск кластеров активности
        sample_df['datetime'] = pd.to_datetime(sample_df['readable_time'])
        sample_df['time_diff'] = sample_df['datetime'].diff().dt.total_seconds()
        
        rapid_transactions = sample_df[sample_df['time_diff'] < 5]
        print(f"      Быстрых транзакций (<5 сек): {len(rapid_transactions)} из {len(sample_df)}")
        
        if len(rapid_transactions) > 0:
            print(f"      Минимальный интервал: {sample_df['time_diff'].min():.1f} сек")
            print(f"      Медианный интервал: {sample_df['time_diff'].median():.1f} сек")
        
        return {
            'token': token_address,
            'sample_size': len(sample_df),
            'unique_wallets': unique_wallets,
            'rapid_count': len(rapid_transactions),
            'median_interval': sample_df['time_diff'].median(),
            'first_time': sample_df['readable_time'].min(),
            'last_time': sample_df['readable_time'].max()
        }
        
    except Exception as e:
        print(f"   ❌ Ошибка анализа: {e}")
        return None

def compare_tokens(results):
    """Сравнение результатов между токенами"""
    print(f"\n🔄 СРАВНИТЕЛЬНЫЙ АНАЛИЗ")
    print("=" * 40)
    
    if len(results) < 2:
        print("❌ Недостаточно данных для сравнения")
        return
    
    print(f"📊 Сравнение паттернов:")
    
    # Сравниваем медианные интервалы
    intervals = [r['median_interval'] for r in results if r['median_interval'] is not None]
    
    if len(intervals) >= 2:
        print(f"   ⏱️ Медианные интервалы:")
        for i, result in enumerate(results):
            if result['median_interval'] is not None:
                print(f"      Токен {i+1}: {result['median_interval']:.1f} сек")
        
        # Проверяем схожесть
        if len(intervals) == 2:
            diff = abs(intervals[0] - intervals[1])
            print(f"   📊 Разность интервалов: {diff:.1f} сек")
            
            if diff < 2.0:
                print(f"   ✅ СИГНАЛ КООРДИНАЦИИ: Схожие временные интервалы!")
            else:
                print(f"   ❌ Значительная разность в интервалах")
    
    # Сравниваем процент быстрых транзакций
    rapid_percentages = []
    for result in results:
        if result['sample_size'] > 0:
            rapid_pct = result['rapid_count'] / result['sample_size'] * 100
            rapid_percentages.append(rapid_pct)
            print(f"   🚀 Быстрых транзакций: {rapid_pct:.1f}%")
    
    if len(rapid_percentages) >= 2:
        rapid_diff = abs(rapid_percentages[0] - rapid_percentages[1])
        print(f"   📊 Разность %% быстрых: {rapid_diff:.1f}%")
        
        if rapid_diff < 10.0:
            print(f"   ✅ СИГНАЛ КООРДИНАЦИИ: Схожий процент быстрых транзакций!")

def main():
    """Основная функция"""
    print("🧠 АНАЛИЗ ПАТТЕРНОВ ТРАНЗАКЦИЙ - ОТЛАДОЧНАЯ ВЕРСИЯ")
    print("=" * 60)
    
    # Тестируем подключение
    if not test_database_connection():
        return
    
    print(f"\n📊 Анализируем {len(TOKEN_ADDRESSES)} токенов")
    
    results = []
    
    # Анализ каждого токена
    for token_address in TOKEN_ADDRESSES:
        result = analyze_basic_patterns(token_address)
        if result:
            results.append(result)
    
    # Сравнительный анализ
    compare_tokens(results)
    
    print(f"\n🎉 ОТЛАДОЧНЫЙ АНАЛИЗ ЗАВЕРШЕН")
    print(f"📝 Найдено результатов: {len(results)}")

if __name__ == "__main__":
    main() 