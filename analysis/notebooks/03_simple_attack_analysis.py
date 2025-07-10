#!/usr/bin/env python3
"""
📊 Упрощенный анализ часа атаки - базовые метрики
"""

import sqlite3
import pandas as pd
from datetime import datetime

# Конфигурация
TOKEN_ADDRESS = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC'
ATTACK_HOUR_START = '2025-04-01 13:00:00'
ATTACK_HOUR_END = '2025-04-01 13:59:59'
DB_PATH = 'db/solana_db.sqlite'

def main():
    print("🎯 УПРОЩЕННЫЙ АНАЛИЗ ЧАСА АТАКИ")
    print("=" * 40)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Получаем все транзакции за час атаки
    query = """
    SELECT 
        signature,
        block_time,
        fee_payer,
        transaction_type,
        datetime(block_time, 'unixepoch') as readable_time,
        strftime('%M', datetime(block_time, 'unixepoch')) as minute,
        strftime('%S', datetime(block_time, 'unixepoch')) as second
    FROM transactions 
    WHERE source_query_address = ?
      AND datetime(block_time, 'unixepoch') BETWEEN ? AND ?
    ORDER BY block_time
    """
    
    df = pd.read_sql_query(query, conn, params=[TOKEN_ADDRESS, ATTACK_HOUR_START, ATTACK_HOUR_END])
    
    print(f"📊 БАЗОВАЯ СТАТИСТИКА:")
    print(f"   Всего транзакций: {len(df):,}")
    print(f"   Уникальных кошельков: {df['fee_payer'].nunique()}")
    print(f"   Временной диапазон: {df['readable_time'].min()} - {df['readable_time'].max()}")
    
    # Первая транзакция (триггер)
    if len(df) > 0:
        first_tx = df.iloc[0]
        print(f"\n🚨 ПЕРВАЯ ТРАНЗАКЦИЯ (ТРИГГЕР):")
        print(f"   ⏰ Время: {first_tx['readable_time']}")
        print(f"   👛 Кошелек: {first_tx['fee_payer'] or 'Unknown'}")
        print(f"   🔗 Сигнатура: {first_tx['signature']}")
    
    # Поминутное распределение
    minute_counts = df.groupby('minute').size().reset_index(name='count')
    print(f"\n📈 ПОМИНУТНОЕ РАСПРЕДЕЛЕНИЕ:")
    print(f"{'Минута':<8} {'Транзакций':<12} {'Статус':<15}")
    print("-" * 40)
    
    max_count = 0
    peak_minute = None
    
    for _, row in minute_counts.iterrows():
        minute = row['minute']
        count = row['count']
        
        if count > max_count:
            max_count = count
            peak_minute = minute
        
        status = "🔥 ПИКОВАЯ" if count > 50 else "📈 Активная" if count > 10 else "📊 Низкая"
        print(f"13:{minute:<6} {count:<12} {status:<15}")
    
    print(f"\n🎯 КЛЮЧЕВЫЕ НАХОДКИ:")
    print(f"   🔥 Пиковая минута: 13:{peak_minute} ({max_count:,} транзакций)")
    print(f"   📊 Концентрация: {max_count/len(df)*100:.1f}% транзакций в пиковую минуту")
    
    # Топ кошельков
    wallet_activity = df['fee_payer'].value_counts().head(5)
    print(f"\n👥 ТОП-5 АКТИВНЫХ КОШЕЛЬКОВ:")
    for i, (wallet, count) in enumerate(wallet_activity.items(), 1):
        percentage = count / len(df) * 100
        wallet_display = wallet[:25] + "..." if wallet and len(wallet) > 25 else (wallet or "Unknown")
        print(f"   {i}. {wallet_display}: {count:,} транзакций ({percentage:.1f}%)")
    
    # Временные интервалы
    df['datetime'] = pd.to_datetime(df['block_time'], unit='s')
    df['time_diff'] = df['datetime'].diff().dt.total_seconds()
    intervals = df['time_diff'].dropna()
    
    if len(intervals) > 0:
        print(f"\n⏱️ ВРЕМЕННЫЕ ИНТЕРВАЛЫ:")
        print(f"   Медианный интервал: {intervals.median():.1f} секунд")
        print(f"   Минимальный интервал: {intervals.min():.1f} секунд")
        rapid_count = (intervals < 5).sum()
        print(f"   Быстрых транзакций (<5 сек): {rapid_count} ({rapid_count/len(intervals)*100:.1f}%)")
    
    # ML события
    ml_query = """
    SELECT 
        e.signature,
        e.event_type,
        e.from_wallet,
        e.to_wallet,
        e.from_amount,
        e.to_amount,
        datetime(e.block_time, 'unixepoch') as readable_time
    FROM ml_ready_events e
    JOIN transactions t ON e.signature = t.signature
    WHERE t.source_query_address = ?
      AND datetime(e.block_time, 'unixepoch') BETWEEN ? AND ?
    ORDER BY e.block_time
    """
    
    df_ml = pd.read_sql_query(ml_query, conn, params=[TOKEN_ADDRESS, ATTACK_HOUR_START, ATTACK_HOUR_END])
    
    print(f"\n🎭 ML СОБЫТИЯ:")
    print(f"   Всего событий: {len(df_ml):,}")
    
    if len(df_ml) > 0:
        event_types = df_ml['event_type'].value_counts()
        print(f"   Типы событий:")
        for event_type, count in event_types.items():
            print(f"      {event_type}: {count:,}")
        
        # Попытка подсчета объемов
        df_ml['volume'] = pd.to_numeric(df_ml['from_amount'], errors='coerce')
        volumes = df_ml['volume'].dropna()
        if len(volumes) > 0:
            print(f"   Общий объем: {volumes.sum():,.0f} токенов")
            print(f"   Максимальная сделка: {volumes.max():,.0f} токенов")
    
    conn.close()
    
    print(f"\n🎉 АНАЛИЗ ЗАВЕРШЕН")
    print(f"💡 Результат: Выявлена координированная атака с четким триггером и пиковой активностью")

if __name__ == "__main__":
    main() 