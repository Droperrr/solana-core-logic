#!/usr/bin/env python3
"""
Скрипт для комплексного профилирования данных (Фаза 3.1) - Исправленная версия
Анализирует "ландшафт" собранного датасета для подготовки к поиску паттернов.
"""
import os
import sys
import json
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from analysis.data_provider import get_db_connection, dict_factory

def print_section(title):
    """Печать заголовка раздела"""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)

def print_subsection(title):
    """Печать подзаголовка"""
    print(f"\n--- {title} ---")

def analyze_basic_stats(conn):
    """Анализ основных статистик"""
    print_section("1. ОСНОВНЫЕ ХАРАКТЕРИСТИКИ ДАТАСЕТА")
    
    cursor = conn.cursor()
    
    # Проверяем доступные таблицы
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row['name'] for row in cursor.fetchall()]
    print(f"Доступные таблицы: {', '.join(tables)}")
    
    # Основные метрики
    stats = {}
    for table in ['transactions', 'ml_ready_events', 'feature_store_daily', 'wallets']:
        if table in tables:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
            stats[table] = cursor.fetchone()['count']
    
    print_subsection("Объем данных")
    for table, count in stats.items():
        print(f"  {table}: {count:,} записей")
    
    # Детальная статистика по транзакциям
    if 'transactions' in tables:
        query = """
        SELECT 
            COUNT(*) as total_transactions,
            COUNT(DISTINCT fee_payer) as unique_wallets,
            COUNT(DISTINCT source_query_address) as unique_tokens,
            MIN(datetime(block_time, 'unixepoch')) as first_transaction,
            MAX(datetime(block_time, 'unixepoch')) as last_transaction,
            AVG(CASE WHEN enriched_data IS NOT NULL THEN 1 ELSE 0 END) * 100 as enriched_rate
        FROM transactions;
        """
        cursor.execute(query)
        result = cursor.fetchone()
        
        print_subsection("Детальная статистика")
        print(f"  Всего транзакций: {result['total_transactions']:,}")
        print(f"  Уникальных кошельков: {result['unique_wallets']:,}")
        print(f"  Уникальных токенов: {result['unique_tokens']:,}")
        print(f"  Период данных: {result['first_transaction']} — {result['last_transaction']}")
        print(f"  Обогащенные транзакции: {result['enriched_rate']:.1f}%")
    
    return stats

def analyze_tokens(conn):
    """Анализ статистики по токенам"""
    print_section("2. АНАЛИЗ ТОКЕНОВ")
    
    cursor = conn.cursor()
    
    # Статистика по токенам
    query = """
    SELECT 
        source_query_address as token_address,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT fee_payer) as unique_wallets,
        MIN(datetime(block_time, 'unixepoch')) as first_tx,
        MAX(datetime(block_time, 'unixepoch')) as last_tx
    FROM transactions 
    GROUP BY source_query_address
    ORDER BY transaction_count DESC;
    """
    
    cursor.execute(query)
    tokens = cursor.fetchall()
    
    print_subsection("Статистика по токенам")
    for i, token in enumerate(tokens, 1):
        print(f"  Токен {i} ({token['token_address'][:8]}...):")
        print(f"    Транзакций: {token['transaction_count']:,}")
        print(f"    Уникальных кошельков: {token['unique_wallets']:,}")
        print(f"    Период: {token['first_tx']} — {token['last_tx']}")
    
    return tokens

def analyze_ml_events(conn):
    """Анализ ML событий"""
    print_section("3. АНАЛИЗ ML СОБЫТИЙ")
    
    cursor = conn.cursor()
    
    # Проверяем наличие ml_ready_events
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ml_ready_events';")
    if not cursor.fetchall():
        print("⚠️  Таблица ml_ready_events не найдена")
        return
    
    # Общая статистика
    cursor.execute("SELECT COUNT(*) as total FROM ml_ready_events;")
    total_events = cursor.fetchone()['total']
    print(f"Всего ML событий: {total_events:,}")
    
    # Статистика по типам событий
    query = """
    SELECT 
        event_type,
        COUNT(*) as event_count
    FROM ml_ready_events 
    WHERE event_type IS NOT NULL
    GROUP BY event_type
    ORDER BY event_count DESC;
    """
    
    cursor.execute(query)
    event_types = cursor.fetchall()
    
    print_subsection("Распределение типов событий")
    for event_type in event_types:
        print(f"  {event_type['event_type']}: {event_type['event_count']:,}")
    
    # Статистика по платформам
    query = """
    SELECT 
        platform,
        COUNT(*) as event_count
    FROM ml_ready_events 
    WHERE platform IS NOT NULL
    GROUP BY platform
    ORDER BY event_count DESC;
    """
    
    cursor.execute(query)
    platforms = cursor.fetchall()
    
    print_subsection("Распределение по платформам")
    for platform in platforms:
        print(f"  {platform['platform']}: {platform['event_count']:,}")

def analyze_wallet_roles(conn):
    """Анализ ролей кошельков"""
    print_section("4. АНАЛИЗ РОЛЕЙ КОШЕЛЬКОВ")
    
    cursor = conn.cursor()
    
    # Проверяем таблицу wallets
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='wallets';")
    has_wallets = len(cursor.fetchall()) > 0
    
    if has_wallets:
        query = """
        SELECT 
            role,
            COUNT(*) as wallet_count
        FROM wallets 
        GROUP BY role
        ORDER BY wallet_count DESC;
        """
        
        cursor.execute(query)
        roles = cursor.fetchall()
        
        print_subsection("Распределение ролей (из таблицы wallets)")
        for role in roles:
            print(f"  {role['role']}: {role['wallet_count']:,} кошельков")
            
        # Детальная информация по ролям
        for role in roles:
            if role['role'] in ['creator', 'dumper']:
                query = f"""
                SELECT address, notes
                FROM wallets 
                WHERE role = '{role['role']}'
                LIMIT 5;
                """
                cursor.execute(query)
                examples = cursor.fetchall()
                
                print(f"\n  Примеры {role['role']}:")
                for example in examples:
                    notes = example['notes'][:100] + "..." if example['notes'] and len(example['notes']) > 100 else example['notes']
                    print(f"    {example['address'][:10]}...: {notes}")
    else:
        print("⚠️  Таблица wallets не найдена")

def analyze_volumes(conn):
    """Анализ объемов торгов"""
    print_section("5. АНАЛИЗ ОБЪЕМОВ ТОРГОВ")
    
    cursor = conn.cursor()
    
    # Проверяем ml_ready_events
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='ml_ready_events';")
    if not cursor.fetchall():
        print("⚠️  Таблица ml_ready_events не найдена")
        return
    
    # Анализ объемов
    query = """
    SELECT 
        COUNT(*) as total_events,
        COUNT(CASE WHEN from_amount IS NOT NULL THEN 1 END) as events_with_from_amount,
        COUNT(CASE WHEN to_amount IS NOT NULL THEN 1 END) as events_with_to_amount,
        AVG(from_amount) as avg_from_amount,
        AVG(to_amount) as avg_to_amount,
        MIN(from_amount) as min_from_amount,
        MAX(from_amount) as max_from_amount,
        MIN(to_amount) as min_to_amount,
        MAX(to_amount) as max_to_amount
    FROM ml_ready_events;
    """
    
    cursor.execute(query)
    volume_stats = cursor.fetchone()
    
    print_subsection("Статистики объемов")
    print(f"  Всего событий: {volume_stats['total_events']:,}")
    print(f"  События с from_amount: {volume_stats['events_with_from_amount']:,}")
    print(f"  События с to_amount: {volume_stats['events_with_to_amount']:,}")
    
    if volume_stats['avg_from_amount']:
        print(f"\n  from_amount:")
        print(f"    Среднее: {volume_stats['avg_from_amount']:.6f}")
        print(f"    Минимум: {volume_stats['min_from_amount']:.6f}")
        print(f"    Максимум: {volume_stats['max_from_amount']:.6f}")
    
    if volume_stats['avg_to_amount']:
        print(f"\n  to_amount:")
        print(f"    Среднее: {volume_stats['avg_to_amount']:.6f}")
        print(f"    Минимум: {volume_stats['min_to_amount']:.6f}")
        print(f"    Максимум: {volume_stats['max_to_amount']:.6f}")

def analyze_time_patterns(conn):
    """Анализ временных паттернов"""
    print_section("6. ВРЕМЕННОЙ АНАЛИЗ")
    
    cursor = conn.cursor()
    
    # Анализ по дням
    query = """
    SELECT 
        DATE(datetime(block_time, 'unixepoch')) as tx_date,
        COUNT(*) as transaction_count,
        COUNT(DISTINCT fee_payer) as unique_wallets,
        source_query_address as token
    FROM transactions 
    GROUP BY tx_date, source_query_address
    ORDER BY tx_date;
    """
    
    cursor.execute(query)
    daily_stats = cursor.fetchall()
    
    print_subsection("Активность по дням")
    daily_totals = {}
    
    for row in daily_stats:
        date = row['tx_date']
        if date not in daily_totals:
            daily_totals[date] = 0
        daily_totals[date] += row['transaction_count']
    
    for date, total in sorted(daily_totals.items()):
        print(f"  {date}: {total:,} транзакций")
    
    # Найти самые активные дни
    if daily_totals:
        max_day = max(daily_totals.items(), key=lambda x: x[1])
        min_day = min(daily_totals.items(), key=lambda x: x[1])
        
        print_subsection("Экстремумы активности")
        print(f"  Самый активный день: {max_day[0]} ({max_day[1]:,} транзакций)")
        print(f"  Наименее активный день: {min_day[0]} ({min_day[1]:,} транзакций)")

def analyze_feature_store(conn):
    """Анализ Feature Store"""
    print_section("7. АНАЛИЗ FEATURE STORE")
    
    cursor = conn.cursor()
    
    # Проверяем feature_store_daily
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='feature_store_daily';")
    if not cursor.fetchall():
        print("⚠️  Таблица feature_store_daily не найдена")
        return
    
    cursor.execute("SELECT COUNT(*) as count FROM feature_store_daily;")
    feature_count = cursor.fetchone()['count']
    
    if feature_count == 0:
        print("⚠️  Feature Store пуст")
        return
    
    print_subsection(f"Feature Store содержит {feature_count} записей")
    
    # Анализ ключевых признаков
    features_to_analyze = [
        'volume_24h', 'transaction_count_24h', 'unique_wallets_24h',
        'gini_coefficient', 'liquidity_change_velocity', 'external_to_internal_volume_ratio'
    ]
    
    # Получаем информацию о столбцах
    cursor.execute("PRAGMA table_info(feature_store_daily);")
    columns = [col[1] for col in cursor.fetchall()]
    
    for feature in features_to_analyze:
        if feature in columns:
            query = f"""
            SELECT 
                COUNT({feature}) as non_null_count,
                AVG({feature}) as avg_value,
                MIN({feature}) as min_value,
                MAX({feature}) as max_value
            FROM feature_store_daily 
            WHERE {feature} IS NOT NULL;
            """
            
            cursor.execute(query)
            stats = cursor.fetchone()
            
            if stats['non_null_count'] > 0:
                print(f"\n  {feature}:")
                print(f"    Заполненность: {stats['non_null_count']}/{feature_count} ({stats['non_null_count']/feature_count*100:.1f}%)")
                print(f"    Среднее: {stats['avg_value']:.6f}")
                print(f"    Диапазон: {stats['min_value']:.6f} — {stats['max_value']:.6f}")

def create_summary_report(conn):
    """Создание итогового отчета"""
    print_section("8. ИТОГОВЫЙ ОТЧЕТ И РЕКОМЕНДАЦИИ")
    
    cursor = conn.cursor()
    
    # Собираем ключевые метрики
    metrics = {}
    
    # Основные счетчики
    for table in ['transactions', 'ml_ready_events', 'feature_store_daily']:
        try:
            cursor.execute(f"SELECT COUNT(*) as count FROM {table};")
            metrics[table] = cursor.fetchone()['count']
        except:
            metrics[table] = 0
    
    # Дополнительные метрики
    cursor.execute("SELECT COUNT(DISTINCT fee_payer) as count FROM transactions;")
    metrics['unique_wallets'] = cursor.fetchone()['count']
    
    cursor.execute("SELECT COUNT(DISTINCT source_query_address) as count FROM transactions;")
    metrics['unique_tokens'] = cursor.fetchone()['count']
    
    print_subsection("Ключевые метрики")
    print(f"  📊 Транзакций: {metrics['transactions']:,}")
    print(f"  🎯 ML событий: {metrics['ml_ready_events']:,}")
    print(f"  💎 Токенов: {metrics['unique_tokens']:,}")
    print(f"  👥 Кошельков: {metrics['unique_wallets']:,}")
    print(f"  📈 Feature Store: {metrics['feature_store_daily']:,} записей")
    
    print_subsection("Готовность к анализу")
    
    # Оценка готовности
    readiness_score = 0
    max_score = 4
    
    if metrics['transactions'] > 1000:
        print("  ✅ Достаточный объем транзакций")
        readiness_score += 1
    else:
        print("  ❌ Недостаточный объем транзакций")
    
    if metrics['ml_ready_events'] > 0:
        print("  ✅ ML события готовы")
        readiness_score += 1
    else:
        print("  ❌ ML события не готовы")
    
    if metrics['unique_tokens'] >= 2:
        print("  ✅ Множественные токены для сравнения")
        readiness_score += 1
    else:
        print("  ⚠️  Ограниченное количество токенов")
    
    if metrics['feature_store_daily'] > 0:
        print("  ✅ Feature Store готов")
        readiness_score += 1
    else:
        print("  ❌ Feature Store требует построения")
    
    print_subsection("Рекомендации для следующих фаз")
    
    if readiness_score >= 3:
        print("  🎯 Система готова к проверке гипотез")
        print("  📊 Рекомендуется начать с анализа 'тестовых транзакций'")
        print("  ⏱️  Изучить временные паттерны перед дампами")
        print("  📈 Проанализировать скорость изменения ликвидности")
    elif readiness_score >= 2:
        print("  ⚠️  Частичная готовность к анализу")
        print("  🔧 Рекомендуется завершить построение Feature Store")
        print("  📋 Проверить обогащение данных")
    else:
        print("  ❌ Требуется дополнительная подготовка данных")
        print("  🔄 Обновить материализованные представления")
        print("  🏗️  Построить Feature Store")
    
    print(f"\n  📊 Общая готовность: {readiness_score}/{max_score} ({readiness_score/max_score*100:.0f}%)")

def main():
    """Основная функция"""
    print("🚀 ЗАПУСК КОМПЛЕКСНОГО ПРОФИЛИРОВАНИЯ ДАННЫХ")
    print("Фаза 3.1: Аудит и профилирование данных")
    print(f"Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Подключение к БД
        conn = get_db_connection()
        conn.row_factory = dict_factory
        
        # Выполняем анализ по разделам
        analyze_basic_stats(conn)
        analyze_tokens(conn)
        analyze_ml_events(conn)
        analyze_wallet_roles(conn)
        analyze_volumes(conn)
        analyze_time_patterns(conn)
        analyze_feature_store(conn)
        create_summary_report(conn)
        
        conn.close()
        
        print_section("ПРОФИЛИРОВАНИЕ ЗАВЕРШЕНО")
        print("✅ Данные проанализированы")
        print("📋 Отчет готов для использования в следующих фазах")
        print("🎯 Переходите к проверке гипотез (Фаза 3.2)")
        
    except Exception as e:
        print(f"\n❌ Ошибка при профилировании: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main() 