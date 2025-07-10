#!/usr/bin/env python3
"""
Комплексное профилирование данных и анализ базовых метрик
Задача #6: Анализ датасета для 7 целевых токенов
"""

import pandas as pd
import numpy as np
import sqlite3
import json
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Настройка отображения
pd.set_option('display.max_columns', None)
pd.set_option('display.max_colwidth', None)
plt.style.use('default')
sns.set_palette("husl")

def main():
    print("=== КОМПЛЕКСНОЕ ПРОФИЛИРОВАНИЕ ДАННЫХ ===")
    print("Задача #6: Анализ датасета для 7 целевых токенов\n")
    
    # Список целевых токенов
    target_tokens = [
        'GXjfhyftcfEdZAoHvq3M5N5CsYRHPHv1zyXcAb4bgEZX',
        'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC',
        'AGmXzgFQw8bLtsJvr5dgnHtyP6rP6GLwmZwgVqjFL6Q8',
        'CUZBsiF17GCU8xFnikRAgwxvZdBBQfLfGDp8FzASE4br',
        '5rFkk8z4EoQF2v9RZVaBM5PWUePsfziB9jQvw5ZNG3hs',
        'FYwAKe7iPKeXc9JkZJVfozYkjaFwDtnqtwxM4xYCMVyF',
        '8sQnmbTENLz3reZBT4jK2DMDnFA5YYXDFbi4WUZxwgmw'
    ]
    
    print(f"Целевые токены: {len(target_tokens)}")
    for i, token in enumerate(target_tokens, 1):
        print(f"{i}. {token}")
    print()
    
    # Подключение к базе данных и загрузка данных
    db_path = "db/solana_db.sqlite"
    
    # Создаем SQL запрос для получения транзакций целевых токенов
    tokens_str = "', '".join(target_tokens)
    query = f"""
    SELECT * FROM transactions 
    WHERE source_query_address IN ('{tokens_str}')
    ORDER BY block_time
    """
    
    print("Выполняем запрос к базе данных...")
    
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"Данные загружены успешно!")
        print(f"Размер датасета: {df.shape}")
        print()
        
    except Exception as e:
        print(f"Ошибка при загрузке данных: {e}")
        return
    
    # 2. Общий анализ датасета
    print("=== ОБЩИЙ АНАЛИЗ ДАТАСЕТА ===")
    print(f"Общее количество транзакций: {len(df):,}")
    print(f"Количество уникальных токенов: {df['source_query_address'].nunique()}")
    
    # Временной диапазон
    df['block_time'] = pd.to_datetime(df['block_time'], unit='s')
    print(f"Временной диапазон: {df['block_time'].min()} - {df['block_time'].max()}")
    print(f"Продолжительность периода: {df['block_time'].max() - df['block_time'].min()}")
    
    # Распределение по токенам
    print("\nРаспределение транзакций по токенам:")
    token_counts = df['source_query_address'].value_counts()
    for token, count in token_counts.items():
        print(f"{token}: {count:,} транзакций")
    print()
    
    # 3. Детальный анализ каждого токена
    print("=== ДЕТАЛЬНЫЙ АНАЛИЗ КАЖДОГО ТОКЕНА ===")
    token_analyses = []
    
    for token in target_tokens:
        token_df = df[df['source_query_address'] == token].copy()
        if len(token_df) > 0:
            analysis = analyze_token_data(token_df, token)
            token_analyses.append(analysis)
        else:
            print(f"\n=== ТОКЕН НЕ НАЙДЕН: {token} ===")
            print("Транзакции для этого токена отсутствуют в базе данных")
    
    # 4. Агрегированный анализ
    if token_analyses:
        print("\n=== АГРЕГИРОВАННЫЙ АНАЛИЗ ===")
        analysis_df = pd.DataFrame(token_analyses)
        
        print("\nСредние значения метрик:")
        print(f"Среднее время жизни токена: {analysis_df['lifetime_hours'].mean():.1f} часов")
        print(f"Среднее количество уникальных участников: {analysis_df['unique_participants'].mean():.1f}")
        print(f"Средний процент транзакций с enriched_data: {analysis_df['enriched_percent'].mean():.1f}%")
        print(f"Среднее количество событий на транзакцию: {analysis_df['avg_events_per_tx'].mean():.2f}")
        
        print("\nМедианные значения метрик:")
        print(f"Медианное время жизни токена: {analysis_df['lifetime_hours'].median():.1f} часов")
        print(f"Медианное количество уникальных участников: {analysis_df['unique_participants'].median():.1f}")
        print(f"Медианный процент транзакций с enriched_data: {analysis_df['enriched_percent'].median():.1f}%")
        print(f"Медианное количество событий на транзакцию: {analysis_df['avg_events_per_tx'].median():.2f}")
        
        # Сохраняем результаты в CSV
        analysis_df.to_csv('analysis/token_analysis_results.csv', index=False)
        print(f"\nРезультаты сохранены в analysis/token_analysis_results.csv")
        
        # Создаем визуализации
        create_visualizations(analysis_df, df, target_tokens)
        
        # Выводим ключевые выводы
        print_key_findings(analysis_df, df)
    else:
        print("Нет данных для агрегированного анализа")

def analyze_token_data(token_df, token_address):
    """
    Анализирует данные для конкретного токена
    """
    print(f"\n=== АНАЛИЗ ТОКЕНА: {token_address} ===")
    
    # 1. Время жизни токена
    first_tx = token_df['block_time'].min()
    last_tx = token_df['block_time'].max()
    lifetime = last_tx - first_tx
    
    print(f"Время жизни токена: {lifetime}")
    print(f"Первая транзакция: {first_tx}")
    print(f"Последняя транзакция: {last_tx}")
    
    # 2. Количество уникальных участников
    unique_participants = token_df['fee_payer'].nunique()
    print(f"Количество уникальных участников: {unique_participants:,}")
    
    # 3. Анализ enriched_data
    enriched_count = token_df['enriched_data'].notna().sum()
    enriched_percent = (enriched_count / len(token_df)) * 100
    print(f"Транзакции с enriched_data: {enriched_count:,} ({enriched_percent:.1f}%)")
    
    # Подсчет среднего количества событий
    event_counts = []
    for enriched_data in token_df['enriched_data'].dropna():
        try:
            events = json.loads(enriched_data)
            if isinstance(events, list):
                event_counts.append(len(events))
        except:
            continue
    
    if event_counts:
        avg_events = np.mean(event_counts)
        print(f"Среднее количество событий на транзакцию: {avg_events:.2f}")
    else:
        print("События не найдены в enriched_data")
        avg_events = 0
    
    return {
        'token': token_address,
        'total_transactions': len(token_df),
        'lifetime_hours': lifetime.total_seconds() / 3600,
        'unique_participants': unique_participants,
        'enriched_percent': enriched_percent,
        'avg_events_per_tx': avg_events
    }

def create_visualizations(analysis_df, df, target_tokens):
    """
    Создает визуализации для анализа
    """
    print("\nСоздание визуализаций...")
    
    # 1. Распределение активности по времени
    fig, axes = plt.subplots(2, 4, figsize=(20, 10))
    axes = axes.flatten()
    
    for i, token in enumerate(target_tokens):
        if i >= len(axes):
            break
            
        token_df = df[df['source_query_address'] == token].copy()
        if len(token_df) > 0:
            # Распределение по часам
            hourly_activity = token_df['block_time'].dt.hour.value_counts().sort_index()
            
            axes[i].bar(hourly_activity.index, hourly_activity.values, alpha=0.7)
            axes[i].set_title(f'Активность токена {token[:8]}...')
            axes[i].set_xlabel('Час (UTC)')
            axes[i].set_ylabel('Количество транзакций')
            axes[i].grid(True, alpha=0.3)
        else:
            axes[i].text(0.5, 0.5, 'Нет данных', ha='center', va='center', transform=axes[i].transAxes)
            axes[i].set_title(f'Токен {token[:8]}...')
    
    plt.tight_layout()
    plt.savefig('analysis/hourly_activity.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    # 2. Агрегированные метрики
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    
    # Время жизни
    axes[0,0].hist(analysis_df['lifetime_hours'], bins=10, alpha=0.7, edgecolor='black')
    axes[0,0].set_title('Распределение времени жизни токенов')
    axes[0,0].set_xlabel('Время жизни (часы)')
    axes[0,0].set_ylabel('Количество токенов')
    
    # Уникальные участники
    axes[0,1].hist(analysis_df['unique_participants'], bins=10, alpha=0.7, edgecolor='black')
    axes[0,1].set_title('Распределение количества уникальных участников')
    axes[0,1].set_xlabel('Количество участников')
    axes[0,1].set_ylabel('Количество токенов')
    
    # Процент enriched_data
    axes[1,0].hist(analysis_df['enriched_percent'], bins=10, alpha=0.7, edgecolor='black')
    axes[1,0].set_title('Распределение процента транзакций с enriched_data')
    axes[1,0].set_xlabel('Процент (%)')
    axes[1,0].set_ylabel('Количество токенов')
    
    # Количество событий
    axes[1,1].hist(analysis_df['avg_events_per_tx'], bins=10, alpha=0.7, edgecolor='black')
    axes[1,1].set_title('Распределение среднего количества событий')
    axes[1,1].set_xlabel('Среднее количество событий')
    axes[1,1].set_ylabel('Количество токенов')
    
    plt.tight_layout()
    plt.savefig('analysis/aggregated_metrics.png', dpi=300, bbox_inches='tight')
    plt.close()
    
    print("Визуализации сохранены:")
    print("- analysis/hourly_activity.png")
    print("- analysis/aggregated_metrics.png")

def print_key_findings(analysis_df, df):
    """
    Выводит ключевые выводы анализа
    """
    print("\n" + "="*60)
    print("КЛЮЧЕВЫЕ ВЫВОДЫ")
    print("="*60)
    
    # 1. Временные паттерны
    print("\n1. ВРЕМЕННЫЕ ПАТТЕРНЫ АКТИВНОСТИ:")
    print(f"   - Среднее время жизни токенов: {analysis_df['lifetime_hours'].mean():.1f} часов")
    print(f"   - Медианное время жизни: {analysis_df['lifetime_hours'].median():.1f} часов")
    print(f"   - Самый долгоживущий токен: {analysis_df.loc[analysis_df['lifetime_hours'].idxmax(), 'token'][:8]}... ({analysis_df['lifetime_hours'].max():.1f} часов)")
    
    # 2. Качество данных
    print("\n2. КАЧЕСТВО ДАННЫХ:")
    print(f"   - Средний процент транзакций с enriched_data: {analysis_df['enriched_percent'].mean():.1f}%")
    print(f"   - Среднее количество событий на транзакцию: {analysis_df['avg_events_per_tx'].mean():.2f}")
    
    # 3. Участники и активность
    print("\n3. УЧАСТНИКИ И АКТИВНОСТЬ:")
    print(f"   - Среднее количество уникальных участников: {analysis_df['unique_participants'].mean():.1f}")
    print(f"   - Токен с наибольшим количеством участников: {analysis_df.loc[analysis_df['unique_participants'].idxmax(), 'token'][:8]}... ({analysis_df['unique_participants'].max():.0f} участников)")
    
    # 4. Общие тенденции
    print("\n4. ОБЩИЕ ТЕНДЕНЦИИ:")
    total_transactions = analysis_df['total_transactions'].sum()
    print(f"   - Общее количество транзакций: {total_transactions:,}")
    print(f"   - Среднее количество транзакций на токен: {analysis_df['total_transactions'].mean():.1f}")
    
    # 5. Рекомендации
    print("\n5. РЕКОМЕНДАЦИИ ДЛЯ ДАЛЬНЕЙШЕГО АНАЛИЗА:")
    print("   - Исследовать паттерны активности по времени суток")
    print("   - Анализировать типы событий в enriched_data")
    print("   - Изучить корреляции между метриками")
    print("   - Рассмотреть возможность кластеризации токенов по поведению")

if __name__ == "__main__":
    main() 