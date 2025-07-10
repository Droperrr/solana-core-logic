#!/usr/bin/env python3
"""
🧠 Проверка гипотезы H-001: Фиксированный временной лаг между триггером и дампом
Упрощенная версия без scipy
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Конфигурация
DB_PATH = 'db/solana_db.sqlite'
TOKEN_ADDRESSES = [
    'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC',
    'AGmXzgFQw8bLtsJvr5dgnHtyP6rP6GLwmZwgVqjFL6Q8'
]

def get_token_events(token_address):
    """Получить ML события для токена"""
    conn = sqlite3.connect(DB_PATH)
    
    ml_events_query = """
    SELECT 
        e.signature,
        e.block_time,
        e.event_type,
        e.from_wallet,
        e.to_wallet,
        e.from_amount,
        e.to_amount,
        e.token_a_mint,
        e.token_b_mint,
        datetime(e.block_time, 'unixepoch') as readable_time
    FROM ml_ready_events e
    JOIN transactions t ON e.signature = t.signature
    WHERE t.source_query_address = ?
    ORDER BY e.block_time
    """
    
    df_ml_events = pd.read_sql_query(ml_events_query, conn, params=[token_address])
    conn.close()
    return df_ml_events

def find_creation_and_dump(df_ml_events, token_address):
    """Найти событие создания и первый дамп"""
    print(f"   📊 Анализируем {len(df_ml_events)} ML событий")
    
    if len(df_ml_events) == 0:
        return None, None
    
    # Преобразуем amount в числа
    df_ml_events['from_amount_num'] = pd.to_numeric(df_ml_events['from_amount'], errors='coerce')
    df_ml_events['to_amount_num'] = pd.to_numeric(df_ml_events['to_amount'], errors='coerce')
    
    # Ищем первую крупную покупку (получаем наш токен)
    buys = df_ml_events[
        (df_ml_events['token_b_mint'] == token_address) &
        (df_ml_events['from_amount_num'] >= 500_000_000)  # 0.5 SOL в lamports
    ].copy()
    
    creation_event = None
    if len(buys) > 0:
        creation_event = buys.sort_values('block_time').iloc[0]
        print(f"   ✅ Найдено событие создания: {creation_event['readable_time']}")
        print(f"      💰 Размер: {creation_event['from_amount_num']/1e9:.2f} SOL")
    
    # Ищем первый крупный дамп (отдаем наш токен)
    dumps = df_ml_events[
        (df_ml_events['token_a_mint'] == token_address) &
        (df_ml_events['from_amount_num'] >= 1_000_000)  # 1М токенов
    ].copy()
    
    dump_event = None
    if len(dumps) > 0:
        dump_event = dumps.sort_values('block_time').iloc[0]
        print(f"   ✅ Найден первый дамп: {dump_event['readable_time']}")
        print(f"      💣 Размер: {dump_event['from_amount_num']/1e6:.1f}М токенов")
    
    return creation_event, dump_event

def analyze_token(token_address):
    """Анализ одного токена"""
    print(f"\n🔍 Анализ токена: {token_address[:25]}...")
    
    df_events = get_token_events(token_address)
    creation, dump = find_creation_and_dump(df_events, token_address)
    
    if creation is None or dump is None:
        print(f"   ❌ Не найдена пара событий (создание/дамп)")
        return None
    
    # Вычисляем лаг
    creation_time = datetime.fromtimestamp(creation['block_time'])
    dump_time = datetime.fromtimestamp(dump['block_time'])
    lag_seconds = (dump_time - creation_time).total_seconds()
    lag_minutes = lag_seconds / 60
    
    print(f"   ⏱️ Временной лаг: {lag_seconds:.0f} секунд ({lag_minutes:.1f} минут)")
    
    # Проверяем кошельки
    same_wallet = creation['to_wallet'] == dump['from_wallet']
    print(f"   👛 Тот же кошелек: {'Да' if same_wallet else 'Нет'}")
    
    return {
        'token': token_address,
        'creation_time': creation_time,
        'dump_time': dump_time,
        'lag_seconds': lag_seconds,
        'lag_minutes': lag_minutes,
        'creation_amount': creation['from_amount_num'] / 1e9,  # SOL
        'dump_amount': dump['from_amount_num'] / 1e6,  # Млн токенов
        'same_wallet': same_wallet,
        'creation_wallet': creation['to_wallet'],
        'dump_wallet': dump['from_wallet']
    }

def analyze_results(results):
    """Статистический анализ результатов"""
    if len(results) < 2:
        print("\n❌ Недостаточно данных для анализа")
        return
    
    print(f"\n📊 СТАТИСТИЧЕСКИЙ АНАЛИЗ")
    print("=" * 40)
    
    lags = [r['lag_seconds'] for r in results]
    lags_array = np.array(lags)
    
    # Основная статистика
    mean_lag = np.mean(lags_array)
    median_lag = np.median(lags_array)
    std_lag = np.std(lags_array)
    min_lag = np.min(lags_array)
    max_lag = np.max(lags_array)
    
    print(f"📈 Временные лаги (секунды):")
    print(f"   Среднее: {mean_lag:,.0f} сек ({mean_lag/60:.1f} мин)")
    print(f"   Медиана: {median_lag:,.0f} сек ({median_lag/60:.1f} мин)")
    print(f"   Стд. отклонение: {std_lag:,.0f} сек")
    print(f"   Диапазон: {min_lag:,.0f} - {max_lag:,.0f} сек")
    
    # Коэффициент вариации
    cv = std_lag / mean_lag * 100
    print(f"   Коэффициент вариации: {cv:.1f}%")
    
    # Проверка критериев гипотезы H-001
    print(f"\n🎯 ПРОВЕРКА ГИПОТЕЗЫ H-001:")
    
    # Критерий 1: CV < 10%
    criterion_1 = cv < 10
    print(f"   ✅ Критерий 1 (CV < 10%): {'ВЫПОЛНЕН' if criterion_1 else 'НЕ ВЫПОЛНЕН'} (CV = {cv:.1f}%)")
    
    # Критерий 2: 70% в пределах ±5 минут от медианы
    tolerance = 300  # 5 минут
    within_tolerance = sum(1 for lag in lags if abs(lag - median_lag) <= tolerance)
    percentage_within = within_tolerance / len(lags) * 100
    criterion_2 = percentage_within >= 70
    
    print(f"   ✅ Критерий 2 (70% в ±5 мин): {'ВЫПОЛНЕН' if criterion_2 else 'НЕ ВЫПОЛНЕН'} ({percentage_within:.1f}%)")
    
    # Критерий 3: Разброс < 50% от среднего
    range_lag = max_lag - min_lag
    criterion_3 = range_lag < (mean_lag * 0.5)
    print(f"   ✅ Критерий 3 (разброс < 50%): {'ВЫПОЛНЕН' if criterion_3 else 'НЕ ВЫПОЛНЕН'}")
    
    # Итоговый вердикт
    all_criteria = criterion_1 and criterion_2 and criterion_3
    
    print(f"\n🏆 ИТОГОВЫЙ ВЕРДИКТ:")
    if all_criteria:
        print(f"   ✅ ГИПОТЕЗА H-001 ПОДТВЕРЖДЕНА!")
        print(f"   🎯 Обнаружен воспроизводимый временной паттерн: {mean_lag/60:.1f} ± {std_lag/60:.1f} минут")
    elif len(results) == 2 and cv < 20:
        print(f"   🔄 ГИПОТЕЗА H-001 ЧАСТИЧНО ПОДТВЕРЖДЕНА")
        print(f"   💡 Нужно больше данных для окончательного вывода")
    else:
        print(f"   ❌ Гипотеза H-001 не подтверждена")
        print(f"   💡 Возможно, нужно пересмотреть определение событий")

def main():
    """Основная функция"""
    print("🧠 ПРОВЕРКА ГИПОТЕЗЫ H-001: ФИКСИРОВАННЫЙ ВРЕМЕННОЙ ЛАГ")
    print("=" * 60)
    print(f"📊 Анализируем {len(TOKEN_ADDRESSES)} токенов")
    
    results = []
    
    for token_address in TOKEN_ADDRESSES:
        result = analyze_token(token_address)
        if result:
            results.append(result)
    
    print(f"\n📋 РЕЗУЛЬТАТЫ АНАЛИЗА:")
    print("=" * 50)
    
    if results:
        for i, result in enumerate(results, 1):
            print(f"\n{i}. Токен {result['token'][:25]}...")
            print(f"   🎯 Создание: {result['creation_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   💣 Дамп: {result['dump_time'].strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"   ⏱️ Лаг: {result['lag_minutes']:.1f} минут")
            print(f"   💰 Покупка: {result['creation_amount']:.2f} SOL")
            print(f"   💣 Дамп: {result['dump_amount']:.1f}М токенов")
            print(f"   👛 Тот же кошелек: {'Да' if result['same_wallet'] else 'Нет'}")
        
        analyze_results(results)
    else:
        print("❌ Не найдено ни одной пары событий")
        print("💡 Возможные причины:")
        print("   - Пороги слишком высокие")
        print("   - События определены неправильно")
        print("   - Данные неполные")
    
    print(f"\n🎉 АНАЛИЗ ЗАВЕРШЕН")

if __name__ == "__main__":
    main() 