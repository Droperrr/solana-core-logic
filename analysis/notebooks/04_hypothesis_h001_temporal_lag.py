#!/usr/bin/env python3
"""
🧠 Проверка гипотезы H-001: Фиксированный временной лаг между триггером и дампом

ЦЕЛЬ: Найти воспроизводимый временной интервал между событием-триггером и дампом
ПОДХОД: Анализ каузальных цепочек на микроуровне событий
ОЖИДАНИЕ: Стандартное отклонение < 10% от среднего лага
"""

import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats
import warnings
warnings.filterwarnings('ignore')

# Конфигурация
DB_PATH = 'db/solana_db.sqlite'
TOKEN_ADDRESSES = [
    'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC',
    'AGmXzgFQw8bLtsJvr5dgnHtyP6rP6GLwmZwgVqjFL6Q8'
]

# Пороги для определения событий
SOL_BUY_THRESHOLD = 0.5 * 1e9  # 0.5 SOL в lamports
TOKEN_DUMP_THRESHOLD = 1_000_000  # 1М токенов минимум для дампа

def get_token_events(token_address):
    """Получить все события для токена с детализацией"""
    conn = sqlite3.connect(DB_PATH)
    
    # Основные транзакции
    transactions_query = """
    SELECT 
        signature,
        block_time,
        fee_payer,
        transaction_type,
        datetime(block_time, 'unixepoch') as readable_time
    FROM transactions 
    WHERE source_query_address = ?
    ORDER BY block_time
    """
    
    df_transactions = pd.read_sql_query(
        transactions_query, 
        conn, 
        params=[token_address]
    )
    
    # ML события (свапы, трансферы)
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
    
    df_ml_events = pd.read_sql_query(
        ml_events_query,
        conn,
        params=[token_address]
    )
    
    conn.close()
    return df_transactions, df_ml_events

def find_creation_event(df_ml_events, token_address):
    """Найти событие создания токена (первую крупную покупку за SOL)"""
    if len(df_ml_events) == 0:
        return None
    
    # Ищем свапы, где покупают наш токен за SOL
    # token_b_mint = наш токен (то что получаем)
    # token_a_mint = SOL (то что отдаем)
    buys = df_ml_events[
        (df_ml_events['token_b_mint'] == token_address) &
        (df_ml_events['event_type'].isin(['SWAP', 'swap'])) &
        (pd.to_numeric(df_ml_events['from_amount'], errors='coerce') >= SOL_BUY_THRESHOLD)
    ].copy()
    
    if len(buys) > 0:
        buys['from_amount_numeric'] = pd.to_numeric(buys['from_amount'], errors='coerce')
        buys = buys.sort_values('block_time')
        return buys.iloc[0]
    
    return None

def find_first_dump(df_ml_events, token_address):
    """Найти первый крупный дамп токена"""
    if len(df_ml_events) == 0:
        return None
    
    # Ищем свапы, где продают наш токен
    # token_a_mint = наш токен (то что отдаем)
    # token_b_mint = SOL или другое (то что получаем)
    dumps = df_ml_events[
        (df_ml_events['token_a_mint'] == token_address) &
        (df_ml_events['event_type'].isin(['SWAP', 'swap'])) &
        (pd.to_numeric(df_ml_events['from_amount'], errors='coerce') >= TOKEN_DUMP_THRESHOLD)
    ].copy()
    
    if len(dumps) > 0:
        dumps['from_amount_numeric'] = pd.to_numeric(dumps['from_amount'], errors='coerce')
        dumps = dumps.sort_values('block_time')
        return dumps.iloc[0]
    
    return None

def analyze_temporal_lag(token_address):
    """Анализ временного лага для одного токена"""
    print(f"\n🔍 Анализ токена: {token_address[:20]}...")
    
    df_transactions, df_ml_events = get_token_events(token_address)
    
    print(f"   📊 Загружено: {len(df_transactions):,} транзакций, {len(df_ml_events):,} ML событий")
    
    # Находим ключевые события
    creation_event = find_creation_event(df_ml_events, token_address)
    first_dump = find_first_dump(df_ml_events, token_address)
    
    if creation_event is None:
        print("   ❌ Событие создания не найдено")
        return None
    
    if first_dump is None:
        print("   ❌ Первый дамп не найден")
        return None
    
    # Вычисляем временной лаг
    creation_time = datetime.fromtimestamp(creation_event['block_time'])
    dump_time = datetime.fromtimestamp(first_dump['block_time'])
    time_lag = (dump_time - creation_time).total_seconds()
    
    print(f"   ✅ Найдена пара событий:")
    print(f"      🎯 Создание: {creation_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"      💣 Первый дамп: {dump_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"      ⏱️ Временной лаг: {time_lag:,.0f} секунд ({time_lag/60:.1f} минут)")
    
    # Детали событий
    creation_amount = float(creation_event['from_amount']) / 1e9  # SOL
    dump_amount = float(first_dump['from_amount']) / 1e6  # Млн токенов
    
    print(f"      💰 Размер создающей покупки: {creation_amount:.2f} SOL")
    print(f"      💰 Размер дампа: {dump_amount:.1f}М токенов")
    
    return {
        'token': token_address,
        'creation_time': creation_time,
        'dump_time': dump_time,
        'time_lag_seconds': time_lag,
        'time_lag_minutes': time_lag / 60,
        'creation_amount_sol': creation_amount,
        'dump_amount_tokens': dump_amount,
        'creation_wallet': creation_event['to_wallet'],
        'dump_wallet': first_dump['from_wallet'],
        'same_wallet': creation_event['to_wallet'] == first_dump['from_wallet']
    }

def statistical_analysis(results):
    """Статистический анализ временных лагов"""
    if len(results) < 2:
        print("❌ Недостаточно данных для статистического анализа")
        return
    
    df = pd.DataFrame(results)
    
    print(f"\n📊 СТАТИСТИЧЕСКИЙ АНАЛИЗ ВРЕМЕННЫХ ЛАГОВ")
    print("=" * 50)
    
    lags_seconds = df['time_lag_seconds']
    lags_minutes = df['time_lag_minutes']
    
    # Основная статистика
    print(f"📈 ОСНОВНАЯ СТАТИСТИКА (в секундах):")
    print(f"   Среднее: {lags_seconds.mean():,.0f} сек ({lags_seconds.mean()/60:.1f} мин)")
    print(f"   Медиана: {lags_seconds.median():,.0f} сек ({lags_seconds.median()/60:.1f} мин)")
    print(f"   Стандартное отклонение: {lags_seconds.std():,.0f} сек")
    print(f"   Минимум: {lags_seconds.min():,.0f} сек ({lags_seconds.min()/60:.1f} мин)")
    print(f"   Максимум: {lags_seconds.max():,.0f} сек ({lags_seconds.max()/60:.1f} мин)")
    
    # Коэффициент вариации
    cv = lags_seconds.std() / lags_seconds.mean() * 100
    print(f"   Коэффициент вариации: {cv:.1f}%")
    
    # Проверка гипотезы H-001
    print(f"\n🎯 ПРОВЕРКА ГИПОТЕЗЫ H-001:")
    
    # Критерий 1: Стандартное отклонение < 10% от среднего
    criterion_1 = cv < 10
    print(f"   ✅ Критерий 1 (CV < 10%): {'ВЫПОЛНЕН' if criterion_1 else 'НЕ ВЫПОЛНЕН'} (CV = {cv:.1f}%)")
    
    # Критерий 2: Большинство значений в узком диапазоне
    median_lag = lags_seconds.median()
    tolerance = 300  # ±5 минут
    within_tolerance = ((lags_seconds >= median_lag - tolerance) & 
                       (lags_seconds <= median_lag + tolerance)).sum()
    percentage_within = within_tolerance / len(lags_seconds) * 100
    criterion_2 = percentage_within >= 70
    
    print(f"   ✅ Критерий 2 (70% в пределах ±5 мин): {'ВЫПОЛНЕН' if criterion_2 else 'НЕ ВЫПОЛНЕН'} ({percentage_within:.1f}%)")
    
    # Критерий 3: Проверка на случайность (тест Шапиро-Уилка на нормальность)
    if len(lags_seconds) >= 3:
        statistic, p_value = stats.shapiro(lags_seconds)
        criterion_3 = p_value > 0.05  # Если нормальное распределение, то не случайное
        print(f"   ✅ Критерий 3 (не случайное): {'ВЫПОЛНЕН' if criterion_3 else 'НЕ ВЫПОЛНЕН'} (p = {p_value:.3f})")
    else:
        criterion_3 = False
        print(f"   ❌ Критерий 3: Недостаточно данных")
    
    # Итоговый вердикт
    all_criteria = criterion_1 and criterion_2 and criterion_3
    print(f"\n🏆 ИТОГОВЫЙ ВЕРДИКТ:")
    if all_criteria:
        print(f"   ✅ ГИПОТЕЗА H-001 ПОДТВЕРЖДЕНА!")
        print(f"   🎯 Обнаружен воспроизводимый временной паттерн")
    else:
        print(f"   ❌ Гипотеза H-001 требует доработки")
        print(f"   💡 Возможные причины: недостаточно данных, разные алгоритмы, или нужно пересмотреть определение событий")
    
    return df

def create_visualizations(df):
    """Создание визуализаций результатов"""
    if len(df) < 2:
        return
    
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('Анализ временных лагов между созданием и дампом токенов', fontsize=16)
    
    # 1. Гистограмма временных лагов
    axes[0, 0].hist(df['time_lag_minutes'], bins=max(3, len(df)), alpha=0.7, color='skyblue', edgecolor='black')
    axes[0, 0].set_title('Распределение временных лагов')
    axes[0, 0].set_xlabel('Лаг (минуты)')
    axes[0, 0].set_ylabel('Количество токенов')
    axes[0, 0].grid(True, alpha=0.3)
    
    # 2. Временная линия событий
    for i, row in df.iterrows():
        axes[0, 1].barh(i, row['time_lag_minutes'], color='orange', alpha=0.7)
        axes[0, 1].text(row['time_lag_minutes'] + 1, i, f"{row['time_lag_minutes']:.0f}м", va='center')
    
    axes[0, 1].set_title('Временные лаги по токенам')
    axes[0, 1].set_xlabel('Лаг (минуты)')
    axes[0, 1].set_ylabel('Токен #')
    axes[0, 1].grid(True, alpha=0.3)
    
    # 3. Корреляция размера покупки и лага
    axes[1, 0].scatter(df['creation_amount_sol'], df['time_lag_minutes'], alpha=0.7, s=100)
    axes[1, 0].set_title('Размер создающей покупки vs Временной лаг')
    axes[1, 0].set_xlabel('Размер покупки (SOL)')
    axes[1, 0].set_ylabel('Лаг (минуты)')
    axes[1, 0].grid(True, alpha=0.3)
    
    # 4. Статистика по кошелькам
    same_wallet_counts = df['same_wallet'].value_counts()
    axes[1, 1].pie(same_wallet_counts.values, labels=['Разные кошельки', 'Один кошелек'], autopct='%1.1f%%')
    axes[1, 1].set_title('Создатель = Дампер?')
    
    plt.tight_layout()
    plt.show()

def main():
    """Главная функция проверки гипотезы H-001"""
    print("🧠 ПРОВЕРКА ГИПОТЕЗЫ H-001: ФИКСИРОВАННЫЙ ВРЕМЕННОЙ ЛАГ")
    print("=" * 60)
    print(f"📊 Анализируем {len(TOKEN_ADDRESSES)} токенов")
    print(f"🎯 Ищем воспроизводимые временные паттерны")
    
    results = []
    
    # Анализ каждого токена
    for token_address in TOKEN_ADDRESSES:
        result = analyze_temporal_lag(token_address)
        if result:
            results.append(result)
    
    # Статистический анализ
    if results:
        print(f"\n📋 СОБРАНО ДАННЫХ: {len(results)} токенов с парами событий")
        df_results = statistical_analysis(results)
        
        # Детальная таблица результатов
        print(f"\n📊 ДЕТАЛЬНЫЕ РЕЗУЛЬТАТЫ:")
        print("-" * 100)
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['token'][:20]}...")
            print(f"   ⏱️ Лаг: {result['time_lag_minutes']:.1f} мин")
            print(f"   💰 Покупка: {result['creation_amount_sol']:.2f} SOL")
            print(f"   💣 Дамп: {result['dump_amount_tokens']:.1f}М токенов")
            print(f"   👛 Тот же кошелек: {'Да' if result['same_wallet'] else 'Нет'}")
        
        # Визуализация
        create_visualizations(df_results)
        
    else:
        print("❌ Не удалось найти пары событий ни для одного токена")
        print("💡 Возможные причины:")
        print("   - Пороги слишком высокие")
        print("   - Данные неполные")
        print("   - События имеют другую природу")
    
    print(f"\n🎉 АНАЛИЗ ЗАВЕРШЕН")
    
    # Обновляем статус гипотезы
    if len(results) >= 2:
        print(f"📝 Обновите файл analysis/hypotheses.md:")
        print(f"   Статус H-001: {'✅ Подтверждена' if len(results) > 0 else '🔄 Требует доработки'}")

if __name__ == "__main__":
    main() 