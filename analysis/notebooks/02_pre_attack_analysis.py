#!/usr/bin/env python3
"""
📊 Фаза 2.2.1.1: Анализ событий ПЕРЕД аномальным днем

ЦЕЛЬ: Найти "сигнал-предвестник" - что происходило ДО 13:01:18 1 апреля?
ГИПОТЕЗА: Триггер находится не в самой атаке, а непосредственно перед ней
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Конфигурация анализа
TOKEN_ADDRESS = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC'
ANOMALOUS_DAY_START = '2025-04-01 13:00:00'
ANALYSIS_WINDOW_HOURS = 48  # 48 часов до атаки
DB_PATH = 'db/solana_db.sqlite'

def get_attackers_wallets():
    """Получить список кошельков-участников часа атаки"""
    conn = sqlite3.connect(DB_PATH)
    
    # Кошельки из fee_payer (основные транзакции)
    fee_payer_query = """
    SELECT DISTINCT fee_payer
    FROM transactions 
    WHERE source_query_address = ?
      AND datetime(block_time, 'unixepoch') BETWEEN ? AND ?
    """
    
    attack_start = ANOMALOUS_DAY_START
    attack_end = '2025-04-01 14:00:00'
    
    fee_payers = pd.read_sql_query(
        fee_payer_query, 
        conn, 
        params=[TOKEN_ADDRESS, attack_start, attack_end]
    )['fee_payer'].tolist()
    
    # Кошельки из ML событий
    ml_wallets_query = """
    SELECT DISTINCT e.from_wallet, e.to_wallet
    FROM ml_ready_events e
    JOIN transactions t ON e.signature = t.signature
    WHERE t.source_query_address = ?
      AND datetime(t.block_time, 'unixepoch') BETWEEN ? AND ?
    """
    
    ml_wallets_df = pd.read_sql_query(
        ml_wallets_query, 
        conn, 
        params=[TOKEN_ADDRESS, attack_start, attack_end]
    )
    
    ml_wallets = []
    if len(ml_wallets_df) > 0:
        ml_wallets.extend(ml_wallets_df['from_wallet'].dropna().tolist())
        ml_wallets.extend(ml_wallets_df['to_wallet'].dropna().tolist())
    
    conn.close()
    
    # Объединяем и удаляем дубликаты
    all_attackers = list(set(fee_payers + ml_wallets))
    all_attackers = [w for w in all_attackers if w and w != 'None']
    
    return all_attackers

def analyze_pre_attack_period():
    """Анализ периода ПЕРЕД атакой"""
    print("🔍 АНАЛИЗ ПЕРИОДА ПЕРЕД АТАКОЙ")
    print("=" * 50)
    
    # Временные рамки
    anomaly_start = datetime.strptime(ANOMALOUS_DAY_START, '%Y-%m-%d %H:%M:%S')
    analysis_start = anomaly_start - timedelta(hours=ANALYSIS_WINDOW_HOURS)
    
    print(f"🎯 Токен: {TOKEN_ADDRESS}")
    print(f"⏰ Анализируемый период: {analysis_start.strftime('%Y-%m-%d %H:%M')} - {anomaly_start.strftime('%Y-%m-%d %H:%M')}")
    print(f"📊 Окно анализа: {ANALYSIS_WINDOW_HOURS} часов")
    
    # Получаем список атакующих
    attackers = get_attackers_wallets()
    print(f"👥 Кошельков-участников атаки: {len(attackers)}")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Ищем активность всех кошельков в предшествующий период
    all_activity_query = """
    SELECT 
        signature,
        block_time,
        fee_payer,
        transaction_type,
        datetime(block_time, 'unixepoch') as readable_time
    FROM transactions 
    WHERE source_query_address = ?
      AND datetime(block_time, 'unixepoch') BETWEEN ? AND ?
    ORDER BY block_time
    """
    
    df_pre_activity = pd.read_sql_query(
        all_activity_query,
        conn,
        params=[TOKEN_ADDRESS, analysis_start.strftime('%Y-%m-%d %H:%M:%S'), ANOMALOUS_DAY_START]
    )
    
    print(f"\n📊 ОБЩАЯ АКТИВНОСТЬ ДО АТАКИ:")
    print(f"   Всего транзакций: {len(df_pre_activity):,}")
    print(f"   Уникальных кошельков: {df_pre_activity['fee_payer'].nunique()}")
    
    if len(df_pre_activity) > 0:
        # Распределение по дням
        df_pre_activity['datetime'] = pd.to_datetime(df_pre_activity['block_time'], unit='s')
        df_pre_activity['date'] = df_pre_activity['datetime'].dt.date
        daily_activity = df_pre_activity.groupby('date').size()
        
        print(f"\n📅 Распределение по дням:")
        for date, count in daily_activity.items():
            print(f"   {date}: {count:,} транзакций")
        
        # Проверяем активность участников атаки
        pre_attack_participants = df_pre_activity[df_pre_activity['fee_payer'].isin(attackers)]
        
        print(f"\n🎯 АКТИВНОСТЬ УЧАСТНИКОВ АТАКИ ДО СОБЫТИЯ:")
        print(f"   Участников, активных до атаки: {pre_attack_participants['fee_payer'].nunique()}")
        print(f"   Их транзакций: {len(pre_attack_participants):,}")
        
        if len(pre_attack_participants) > 0:
            print(f"\n📋 Детальная активность участников:")
            for _, tx in pre_attack_participants.iterrows():
                print(f"   {tx['readable_time']} | {tx['fee_payer'][:15]}... | {tx['transaction_type']}")
        else:
            print(f"   ✅ НИ ОДИН участник атаки не проявлял активности!")
            print(f"   🚨 Это сильный сигнал: 'затишье перед бурей'")
    
    # Анализ ML событий
    ml_pre_activity_query = """
    SELECT 
        e.signature,
        e.block_time,
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
    
    df_ml_pre = pd.read_sql_query(
        ml_pre_activity_query,
        conn,
        params=[TOKEN_ADDRESS, analysis_start.strftime('%Y-%m-%d %H:%M:%S'), ANOMALOUS_DAY_START]
    )
    
    print(f"\n🎭 ML СОБЫТИЯ ДО АТАКИ:")
    print(f"   Всего событий: {len(df_ml_pre):,}")
    
    if len(df_ml_pre) > 0:
        # Типы событий
        event_types = df_ml_pre['event_type'].value_counts()
        print(f"   Типы событий:")
        for event_type, count in event_types.items():
            print(f"      {event_type}: {count:,}")
        
        # Проверяем участие будущих атакующих в ML событиях
        ml_attackers_involved = df_ml_pre[
            df_ml_pre['from_wallet'].isin(attackers) | 
            df_ml_pre['to_wallet'].isin(attackers)
        ]
        
        print(f"\n   👥 События с участием будущих атакующих: {len(ml_attackers_involved):,}")
        
        if len(ml_attackers_involved) > 0:
            print(f"   📋 Детали:")
            for _, event in ml_attackers_involved.iterrows():
                print(f"      {event['readable_time']} | {event['event_type']} | {event['from_wallet'][:10]}... → {event['to_wallet'][:10]}...")
    
    conn.close()
    
    return df_pre_activity, df_ml_pre, attackers

def analyze_immediate_precursors():
    """Анализ непосредственных предшественников (последние 6 часов)"""
    print(f"\n🔍 АНАЛИЗ НЕПОСРЕДСТВЕННЫХ ПРЕДШЕСТВЕННИКОВ")
    print("=" * 50)
    
    # Фокусируемся на последних 6 часах перед атакой
    anomaly_start = datetime.strptime(ANOMALOUS_DAY_START, '%Y-%m-%d %H:%M:%S')
    immediate_start = anomaly_start - timedelta(hours=6)
    
    print(f"⏰ Период: {immediate_start.strftime('%Y-%m-%d %H:%M')} - {anomaly_start.strftime('%Y-%m-%d %H:%M')}")
    
    conn = sqlite3.connect(DB_PATH)
    
    # Все транзакции в последние 6 часов
    immediate_query = """
    SELECT 
        signature,
        block_time,
        fee_payer,
        transaction_type,
        datetime(block_time, 'unixepoch') as readable_time
    FROM transactions 
    WHERE source_query_address = ?
      AND datetime(block_time, 'unixepoch') BETWEEN ? AND ?
    ORDER BY block_time DESC
    """
    
    df_immediate = pd.read_sql_query(
        immediate_query,
        conn,
        params=[TOKEN_ADDRESS, immediate_start.strftime('%Y-%m-%d %H:%M:%S'), ANOMALOUS_DAY_START]
    )
    
    print(f"📊 Транзакций в последние 6 часов: {len(df_immediate):,}")
    
    if len(df_immediate) > 0:
        print(f"\n⏰ БЛИЖАЙШИЕ К АТАКЕ СОБЫТИЯ:")
        # Показываем последние 10 транзакций перед атакой
        for _, tx in df_immediate.head(10).iterrows():
            time_diff = anomaly_start - datetime.strptime(tx['readable_time'], '%Y-%m-%d %H:%M:%S')
            hours_before = time_diff.total_seconds() / 3600
            print(f"   -{hours_before:.1f}ч | {tx['readable_time']} | {tx['fee_payer'][:15]}... | {tx['transaction_type']}")
    else:
        print(f"   ✅ ПОЛНОЕ ЗАТИШЬЕ в последние 6 часов!")
        print(f"   🚨 Это классический паттерн координированной атаки")
    
    conn.close()

def search_for_triggers(df_pre_activity, attackers):
    """Поиск потенциальных триггеров"""
    print(f"\n🎯 ПОИСК ПОТЕНЦИАЛЬНЫХ ТРИГГЕРОВ")
    print("=" * 40)
    
    triggers_found = []
    
    # 1. Паттерн "затишье перед бурей"
    if len(df_pre_activity) == 0:
        triggers_found.append("Полное затишье перед атакой")
        print(f"🚨 ТРИГГЕР #1: ПОЛНОЕ ЗАТИШЬЕ")
        print(f"   ⚠️ Отсутствие активности за {ANALYSIS_WINDOW_HOURS}ч до атаки")
        print(f"   💡 Указывает на заранее спланированную координированную атаку")
    
    # 2. Низкая активность
    elif len(df_pre_activity) < 10:
        triggers_found.append("Аномально низкая предварительная активность")
        print(f"🚨 ТРИГГЕР #2: АНОМАЛЬНО НИЗКАЯ АКТИВНОСТЬ")
        print(f"   📊 Всего {len(df_pre_activity):,} транзакций за {ANALYSIS_WINDOW_HOURS}ч")
        print(f"   💡 На фоне 1,063 транзакций за 1 час - очевидная аномалия")
    
    # 3. Проверка на координацию времени
    if len(df_pre_activity) > 0:
        df_pre_activity['datetime'] = pd.to_datetime(df_pre_activity['block_time'], unit='s')
        
        # Ищем кластеры активности
        df_pre_activity['hour'] = df_pre_activity['datetime'].dt.hour
        hourly_activity = df_pre_activity.groupby('hour').size()
        
        if hourly_activity.max() > 5:  # Если в какой-то час было больше 5 транзакций
            peak_hour = hourly_activity.idxmax()
            peak_count = hourly_activity.max()
            triggers_found.append("Предварительный пик активности")
            print(f"🚨 ТРИГГЕР #3: ПРЕДВАРИТЕЛЬНЫЙ ПИК")
            print(f"   ⏰ В {peak_hour}:00 было {peak_count} транзакций")
            print(f"   💡 Возможная подготовительная фаза")
    
    # Резюме
    print(f"\n📋 РЕЗЮМЕ ПОИСКА ТРИГГЕРОВ:")
    if triggers_found:
        print(f"   ✅ Найдено триггеров: {len(triggers_found)}")
        for i, trigger in enumerate(triggers_found, 1):
            print(f"      {i}. {trigger}")
    else:
        print(f"   ⚠️ Явные триггеры не обнаружены")
    
    return triggers_found

def main():
    """Главная функция анализа предвестников"""
    print("🎯 ФАЗА 2.2.1.1: ПОИСК СИГНАЛОВ-ПРЕДВЕСТНИКОВ")
    print("=" * 60)
    print(f"📅 Цель: Найти триггер ДО атаки 1 апреля 13:00")
    print(f"📊 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Основной анализ
    df_pre_activity, df_ml_pre, attackers = analyze_pre_attack_period()
    
    # Анализ непосредственных предшественников
    analyze_immediate_precursors()
    
    # Поиск триггеров
    triggers = search_for_triggers(df_pre_activity, attackers)
    
    print(f"\n🎉 АНАЛИЗ ПРЕДВЕСТНИКОВ ЗАВЕРШЕН")
    print(f"📊 Результат: {'Найдены сигналы' if triggers else 'Подтверждено затишье перед бурей'}")
    print(f"🎯 Следующий шаг: Поминутный анализ часа атаки (13:00-13:59)")

if __name__ == "__main__":
    main() 