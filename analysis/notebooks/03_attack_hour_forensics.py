#!/usr/bin/env python3
"""
📊 Фаза 2.2.1.2: Поминутная реконструкция "часа атаки"

ЦЕЛЬ: Понять механику и последовательность скоординированных действий
ПЕРИОД: 1 апреля 2025 г. с 13:00 до 13:59
ФОКУС: Первая транзакция-триггер и развитие атаки
"""

import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Конфигурация анализа
TOKEN_ADDRESS = 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC'
ATTACK_HOUR_START = '2025-04-01 13:00:00'
ATTACK_HOUR_END = '2025-04-01 13:59:59'
DB_PATH = 'db/solana_db.sqlite'

def get_attack_hour_data():
    """Получить все данные за час атаки"""
    conn = sqlite3.connect(DB_PATH)
    
    # Основные транзакции
    transactions_query = """
    SELECT 
        signature,
        block_time,
        fee_payer,
        transaction_type,
        datetime(block_time, 'unixepoch') as readable_time,
        strftime('%M', datetime(block_time, 'unixepoch')) as minute
    FROM transactions 
    WHERE source_query_address = ?
      AND datetime(block_time, 'unixepoch') BETWEEN ? AND ?
    ORDER BY block_time
    """
    
    df_transactions = pd.read_sql_query(
        transactions_query,
        conn,
        params=[TOKEN_ADDRESS, ATTACK_HOUR_START, ATTACK_HOUR_END]
    )
    
    # ML события
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
        datetime(e.block_time, 'unixepoch') as readable_time,
        strftime('%M', datetime(e.block_time, 'unixepoch')) as minute,
        strftime('%S', datetime(e.block_time, 'unixepoch')) as second
    FROM ml_ready_events e
    JOIN transactions t ON e.signature = t.signature
    WHERE t.source_query_address = ?
      AND datetime(e.block_time, 'unixepoch') BETWEEN ? AND ?
    ORDER BY e.block_time
    """
    
    df_ml_events = pd.read_sql_query(
        ml_events_query,
        conn,
        params=[TOKEN_ADDRESS, ATTACK_HOUR_START, ATTACK_HOUR_END]
    )
    
    conn.close()
    return df_transactions, df_ml_events

def analyze_first_transactions(df_transactions, df_ml_events):
    """Анализ первых транзакций - поиск триггера"""
    print("🎯 АНАЛИЗ ПЕРВЫХ ТРАНЗАКЦИЙ - ПОИСК ТРИГГЕРА")
    print("=" * 50)
    
    if len(df_transactions) > 0:
        # Первые 10 транзакций
        first_10 = df_transactions.head(10)
        
        print(f"📊 Первые 10 транзакций атаки:")
        print(f"{'⏰ Время':<20} {'👛 Кошелек':<20} {'📋 Тип':<15}")
        print("-" * 60)
        
        for i, tx in first_10.iterrows():
            wallet = tx['fee_payer'] if tx['fee_payer'] and tx['fee_payer'] != 'Unknown' else "Unknown"
            wallet_short = wallet[:15] + "..." if len(wallet) > 15 else wallet
            print(f"{tx['readable_time']:<20} {wallet_short:<20} {tx['transaction_type']:<15}")
        
        # Анализ первой транзакции
        first_tx = df_transactions.iloc[0]
        print(f"\n🚨 ПЕРВАЯ ТРАНЗАКЦИЯ (ПОТЕНЦИАЛЬНЫЙ ТРИГГЕР):")
        print(f"   ⏰ Время: {first_tx['readable_time']}")
        print(f"   👛 Кошелек: {first_tx['fee_payer'] or 'Unknown'}")
        print(f"   📋 Тип: {first_tx['transaction_type']}")
        print(f"   🔗 Сигнатура: {first_tx['signature']}")
        
        # Проверяем ML события для первой транзакции
        if len(df_ml_events) > 0:
            first_ml_events = df_ml_events[df_ml_events['signature'] == first_tx['signature']]
            if len(first_ml_events) > 0:
                print(f"   🎭 ML события в первой транзакции:")
                for _, event in first_ml_events.iterrows():
                    from_wallet = event['from_wallet'] or 'Unknown'
                    to_wallet = event['to_wallet'] or 'Unknown'
                    print(f"      • {event['event_type']}: {from_wallet[:10]}... → {to_wallet[:10]}...")
                    if event['from_amount'] and event['to_amount']:
                        try:
                            from_amt = float(event['from_amount'])
                            to_amt = float(event['to_amount'])
                            print(f"        💰 {from_amt:,.0f} → {to_amt:,.0f}")
                        except (ValueError, TypeError):
                            print(f"        💰 {event['from_amount']} → {event['to_amount']}")
    
    return first_tx if len(df_transactions) > 0 else None

def minute_by_minute_analysis(df_transactions, df_ml_events):
    """Поминутный анализ активности"""
    print(f"\n📊 ПОМИНУТНЫЙ АНАЛИЗ АКТИВНОСТИ")
    print("=" * 40)
    
    # Подсчет транзакций по минутам
    minute_counts = df_transactions.groupby('minute').agg({
        'signature': 'nunique',
        'fee_payer': 'nunique'
    }).rename(columns={'signature': 'transactions', 'fee_payer': 'unique_wallets'})
    
    # Подсчет ML событий по минутам (если есть)
    if len(df_ml_events) > 0:
        ml_minute_counts = df_ml_events.groupby('minute').agg({
            'signature': 'nunique',
            'from_amount': lambda x: pd.to_numeric(x, errors='coerce').sum(),
            'to_amount': lambda x: pd.to_numeric(x, errors='coerce').sum()
        }).rename(columns={'signature': 'ml_events'})
        
        # Объединяем данные
        combined_analysis = minute_counts.join(ml_minute_counts, how='outer').fillna(0)
    else:
        # Если ML событий нет, создаем пустые столбцы
        combined_analysis = minute_counts.copy()
        combined_analysis['ml_events'] = 0
        combined_analysis['from_amount'] = 0
        combined_analysis['to_amount'] = 0
    
    print(f"📈 Поминутное распределение:")
    print(f"{'Мин':<5} {'Транз':<8} {'Кошел':<8} {'ML соб':<8} {'Объем (млн)':<12}")
    print("-" * 50)
    
    total_volume = 0
    peak_minute = None
    peak_transactions = 0
    
    # Перебираем все минуты из индекса
    for minute in combined_analysis.index:
        minute_int = int(minute)
        minute_str = f"{minute_int:02d}"
        row = combined_analysis.loc[minute]
        
        transactions = int(row['transactions'])
        wallets = int(row['unique_wallets'])
        ml_events = int(row['ml_events'])
        
        # Безопасное вычисление объема
        from_amount = row.get('from_amount', 0) or 0
        to_amount = row.get('to_amount', 0) or 0
        volume_millions = (from_amount + to_amount) / 2_000_000  # Средний объем в млн
        total_volume += volume_millions
        
        # Отмечаем пиковую минуту
        marker = "🔥" if transactions > peak_transactions else "  "
        if transactions > peak_transactions:
            peak_transactions = transactions
            peak_minute = minute_str
        
        print(f"{minute_str:<5} {transactions:<8} {wallets:<8} {ml_events:<8} {volume_millions:<12.1f} {marker}")
    
    print(f"\n🎯 КЛЮЧЕВЫЕ МЕТРИКИ:")
    print(f"   🔥 Пиковая минута: 13:{peak_minute} ({peak_transactions:,} транзакций)")
    print(f"   💰 Общий объем торгов: {total_volume:,.1f} млн токенов")
    if len(df_transactions) > 0:
        print(f"   📊 Средняя активность: {len(df_transactions) / 60:.1f} транзакций/мин")
    
    return combined_analysis, peak_minute

def analyze_coordination_patterns(df_transactions, df_ml_events):
    """Анализ паттернов координации"""
    print(f"\n🎭 АНАЛИЗ ПАТТЕРНОВ КООРДИНАЦИИ")
    print("=" * 35)
    
    # Анализ времени между транзакциями
    df_transactions['datetime'] = pd.to_datetime(df_transactions['block_time'], unit='s')
    df_transactions['time_diff'] = df_transactions['datetime'].diff().dt.total_seconds()
    
    # Статистика интервалов
    intervals = df_transactions['time_diff'].dropna()
    
    print(f"⏱️ ВРЕМЕННЫЕ ИНТЕРВАЛЫ МЕЖДУ ТРАНЗАКЦИЯМИ:")
    print(f"   Медианный интервал: {intervals.median():.1f} секунд")
    print(f"   Средний интервал: {intervals.mean():.1f} секунд")
    print(f"   Минимальный интервал: {intervals.min():.1f} секунд")
    print(f"   Максимальный интервал: {intervals.max():.1f} секунд")
    
    # Поиск всплесков активности (интервалы < 5 секунд)
    rapid_transactions = intervals[intervals < 5].count()
    print(f"   🚨 Транзакций с интервалом < 5 сек: {rapid_transactions} ({rapid_transactions/len(intervals)*100:.1f}%)")
    
    # Анализ уникальных кошельков
    unique_wallets = df_transactions['fee_payer'].nunique()
    total_transactions = len(df_transactions)
    
    print(f"\n👥 АНАЛИЗ УЧАСТНИКОВ:")
    print(f"   Уникальных кошельков: {unique_wallets}")
    print(f"   Общих транзакций: {total_transactions:,}")
    print(f"   Среднее транзакций на кошелек: {total_transactions/unique_wallets:.1f}")
    
    # Топ активных кошельков
    wallet_activity = df_transactions['fee_payer'].value_counts().head(5)
    print(f"\n🏆 ТОП-5 АКТИВНЫХ КОШЕЛЬКОВ:")
    for i, (wallet, count) in enumerate(wallet_activity.items(), 1):
        percentage = count / total_transactions * 100
        print(f"   {i}. {wallet[:20]}... : {count:,} транзакций ({percentage:.1f}%)")
    
    # Проверка на координацию (одинаковые временные метки)
    same_timestamp_groups = df_transactions.groupby('block_time').size()
    coordinated_groups = same_timestamp_groups[same_timestamp_groups > 1]
    
    if len(coordinated_groups) > 0:
        print(f"\n🎯 СИНХРОНИЗИРОВАННЫЕ ГРУППЫ:")
        print(f"   Групп с одинаковым временем: {len(coordinated_groups)}")
        print(f"   Максимальный размер группы: {coordinated_groups.max()} транзакций")
        
        # Показываем крупнейшие синхронизированные группы
        top_sync_groups = coordinated_groups.sort_values(ascending=False).head(3)
        for timestamp, count in top_sync_groups.items():
            readable_time = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
            print(f"      {readable_time}: {count} синхронных транзакций")

def analyze_trading_patterns(df_ml_events):
    """Анализ торговых паттернов"""
    print(f"\n💹 АНАЛИЗ ТОРГОВЫХ ПАТТЕРНОВ")
    print("=" * 30)
    
    if len(df_ml_events) == 0:
        print("⚠️ ML события не найдены")
        return
    
    # Типы событий
    event_types = df_ml_events['event_type'].value_counts()
    print(f"📊 ТИПЫ СОБЫТИЙ:")
    for event_type, count in event_types.items():
        percentage = count / len(df_ml_events) * 100
        print(f"   {event_type}: {count:,} ({percentage:.1f}%)")
    
    # Анализ объемов
    df_ml_events['volume'] = pd.to_numeric(df_ml_events['from_amount'], errors='coerce')
    volumes = df_ml_events['volume'].dropna()
    
    if len(volumes) > 0:
        print(f"\n💰 АНАЛИЗ ОБЪЕМОВ:")
        print(f"   Общий объем: {volumes.sum():,.0f} токенов")
        print(f"   Средний объем: {volumes.mean():,.0f} токенов")
        print(f"   Медианный объем: {volumes.median():,.0f} токенов")
        print(f"   Максимальная сделка: {volumes.max():,.0f} токенов")
        
        # Крупные сделки (> среднего + 2σ)
        large_threshold = volumes.mean() + 2 * volumes.std()
        large_trades = volumes[volumes > large_threshold]
        print(f"   Крупных сделок (>{large_threshold:,.0f}): {len(large_trades)}")

def create_attack_timeline(df_transactions, df_ml_events):
    """Создание временной линии атаки"""
    print(f"\n📅 ВРЕМЕННАЯ ЛИНИЯ АТАКИ")
    print("=" * 25)
    
    # Разбиваем час на 5-минутные интервалы
    intervals = ['00-04', '05-09', '10-14', '15-19', '20-24', '25-29', 
                '30-34', '35-39', '40-44', '45-49', '50-54', '55-59']
    
    print(f"📊 Активность по 5-минутным интервалам:")
    print(f"{'Интервал':<10} {'Транзакции':<12} {'ML события':<12} {'Статус':<15}")
    print("-" * 55)
    
    for interval in intervals:
        start_min, end_min = map(int, interval.split('-'))
        
        # Фильтруем транзакции для этого интервала
        interval_transactions = df_transactions[
            (df_transactions['minute'].astype(int) >= start_min) & 
            (df_transactions['minute'].astype(int) <= end_min)
        ]
        
        interval_ml = df_ml_events[
            (df_ml_events['minute'].astype(int) >= start_min) & 
            (df_ml_events['minute'].astype(int) <= end_min)
        ]
        
        tx_count = len(interval_transactions)
        ml_count = len(interval_ml)
        
        # Определяем статус интервала
        if tx_count > 100:
            status = "🔥 ПИКОВЫЙ"
        elif tx_count > 10:
            status = "📈 Активный"
        elif tx_count > 0:
            status = "📊 Низкий"
        else:
            status = "💤 Тишина"
        
        print(f"13:{interval:<7} {tx_count:<12} {ml_count:<12} {status:<15}")

def main():
    """Главная функция поминутного анализа"""
    print("🎯 ФАЗА 2.2.1.2: ПОМИНУТНАЯ РЕКОНСТРУКЦИЯ ЧАСА АТАКИ")
    print("=" * 65)
    print(f"📅 Период анализа: {ATTACK_HOUR_START} - {ATTACK_HOUR_END}")
    print(f"🎯 Цель: Реконструировать механику скоординированной атаки")
    print(f"📊 Время запуска: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Загрузка данных
    df_transactions, df_ml_events = get_attack_hour_data()
    
    print(f"\n📊 ЗАГРУЖЕНО ДАННЫХ:")
    print(f"   Транзакций: {len(df_transactions):,}")
    print(f"   ML событий: {len(df_ml_events):,}")
    print(f"   Уникальных кошельков: {df_transactions['fee_payer'].nunique()}")
    
    # Анализы
    first_tx = analyze_first_transactions(df_transactions, df_ml_events)
    combined_analysis, peak_minute = minute_by_minute_analysis(df_transactions, df_ml_events)
    analyze_coordination_patterns(df_transactions, df_ml_events)
    analyze_trading_patterns(df_ml_events)
    create_attack_timeline(df_transactions, df_ml_events)
    
    print(f"\n🎉 ПОМИНУТНАЯ РЕКОНСТРУКЦИЯ ЗАВЕРШЕНА")
    print(f"📊 Ключевые находки:")
    print(f"   🚨 Триггер: {first_tx['readable_time'] if first_tx is not None else 'Не найден'}")
    print(f"   🔥 Пиковая минута: 13:{peak_minute if peak_minute else 'Не определена'}")
    print(f"   👥 Участников: {df_transactions['fee_payer'].nunique()}")
    print(f"   💣 Общий урон: {len(df_transactions):,} транзакций за 1 час")

if __name__ == "__main__":
    main() 