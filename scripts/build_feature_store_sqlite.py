#!/usr/bin/env python3
"""
Скрипт для построения хранилища признаков (feature store) для SQLite на основе данных из таблицы enhanced_ml_events.
Используется для расчета и сохранения признаков, необходимых для ML-моделей.

Рефакторенная версия: использует функции из analysis.feature_library для расчета признаков.
"""
import os
import sys
import time
import logging
import argparse
import datetime
import json
import sqlite3
import pandas as pd
import numpy as np
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Импортируем модули аналитики
from analysis.data_provider import get_all_events_for_token, get_db_connection, dict_factory, get_all_events
from analysis.feature_library import (
    calculate_daily_volume,
    calculate_transaction_count,
    calculate_unique_wallets,
    calculate_price_change,
    calculate_buy_sell_ratio,
    calculate_gini_coefficient,
    calculate_concentration_top5,
    calculate_external_to_internal_volume_ratio,
    calculate_liquidity_change_velocity,
    get_wallet_balances_for_token,
    find_largest_sol_buy,
    find_largest_sol_sell,
    analyze_sol_trading_pattern
)

# Создаем директорию для логов, если её нет
Path("logs").mkdir(exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/build_feature_store_sqlite.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("feature_store_sqlite")

def create_feature_store_table(conn):
    """
    Создает таблицу feature_store_daily, если она не существует.
    
    Args:
        conn: Соединение с базой данных SQLite.
    """
    try:
        logger.info("Создание таблицы feature_store_daily")
        cursor = conn.cursor()
        
        # Создаем таблицу feature_store_daily
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS feature_store_daily (
            token_address TEXT NOT NULL,
            date TEXT NOT NULL,
            volume_24h REAL,
            transaction_count_24h INTEGER,
            unique_wallets_24h INTEGER,
            price_change_pct_24h REAL,
            volatility_24h REAL,
            buy_sell_ratio_24h REAL,
            gini_coefficient REAL,
            concentration_top5_pct REAL,
            external_to_internal_volume_ratio REAL,
            liquidity_change_velocity REAL,
            largest_sol_buy_amount REAL,
            largest_sol_buy_wallet TEXT,
            largest_sol_sell_amount REAL,
            largest_sol_sell_wallet TEXT,
            total_sol_buy_volume REAL,
            total_sol_sell_volume REAL,
            sol_buy_sell_ratio REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (token_address, date)
        );
        """)
        
        # Создаем индексы для быстрого доступа
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_token ON feature_store_daily(token_address);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_date ON feature_store_daily(date);")
        
        conn.commit()
        logger.info("Таблица feature_store_daily успешно создана")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при создании таблицы feature_store_daily: {str(e)}", exc_info=True)
        raise

def load_events_data_for_calculation(conn, token_address: str = None, date_range: tuple = None) -> pd.DataFrame:
    """
    Загружает данные событий для расчета признаков через data_provider.
    
    Args:
        conn: Соединение с базой данных SQLite (не используется, для совместимости)
        token_address: Адрес конкретного токена. Если None, загружаются все события.
        date_range: Диапазон дат для загрузки всех событий.
        
    Returns:
        pandas.DataFrame: DataFrame с событиями
    """
    try:
        if token_address:
            logger.info(f"Загрузка событий для токена {token_address} через data_provider")
            events_df = get_all_events_for_token(token_address)
        else:
            logger.info(f"Загрузка ВСЕХ событий в диапазоне {date_range} через data_provider")
            events_df = get_all_events(date_range)

        if events_df.empty:
            logger.warning(f"Не найдены события для токена {token_address or 'ALL'}")
            return pd.DataFrame()
        
        logger.info(f"Загружено {len(events_df)} событий")
        return events_df
        
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных событий: {str(e)}")
        return pd.DataFrame()

def build_features_for_token(events_df: pd.DataFrame, token_address: str, date: str = None) -> dict:
    """
    Рассчитывает все признаки для указанного токена и даты, используя функции из feature_library.
    
    Args:
        events_df: DataFrame с событиями из enhanced_ml_events
        token_address: Адрес токена
        date: Дата (строка в формате YYYY-MM-DD), если None - используется вчерашняя дата
        
    Returns:
        Словарь с рассчитанными признаками
    """
    # Если дата не указана, используем вчерашнюю дату
    if date is None:
        date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
    
    logger.info(f"Расчет признаков для токена {token_address} за {date}")
    
    try:
        # Преобразуем строку даты в datetime.date для функций библиотеки
        date_obj = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        
        # Рассчитываем базовые признаки с использованием функций из библиотеки
        volume_24h = calculate_daily_volume(events_df, token_address, date_obj)
        transaction_count_24h = calculate_transaction_count(events_df, token_address, date_obj)
        unique_wallets_24h = calculate_unique_wallets(events_df, token_address, date_obj)
        price_change_pct_24h = calculate_price_change(events_df, token_address, date)
        buy_sell_ratio_24h = calculate_buy_sell_ratio(events_df, token_address, date_obj)
        
        # Получаем балансы кошельков для расчета коэффициентов распределения
        wallet_balances = get_wallet_balances_for_token(events_df, token_address, date)
        gini_coefficient = calculate_gini_coefficient(wallet_balances)
        concentration_top5_pct = calculate_concentration_top5(wallet_balances)
        
        # Рассчитываем соотношения объемов
        external_to_internal_volume_ratio = calculate_external_to_internal_volume_ratio(events_df, token_address, date)
        
        # Рассчитываем скорость изменения ликвидности
        liquidity_change_velocity = calculate_liquidity_change_velocity(events_df, token_address, date)
        
        # Рассчитываем волатильность (упрощенное решение)
        # В идеале нужно рассчитывать на основе временного ряда цен
        volatility_24h = abs(price_change_pct_24h) / 100
        
        # НОВЫЕ ПРИЗНАКИ: Анализируем паттерны торговли за SOL
        sol_trading_analysis = analyze_sol_trading_pattern(events_df, token_address)
        
        # Извлекаем данные о крупнейших операциях
        largest_buy = sol_trading_analysis.get('largest_buy', {})
        largest_sell = sol_trading_analysis.get('largest_sell', {})
        
        # Формируем словарь с признаками
        features = {
            'token_address': token_address,
            'date': date,
            'volume_24h': volume_24h,
            'transaction_count_24h': transaction_count_24h,
            'unique_wallets_24h': unique_wallets_24h,
            'price_change_pct_24h': price_change_pct_24h,
            'volatility_24h': volatility_24h,
            'buy_sell_ratio_24h': buy_sell_ratio_24h,
            'gini_coefficient': gini_coefficient,
            'concentration_top5_pct': concentration_top5_pct,
            'external_traders_ratio': external_to_internal_volume_ratio,
            'liquidity_change_velocity': liquidity_change_velocity,
            # Новые признаки SOL торговли
            'largest_sol_buy_amount': largest_buy.get('sol_amount', 0) if not largest_buy.get('error') else 0,
            'largest_sol_buy_wallet': largest_buy.get('fee_payer', '') if not largest_buy.get('error') else '',
            'largest_sol_sell_amount': largest_sell.get('sol_amount', 0) if not largest_sell.get('error') else 0,
            'largest_sol_sell_wallet': largest_sell.get('fee_payer', '') if not largest_sell.get('error') else '',
            'total_sol_buy_volume': sol_trading_analysis.get('total_sol_buy_volume', 0),
            'total_sol_sell_volume': sol_trading_analysis.get('total_sol_sell_volume', 0),
            'sol_buy_sell_ratio': sol_trading_analysis.get('buy_sell_ratio', 0)
        }
        
        logger.info(f"Признаки для токена {token_address} за {date} успешно рассчитаны")
        return features
    except Exception as e:
        logger.error(f"Ошибка при расчете признаков для токена {token_address} за {date}: {str(e)}")
        return None

def save_features_to_db(conn, features):
    """
    Сохраняет рассчитанные признаки в таблицу feature_store_daily.
    
    Args:
        conn: Соединение с базой данных SQLite.
        features: Словарь с признаками
        
    Returns:
        bool: True, если сохранение прошло успешно, иначе False
    """
    try:
        cursor = conn.cursor()
        
        # Проверяем, есть ли уже запись для этого токена и даты
        cursor.execute(
            "SELECT 1 FROM feature_store_daily WHERE token_address = ? AND date = ?",
            (features['token_address'], features['date'])
        )
        exists = cursor.fetchone()
        
        if exists:
            # Обновляем существующую запись
            cursor.execute("""
            UPDATE feature_store_daily SET
                volume_24h = ?,
                transaction_count_24h = ?,
                unique_wallets_24h = ?,
                price_change_pct_24h = ?,
                volatility_24h = ?,
                buy_sell_ratio_24h = ?,
                gini_coefficient = ?,
                concentration_top5_pct = ?,
                external_to_internal_volume_ratio = ?,
                liquidity_change_velocity = ?,
                largest_sol_buy_amount = ?,
                largest_sol_buy_wallet = ?,
                largest_sol_sell_amount = ?,
                largest_sol_sell_wallet = ?,
                total_sol_buy_volume = ?,
                total_sol_sell_volume = ?,
                sol_buy_sell_ratio = ?,
                created_at = CURRENT_TIMESTAMP
            WHERE token_address = ? AND date = ?
            """, (
                features['volume_24h'],
                features['transaction_count_24h'],
                features['unique_wallets_24h'],
                features['price_change_pct_24h'],
                features['volatility_24h'],
                features['buy_sell_ratio_24h'],
                features['gini_coefficient'],
                features['concentration_top5_pct'],
                features['external_traders_ratio'],
                features['liquidity_change_velocity'],
                features['largest_sol_buy_amount'],
                features['largest_sol_buy_wallet'],
                features['largest_sol_sell_amount'],
                features['largest_sol_sell_wallet'],
                features['total_sol_buy_volume'],
                features['total_sol_sell_volume'],
                features['sol_buy_sell_ratio'],
                features['token_address'],
                features['date']
            ))
            logger.info(f"Обновлена запись для токена {features['token_address']} за {features['date']}")
        else:
            # Добавляем новую запись
            cursor.execute("""
            INSERT INTO feature_store_daily (
                token_address, date, volume_24h, transaction_count_24h, unique_wallets_24h,
                price_change_pct_24h, volatility_24h, buy_sell_ratio_24h, gini_coefficient,
                concentration_top5_pct, external_to_internal_volume_ratio, liquidity_change_velocity,
                largest_sol_buy_amount, largest_sol_buy_wallet, largest_sol_sell_amount, 
                largest_sol_sell_wallet, total_sol_buy_volume, total_sol_sell_volume, sol_buy_sell_ratio
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                features['token_address'],
                features['date'],
                features['volume_24h'],
                features['transaction_count_24h'],
                features['unique_wallets_24h'],
                features['price_change_pct_24h'],
                features['volatility_24h'],
                features['buy_sell_ratio_24h'],
                features['gini_coefficient'],
                features['concentration_top5_pct'],
                features['external_traders_ratio'],
                features['liquidity_change_velocity'],
                features['largest_sol_buy_amount'],
                features['largest_sol_buy_wallet'],
                features['largest_sol_sell_amount'],
                features['largest_sol_sell_wallet'],
                features['total_sol_buy_volume'],
                features['total_sol_sell_volume'],
                features['sol_buy_sell_ratio']
            ))
            logger.info(f"Добавлена новая запись для токена {features['token_address']} за {features['date']}")
        
        # Фиксируем изменения
        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при сохранении признаков в БД: {str(e)}", exc_info=True)
        return False

def load_token_addresses(file_path='tokens.txt'):
    """
    Загружает список адресов токенов из текстового файла.
    
    Args:
        file_path: Путь к файлу с адресами токенов (один адрес на строку)
        
    Returns:
        Список адресов токенов
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            addresses = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            logger.info(f"Загружено {len(addresses)} токенов из файла {file_path}")
            return addresses
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла со списком токенов {file_path}: {str(e)}")
        return []

def main():
    """
    Главная функция для запуска процесса построения хранилища признаков.
    """
    parser = argparse.ArgumentParser(description="Построение Feature Store для SQLite на основе данных ml_ready_events.")
    parser.add_argument('--token-address', type=str, help='Адрес токена для обработки. Если не указан, обрабатываются все.')
    parser.add_argument('--days', type=int, default=1, help='Количество прошедших дней для обработки.')
    parser.add_argument('--all', action='store_true', help='Обработать все доступные данные, игнорируя параметр days.')
    parser.add_argument('--db-path', type=str, default=None, help='Путь к файлу базы данных SQLite.')
    args = parser.parse_args()
    
    conn = None
    try:
        conn = get_db_connection(args.db_path)
        create_feature_store_table(conn)
        
        # Определяем диапазон дат для обработки
        if args.all:
            start_date = None
            end_date = datetime.date.today()
            logger.info("Режим обработки: все доступные данные.")
        else:
            end_date = datetime.date.today()
            start_date = end_date - datetime.timedelta(days=args.days)
            logger.info(f"Режим обработки: данные за последние {args.days} дней (с {start_date} по {end_date}).")
        
        # Загружаем данные через data_provider
        events_df = load_events_data_for_calculation(conn, args.token_address, date_range=(start_date, end_date))
        
        if events_df.empty:
            logger.info("Нет данных для обработки. Завершение работы.")
            return
            
        # Определяем список токенов и даты для обработки
        if args.token_address:
            tokens_to_process = [args.token_address]
        else:
            tokens_to_process = events_df['token_of_interest'].unique().tolist()
            
        logger.info(f"Найдено {len(tokens_to_process)} токенов для обработки.")
        
        dates_to_process = pd.to_datetime(events_df['block_time'], unit='s').dt.date.unique()

        processed_count = 0
        for token in tokens_to_process:
            for date_obj in dates_to_process:
                date_str = date_obj.strftime('%Y-%m-%d')
                
                # Фильтруем события для конкретного токена и даты
                daily_events = events_df[
                    (events_df['token_of_interest'] == token) &
                    (pd.to_datetime(events_df['block_time'], unit='s').dt.date == date_obj)
                ]
                
                if daily_events.empty:
                    continue
                
                features = build_features_for_token(daily_events, token, date_str)
                if features:
                    save_features_to_db(conn, features)
                    processed_count += 1
        
        logger.info(f"Обработка завершена. Сохранено/обновлено {processed_count} записей в feature_store_daily.")

    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении скрипта: {str(e)}", exc_info=True)
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main() 