#!/usr/bin/env python3
"""
Скрипт для построения хранилища признаков (feature store) на основе данных из ML-витрин.
Используется для расчета и сохранения признаков, необходимых для ML-моделей.
"""
import os
import sys
import time
import logging
import argparse
import datetime
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import sqlite3
import pandas as pd
import numpy as np
from analysis.data_provider import get_all_events_for_token
from analysis.feature_library import (
    calculate_daily_volume,
    calculate_transaction_count,
    calculate_unique_wallets,
    calculate_buy_sell_ratio,
    calculate_gini_coefficient,
    calculate_concentration_top_N
)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/build_feature_store.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("feature_store")

# SQL для создания таблицы feature_store_daily
CREATE_FEATURE_STORE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS feature_store_daily (
    token_address TEXT NOT NULL,
    date DATE NOT NULL,
    volume_24h NUMERIC,
    transaction_count_24h INTEGER,
    unique_wallets_24h INTEGER,
    price_change_pct_24h NUMERIC,
    volatility_24h NUMERIC,
    buy_sell_ratio_24h NUMERIC,
    gini_coefficient NUMERIC,
    concentration_top5_pct NUMERIC,
    external_traders_ratio NUMERIC,
    PRIMARY KEY (token_address, date)
);

CREATE INDEX IF NOT EXISTS idx_feature_store_token ON feature_store_daily(token_address);
CREATE INDEX IF NOT EXISTS idx_feature_store_date ON feature_store_daily(date);
"""

def get_db_connection():
    """
    Создает и возвращает соединение с базой данных SQLite.
    
    Returns:
        Соединение с базой данных
    """
    try:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')
        conn = sqlite3.connect(db_path)
        logger.info(f"Успешное подключение к SQLite базе данных: {db_path}")
        return conn
    except Exception as e:
        logger.error(f"Ошибка подключения к базе данных: {str(e)}")
        raise

def create_feature_store_table(conn):
    """
    Создает таблицу feature_store_daily, если она не существует.
    
    Args:
        conn: Соединение с базой данных
    """
    try:
        logger.info("Создание таблицы feature_store_daily")
        with conn.cursor() as cur:
            cur.execute(CREATE_FEATURE_STORE_TABLE_SQL)
        conn.commit()
        logger.info("Таблица feature_store_daily успешно создана")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при создании таблицы feature_store_daily: {str(e)}")
        raise

def calculate_price_change(conn, token_address, date):
    """
    Рассчитывает изменение цены за день для указанного токена.
    
    Args:
        conn: Соединение с базой данных
        token_address: Адрес токена
        date: Дата (datetime.date)
        
    Returns:
        Процент изменения цены за день
    """
    try:
        # Получаем цены в начале и в конце дня
        query = """
        WITH prices AS (
            SELECT 
                block_time,
                CASE
                    WHEN token_a_mint = ? THEN to_amount / from_amount
                    WHEN token_b_mint = ? THEN from_amount / to_amount
                END as price
            FROM ml_ready_events
            WHERE (token_a_mint = ? OR token_b_mint = ?)
            AND event_type = 'SWAP'
            AND DATE(datetime(block_time, 'unixepoch')) = ?
            AND to_amount > 0 AND from_amount > 0
            ORDER BY block_time
        )
        SELECT 
            (SELECT price FROM prices ORDER BY block_time ASC LIMIT 1) as first_price,
            (SELECT price FROM prices ORDER BY block_time DESC LIMIT 1) as last_price
        """
        
        cur = conn.cursor()
        cur.execute(query, (token_address, token_address, token_address, token_address, date))
        result = cur.fetchone()
        
        if result and result[0] and result[1]:
            first_price, last_price = float(result[0]), float(result[1])
            if first_price > 0:
                return ((last_price - first_price) / first_price) * 100
        
        return 0.0
    except Exception as e:
        logger.error(f"Ошибка при расчете изменения цены для токена {token_address} за {date}: {str(e)}")
        return 0.0

def calculate_volatility(conn, token_address, date):
    """
    Рассчитывает волатильность за день для указанного токена.
    
    Args:
        conn: Соединение с базой данных
        token_address: Адрес токена
        date: Дата (datetime.date)
        
    Returns:
        Волатильность за день
    """
    try:
        # Получаем цены в начале и в конце дня
        query = """
        WITH prices AS (
            SELECT 
                block_time,
                CASE
                    WHEN token_a_mint = ? THEN to_amount / from_amount
                    WHEN token_b_mint = ? THEN from_amount / to_amount
                END as price
            FROM ml_ready_events
            WHERE (token_a_mint = ? OR token_b_mint = ?)
            AND event_type = 'SWAP'
            AND DATE(datetime(block_time, 'unixepoch')) = ?
            AND to_amount > 0 AND from_amount > 0
            ORDER BY block_time
        )
        SELECT 
            (SELECT price FROM prices ORDER BY block_time ASC LIMIT 1) as first_price,
            (SELECT price FROM prices ORDER BY block_time DESC LIMIT 1) as last_price
        """
        
        cur = conn.cursor()
        cur.execute(query, (token_address, token_address, token_address, token_address, date))
        result = cur.fetchone()
        
        if result and result[0] and result[1]:
            first_price, last_price = float(result[0]), float(result[1])
            if first_price > 0:
                return ((last_price - first_price) / first_price) * 100
        
        return 0.0
    except Exception as e:
        logger.error(f"Ошибка при расчете волатильности для токена {token_address} за {date}: {str(e)}")
        return 0.0

def calculate_external_traders_ratio(conn, token_address, date):
    """
    Рассчитывает долю внешних трейдеров за день для указанного токена.
    
    Args:
        conn: Соединение с базой данных
        token_address: Адрес токена
        date: Дата (datetime.date)
        
    Returns:
        Доля внешних трейдеров за день
    """
    try:
        query = """
        SELECT 
            COALESCE(SUM(CASE WHEN token_a_mint = %s THEN 1 ELSE 0 END), 0) as external_buys,
            COALESCE(SUM(CASE WHEN token_b_mint = %s THEN 1 ELSE 0 END), 0) as external_sells
        FROM ml_ready_events
        WHERE (token_a_mint = %s OR token_b_mint = %s)
        AND event_type = 'swap'
        AND DATE(block_time) = %s
        """
        
        with conn.cursor() as cur:
            cur.execute(query, (token_address, token_address, token_address, token_address, date))
            result = cur.fetchone()
            
            if result:
                external_buys, external_sells = result
                if external_buys + external_sells > 0:
                    return float(external_buys) / float(external_buys + external_sells)
            
            return 0.0
    except Exception as e:
        logger.error(f"Ошибка при расчете доли внешних трейдеров для токена {token_address} за {date}: {str(e)}")
        return 0.0

def build_features_for_token(conn, token_address, date=None):
    """
    Строит признаки для указанного токена за указанную дату.
    
    Args:
        conn: Соединение с базой данных
        token_address: Адрес токена
        date: Дата (datetime.date, по умолчанию - вчера)
        
    Returns:
        Словарь признаков
    """
    if date is None:
        date = datetime.date.today() - datetime.timedelta(days=1)
    
    logger.info(f"Расчет признаков для токена {token_address} за {date}")
    
    try:
        # Проверяем наличие транзакций для этого токена за указанную дату
        check_query = """
        SELECT COUNT(*) FROM ml_ready_events
        WHERE (token_a_mint = %s OR token_b_mint = %s)
        AND DATE(block_time) = %s
        """
        
        with conn.cursor() as cur:
            cur.execute(check_query, (token_address, token_address, date))
            count = cur.fetchone()[0]
            
        if count == 0:
            logger.warning(f"Нет данных для токена {token_address} за {date}")
            return None
            
        # Рассчитываем признаки
        features = {
            'token_address': token_address,
            'date': date,
            'volume_24h': calculate_daily_volume(conn, token_address, date),
            'transaction_count_24h': calculate_transaction_count(conn, token_address, date),
            'unique_wallets_24h': calculate_unique_wallets(conn, token_address, date),
            'price_change_pct_24h': calculate_price_change(conn, token_address, date),
            'volatility_24h': calculate_volatility(conn, token_address, date),
            'buy_sell_ratio_24h': calculate_buy_sell_ratio(conn, token_address, date),
            'gini_coefficient': calculate_gini_coefficient(conn, token_address, date),
            'concentration_top5_pct': calculate_concentration_top_N(conn, token_address, date, n=5),
            'external_traders_ratio': calculate_external_traders_ratio(conn, token_address, date)
        }
        
        logger.info(f"Признаки для токена {token_address} за {date} успешно рассчитаны")
        return features
    except Exception as e:
        logger.error(f"Ошибка при расчете признаков для токена {token_address} за {date}: {str(e)}")
        return None

def save_features_to_db(features: dict):
    """
    Сохраняет рассчитанные признаки в таблицу feature_store_daily.
    Обновляет updated_at при конфликте по (token_address, date).
    :param features: dict с признаками
    """
    query = '''
        INSERT INTO feature_store_daily (
            token_address, date, daily_volume, transaction_count, unique_wallets,
            buy_sell_ratio, gini_coefficient, concentration_top5
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (token_address, date) DO UPDATE SET
            daily_volume = EXCLUDED.daily_volume,
            transaction_count = EXCLUDED.transaction_count,
            unique_wallets = EXCLUDED.unique_wallets,
            buy_sell_ratio = EXCLUDED.buy_sell_ratio,
            gini_coefficient = EXCLUDED.gini_coefficient,
            concentration_top5 = EXCLUDED.concentration_top5,
            updated_at = NOW()
    '''
    values = (
        features['token_address'],
        features['date'],
        features['daily_volume'],
        features['transaction_count'],
        features['unique_wallets'],
        features['buy_sell_ratio'],
        features['gini_coefficient'],
        features['concentration_top5']
    )
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, values)
        conn.commit()

def main():
    """
    Оркестратор для расчёта признаков по токену и дате.
    Принимает token_address и date, загружает события, считает признаки, выводит/сохраняет результат.
    """
    parser = argparse.ArgumentParser(description="Build daily feature store for a token.")
    parser.add_argument('--token_address', type=str, required=True, help='Token address')
    parser.add_argument('--date', type=str, required=True, help='Date in YYYY-MM-DD format')
    args = parser.parse_args()

    token_address = args.token_address
    date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()

    print(f"Загрузка событий для токена {token_address}...")
    events_df = get_all_events_for_token(token_address)
    print(f"Всего событий: {len(events_df)}")

    print(f"Расчёт признаков за {date}...")
    daily_volume = calculate_daily_volume(events_df, token_address, date)
    tx_count = calculate_transaction_count(events_df, token_address, date)
    unique_wallets = calculate_unique_wallets(events_df, token_address, date)
    buy_sell_ratio = calculate_buy_sell_ratio(events_df, token_address, date)
    # Для Gini и concentration нужен список балансов (пример: итоговые балансы на конец дня)
    # Здесь заглушка, в реальном пайплайне нужно получать актуальные балансы
    wallet_balances = []  # TODO: получить реальные балансы на конец дня
    gini = calculate_gini_coefficient(wallet_balances)
    concentration = calculate_concentration_top_N(wallet_balances, n=5)

    features = {
        'token_address': token_address,
        'date': date,
        'daily_volume': daily_volume,
        'transaction_count': tx_count,
        'unique_wallets': unique_wallets,
        'buy_sell_ratio': buy_sell_ratio,
        'gini_coefficient': gini,
        'concentration_top5': concentration
    }

    print("Рассчитанные признаки:")
    for k, v in features.items():
        print(f"{k}: {v}")

    save_features_to_db(features)

if __name__ == "__main__":
    main()