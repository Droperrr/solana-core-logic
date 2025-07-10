"""
Модуль для доступа к данным из базы данных SQLite.
Инкапсулирует всю логику SQL-запросов для аналитических модулей.
"""
import os
import sqlite3
import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger("data_provider")

def get_sqlite_db_path():
    """
    Возвращает путь к файлу базы данных SQLite.
    """
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')

def dict_factory(cursor, row):
    """Конвертирует строки результатов запроса в словари"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_db_connection():
    """
    Создает и возвращает соединение с базой данных SQLite.
    
    Returns:
        Connection: Соединение с базой данных SQLite.
    """
    db_path = get_sqlite_db_path()
    logger.info(f"Подключение к SQLite: {db_path}")
    
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"База данных не найдена: {db_path}")
    
    conn = sqlite3.connect(db_path)
    # НЕ ИСПОЛЬЗУЕМ dict_factory для pandas - это может вызывать проблемы
    # conn.row_factory = dict_factory
    
    # Включаем режим WAL (Write-Ahead Logging) для безопасного одновременного доступа
    conn.execute("PRAGMA journal_mode=WAL;")
    
    return conn

def get_all_events(date_range: tuple = None) -> pd.DataFrame:
    """
    Получает все события из таблицы ml_ready_events, опционально фильтруя по дате.
    
    Args:
        date_range: Кортеж (start_date, end_date) в формате 'YYYY-MM-DD'
        
    Returns:
        pandas.DataFrame: DataFrame со всеми событиями
    """
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            signature, block_time, event_type, token_a_mint, token_b_mint,
            from_wallet, to_wallet, from_amount, to_amount, wallet_tag,
            program_id, instruction_name, event_data_raw
        FROM ml_ready_events
        """
        params = []
        
        if date_range and len(date_range) == 2:
            start_date, end_date = date_range
            # Преобразуем YYYY-MM-DD в unix timestamp
            start_timestamp = int(pd.to_datetime(start_date).timestamp())
            end_timestamp = int(pd.to_datetime(end_date).timestamp()) + 86399 # до конца дня
            
            query += " WHERE block_time BETWEEN ? AND ?"
            params.extend([start_timestamp, end_timestamp])
            
        query += " ORDER BY block_time"
        
        df = pd.read_sql_query(query, conn, params=params)
        
        if len(df) > 0 and 'block_time' in df.columns:
            df['block_time'] = pd.to_datetime(df['block_time'], unit='s')
        
        logger.info(f"Загружено {len(df)} событий.")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при получении всех событий: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals() and 'close' in dir(conn):
            conn.close()

def get_all_events_for_token(token_address: str) -> pd.DataFrame:
    """
    Получает все события для указанного токена из таблицы ml_ready_events.
    
    Args:
        token_address: Адрес токена
        
    Returns:
        pandas.DataFrame: DataFrame с событиями для токена
    """
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            signature,
            block_time,
            event_type,
            token_a_mint,
            token_b_mint,
            from_wallet,
            to_wallet,
            from_amount,
            to_amount,
            wallet_tag,
            program_id,
            instruction_name,
            event_data_raw
        FROM ml_ready_events
        WHERE token_a_mint = ? OR token_b_mint = ?
        ORDER BY block_time
        """
        
        df = pd.read_sql_query(query, conn, params=(token_address, token_address))
        
        logger.info(f"Исходный DataFrame: {len(df)} строк")
        
        # Преобразуем block_time в datetime для удобства анализа
        if len(df) > 0 and 'block_time' in df.columns:
            logger.info(f"Block time столбец присутствует, тип данных: {df['block_time'].dtype}")
            
            # Сначала убираем только явно пустые значения
            initial_len = len(df)
            df = df.dropna(subset=['block_time'])
            logger.info(f"После удаления null block_time: {len(df)} строк (было {initial_len})")
            
            if len(df) > 0:
                try:
                    # Создаём копию для избежания ошибок модификации
                    df = df.copy()
                    
                    # Проверяем образец данных
                    logger.info(f"Образец block_time: {df['block_time'].iloc[0]}")
                    
                    # Принудительно конвертируем в numeric, затем в datetime
                    before_convert = len(df)
                    df['block_time'] = pd.to_numeric(df['block_time'], errors='coerce')
                    df = df.dropna(subset=['block_time'])  # Удаляем строки, где conversion не удался
                    logger.info(f"После numeric conversion: {len(df)} строк (было {before_convert})")
                    
                    if len(df) > 0:
                        df['block_time'] = pd.to_datetime(df['block_time'], unit='s')
                        logger.info(f"Успешно преобразованы datetime: {df['block_time'].iloc[0]}")
                    else:
                        logger.warning("Все данные потеряны при numeric conversion")
                        
                except Exception as e:
                    logger.error(f"Ошибка преобразования block_time: {e}")
                    return pd.DataFrame()
        
        logger.info(f"Загружено {len(df)} событий для токена {token_address}")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при получении событий для токена {token_address}: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

def get_feature_store_for_token(token_address: str) -> pd.DataFrame:
    """
    Получает данные из хранилища признаков для указанного токена.
    
    Args:
        token_address: Адрес токена
        
    Returns:
        pandas.DataFrame: DataFrame с признаками для токена
    """
    try:
        conn = get_db_connection()
        
        query = """
        SELECT 
            token_address,
            date,
            volume_24h,
            transaction_count_24h,
            unique_wallets_24h,
            price_change_pct_24h,
            volatility_24h,
            buy_sell_ratio_24h,
            gini_coefficient,
            concentration_top5_pct,
            external_to_internal_volume_ratio,
            liquidity_change_velocity,
            created_at
        FROM feature_store_daily
        WHERE token_address = ?
        ORDER BY date
        """
        
        df = pd.read_sql_query(query, conn, params=(token_address,))
        
        # Преобразуем date в datetime для удобства анализа
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
        
        logger.info(f"Загружено {len(df)} записей из feature store для токена {token_address}")
        return df
        
    except Exception as e:
        logger.error(f"Ошибка при получении данных feature store для токена {token_address}: {str(e)}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals():
            conn.close()

def get_wallet_info(wallet_address: str) -> dict:
    """
    Получает информацию о кошельке из базы данных.
    
    Args:
        wallet_address: Адрес кошелька
        
    Returns:
        dict: Словарь с информацией о кошельке
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Получаем общую статистику по кошельку
        stats_query = """
        WITH wallet_stats AS (
            SELECT 
                COUNT(DISTINCT signature) as total_transactions,
                COUNT(DISTINCT token_a_mint) + COUNT(DISTINCT token_b_mint) as unique_tokens,
                MIN(block_time) as first_activity,
                MAX(block_time) as last_activity,
                SUM(CASE WHEN event_type = 'SWAP' THEN 1 ELSE 0 END) as swap_count,
                wallet_tag
            FROM ml_ready_events
            WHERE from_wallet = ? OR to_wallet = ?
            GROUP BY wallet_tag
        )
        SELECT * FROM wallet_stats
        """
        
        cursor.execute(stats_query, (wallet_address, wallet_address))
        stats = cursor.fetchone()
        
        if not stats:
            return {
                'wallet_address': wallet_address,
                'exists': False,
                'total_transactions': 0,
                'unique_tokens': 0,
                'first_activity': None,
                'last_activity': None,
                'swap_count': 0,
                'wallet_tag': None
            }
        
        # Получаем список токенов, с которыми взаимодействовал кошелек
        tokens_query = """
        SELECT DISTINCT 
            CASE 
                WHEN from_wallet = ? THEN token_a_mint 
                WHEN to_wallet = ? THEN token_b_mint 
            END as token_address
        FROM ml_ready_events
        WHERE (from_wallet = ? OR to_wallet = ?) 
        AND (token_a_mint IS NOT NULL OR token_b_mint IS NOT NULL)
        """
        
        cursor.execute(tokens_query, (wallet_address, wallet_address, wallet_address, wallet_address))
        tokens = [row['token_address'] for row in cursor.fetchall() if row['token_address']]
        
        # Формируем результат
        wallet_info = {
            'wallet_address': wallet_address,
            'exists': True,
            'total_transactions': stats['total_transactions'],
            'unique_tokens': len(set(tokens)),  # Убираем дубликаты
            'first_activity': pd.to_datetime(stats['first_activity'], unit='s') if stats['first_activity'] else None,
            'last_activity': pd.to_datetime(stats['last_activity'], unit='s') if stats['last_activity'] else None,
            'swap_count': stats['swap_count'],
            'wallet_tag': stats['wallet_tag'],
            'interacted_tokens': tokens[:10]  # Ограничиваем до 10 токенов для краткости
        }
        
        logger.info(f"Получена информация о кошельке {wallet_address}")
        return wallet_info
        
    except Exception as e:
        logger.error(f"Ошибка при получении информации о кошельке {wallet_address}: {str(e)}")
        return {
            'wallet_address': wallet_address,
            'exists': False,
            'error': str(e)
        }
    finally:
        if 'conn' in locals():
            conn.close() 