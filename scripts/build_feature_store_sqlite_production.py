#!/usr/bin/env python3
"""
Производственная версия build_feature_store_sqlite.py с полной интеграцией SOL Trading Analysis
Построение витрины признаков для ML-анализа с поддержкой всех аналитических компонентов
"""

import sqlite3
import json
import sys
import os
import logging
from pathlib import Path
from datetime import datetime, date, timedelta
from typing import Dict, Any, List, Optional

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def extract_sol_trading_features(enriched_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает SOL trading признаки из enriched_data
    
    Args:
        enriched_data: Обогащенные данные транзакции
    
    Returns:
        Словарь с SOL trading признаками
    """
    sol_features = {
        'largest_sol_buy_amount': 0.0,
        'largest_sol_sell_amount': 0.0,
        'total_sol_buy_amount': 0.0,
        'total_sol_sell_amount': 0.0,
        'sol_buy_count': 0,
        'sol_sell_count': 0,
        'net_sol_change': 0.0
    }
    
    try:
        # Проверяем наличие SOL trades в enriched_data
        if isinstance(enriched_data, str):
            enriched_data = json.loads(enriched_data)
        
        # Ищем SOL данные в различных структурах enriched_data
        if isinstance(enriched_data, list):
            # Если enriched_data это список событий
            for event in enriched_data:
                if isinstance(event, dict):
                    sol_trades = event.get('sol_trades', {})
                    if sol_trades:
                        sol_features.update({
                            'largest_sol_buy_amount': max(sol_features['largest_sol_buy_amount'], 
                                                        float(sol_trades.get('largest_sol_buy_amount', 0))),
                            'largest_sol_sell_amount': max(sol_features['largest_sol_sell_amount'], 
                                                         float(sol_trades.get('largest_sol_sell_amount', 0))),
                            'total_sol_buy_amount': sol_features['total_sol_buy_amount'] + 
                                                  float(sol_trades.get('total_sol_buy_amount', 0)),
                            'total_sol_sell_amount': sol_features['total_sol_sell_amount'] + 
                                                   float(sol_trades.get('total_sol_sell_amount', 0)),
                            'sol_buy_count': sol_features['sol_buy_count'] + 
                                           int(sol_trades.get('sol_buy_count', 0)),
                            'sol_sell_count': sol_features['sol_sell_count'] + 
                                            int(sol_trades.get('sol_sell_count', 0)),
                            'net_sol_change': sol_features['net_sol_change'] + 
                                            float(sol_trades.get('net_sol_change', 0))
                        })
        elif isinstance(enriched_data, dict):
            # Если enriched_data это словарь
            sol_trades = enriched_data.get('sol_trades', {})
            if sol_trades:
                sol_features.update({
                    'largest_sol_buy_amount': float(sol_trades.get('largest_sol_buy_amount', 0)),
                    'largest_sol_sell_amount': float(sol_trades.get('largest_sol_sell_amount', 0)),
                    'total_sol_buy_amount': float(sol_trades.get('total_sol_buy_amount', 0)),
                    'total_sol_sell_amount': float(sol_trades.get('total_sol_sell_amount', 0)),
                    'sol_buy_count': int(sol_trades.get('sol_buy_count', 0)),
                    'sol_sell_count': int(sol_trades.get('sol_sell_count', 0)),
                    'net_sol_change': float(sol_trades.get('net_sol_change', 0))
                })
        
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.debug(f"Ошибка извлечения SOL features: {e}")
    
    return sol_features

def extract_basic_features(enriched_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Извлекает базовые признаки из enriched_data
    
    Args:
        enriched_data: Обогащенные данные транзакции
    
    Returns:
        Словарь с базовыми признаками
    """
    basic_features = {
        'total_transactions': 1,
        'swap_count': 0,
        'transfer_count': 0,
        'total_volume': 0.0,
        'buy_volume': 0.0,
        'sell_volume': 0.0
    }
    
    try:
        if isinstance(enriched_data, str):
            enriched_data = json.loads(enriched_data)
        
        if isinstance(enriched_data, list):
            # Подсчитываем события
            for event in enriched_data:
                if isinstance(event, dict):
                    event_type = event.get('type', '').upper()
                    
                    if 'SWAP' in event_type:
                        basic_features['swap_count'] += 1
                        
                        # Извлекаем объемы из деталей события
                        details = event.get('details', {})
                        if isinstance(details, dict):
                            amount_in = float(details.get('amount_in', 0))
                            amount_out = float(details.get('amount_out', 0))
                            basic_features['total_volume'] += max(amount_in, amount_out)
                    
                    elif 'TRANSFER' in event_type:
                        basic_features['transfer_count'] += 1
        
    except (json.JSONDecodeError, ValueError, TypeError) as e:
        logger.debug(f"Ошибка извлечения базовых features: {e}")
    
    return basic_features

def create_production_feature_store_table(conn):
    """Создает производственную таблицу feature_store_daily с полным набором признаков"""
    
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS feature_store_daily (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date DATE NOT NULL,
            wallet_address TEXT NOT NULL,
            token_address TEXT,
            
            -- Базовые метрики
            total_transactions INTEGER DEFAULT 0,
            swap_count INTEGER DEFAULT 0,
            transfer_count INTEGER DEFAULT 0,
            total_volume REAL DEFAULT 0,
            
            -- SWAP метрики  
            buy_volume REAL DEFAULT 0,
            sell_volume REAL DEFAULT 0,
            net_volume REAL DEFAULT 0,
            
            -- SOL Trading Analysis (ИНТЕГРИРОВАННЫЕ ПОЛЯ)
            largest_sol_buy_amount REAL DEFAULT 0,
            largest_sol_sell_amount REAL DEFAULT 0,
            total_sol_buy_amount REAL DEFAULT 0,
            total_sol_sell_amount REAL DEFAULT 0,
            sol_buy_count INTEGER DEFAULT 0,
            sol_sell_count INTEGER DEFAULT 0,
            net_sol_change REAL DEFAULT 0,
            
            -- Метаданные
            parser_version TEXT DEFAULT '2.0.0',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            
            UNIQUE(date, wallet_address, token_address)
        )
    """)
    
    # Создаем оптимизированные индексы
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_date ON feature_store_daily(date)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_wallet ON feature_store_daily(wallet_address)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_token ON feature_store_daily(token_address)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_feature_store_sol_activity ON feature_store_daily(sol_buy_count, sol_sell_count)")
    
    conn.commit()
    logger.info("✅ Производственная таблица feature_store_daily создана с полным набором признаков")

def extract_wallet_from_enriched_data(enriched_data: Dict[str, Any]) -> Optional[str]:
    """Извлекает адрес кошелька из enriched_data"""
    try:
        if isinstance(enriched_data, str):
            enriched_data = json.loads(enriched_data)
        
        # Ищем в различных местах enriched_data
        if isinstance(enriched_data, list):
            for event in enriched_data:
                if isinstance(event, dict):
                    # Проверяем прямое поле wallet_address
                    if event.get('wallet_address'):
                        return event['wallet_address']
                    
                    # Проверяем в деталях события
                    details = event.get('details', {})
                    if isinstance(details, dict):
                        if details.get('wallet_address'):
                            return details['wallet_address']
                        if details.get('user_wallet'):
                            return details['user_wallet']
        
        return None
    except:
        return None

def build_features_for_token(conn, token_address: str, days_back: int = 7) -> int:
    """
    Строит признаки для конкретного токена за указанный период
    ОБНОВЛЕННАЯ ВЕРСИЯ с полной интеграцией SOL Trading Analysis
    
    Args:
        conn: Соединение с БД
        token_address: Адрес токена
        days_back: Количество дней назад для анализа
    
    Returns:
        Количество обработанных записей
    """
    cursor = conn.cursor()
    
    # Определяем временной диапазон
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days_back)
    
    logger.info(f"Строим признаки для токена {token_address[:8]}... за период {start_date} - {end_date}")
    
    # Получаем все транзакции для токена за период
    cursor.execute("""
        SELECT signature, block_time, enriched_data, source_query_address, parser_version
        FROM transactions 
        WHERE source_query_address = ?
        AND enriched_data IS NOT NULL 
        AND enriched_data != ''
        AND block_time >= ?
        AND block_time <= ?
        ORDER BY block_time
    """, (token_address, 
          int(start_date.strftime('%s')), 
          int(end_date.strftime('%s'))))
    
    transactions = cursor.fetchall()
    
    if not transactions:
        logger.info(f"Нет транзакций для токена {token_address[:8]}... за указанный период")
        return 0
    
    logger.info(f"Найдено {len(transactions)} транзакций для обработки")
    
    processed_count = 0
    sol_enriched_count = 0
    
    for signature, block_time, enriched_data_str, source_query_address, parser_version in transactions:
        try:
            if not block_time:
                continue
            
            # Преобразуем block_time в дату
            transaction_date = datetime.fromtimestamp(block_time).date()
            
            # Парсим enriched_data
            try:
                enriched_data = json.loads(enriched_data_str)
            except json.JSONDecodeError:
                logger.warning(f"Некорректный JSON в enriched_data для {signature}")
                continue
            
            # Извлекаем все признаки
            basic_features = extract_basic_features(enriched_data)
            sol_features = extract_sol_trading_features(enriched_data)
            
            # Проверяем, есть ли SOL данные
            has_sol_data = any(v != 0 for v in sol_features.values() if isinstance(v, (int, float)))
            if has_sol_data:
                sol_enriched_count += 1
            
            # Определяем кошелек
            wallet_address = extract_wallet_from_enriched_data(enriched_data) or source_query_address
            
            if not wallet_address:
                logger.debug(f"Не удалось определить кошелек для транзакции {signature}")
                continue
            
            # UPSERT в feature_store_daily с полным набором признаков
            cursor.execute("""
                INSERT INTO feature_store_daily (
                    date, wallet_address, token_address,
                    total_transactions, swap_count, transfer_count, total_volume,
                    buy_volume, sell_volume, net_volume,
                    largest_sol_buy_amount, largest_sol_sell_amount,
                    total_sol_buy_amount, total_sol_sell_amount,
                    sol_buy_count, sol_sell_count, net_sol_change,
                    parser_version, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(date, wallet_address, token_address) DO UPDATE SET
                    total_transactions = total_transactions + excluded.total_transactions,
                    swap_count = swap_count + excluded.swap_count,
                    transfer_count = transfer_count + excluded.transfer_count,
                    total_volume = total_volume + excluded.total_volume,
                    buy_volume = buy_volume + excluded.buy_volume,
                    sell_volume = sell_volume + excluded.sell_volume,
                    net_volume = net_volume + excluded.net_volume,
                    largest_sol_buy_amount = MAX(largest_sol_buy_amount, excluded.largest_sol_buy_amount),
                    largest_sol_sell_amount = MAX(largest_sol_sell_amount, excluded.largest_sol_sell_amount),
                    total_sol_buy_amount = total_sol_buy_amount + excluded.total_sol_buy_amount,
                    total_sol_sell_amount = total_sol_sell_amount + excluded.total_sol_sell_amount,
                    sol_buy_count = sol_buy_count + excluded.sol_buy_count,
                    sol_sell_count = sol_sell_count + excluded.sol_sell_count,
                    net_sol_change = net_sol_change + excluded.net_sol_change,
                    parser_version = excluded.parser_version,
                    updated_at = CURRENT_TIMESTAMP
            """, (
                transaction_date,
                wallet_address,
                token_address,
                basic_features['total_transactions'],
                basic_features['swap_count'],
                basic_features['transfer_count'],
                basic_features['total_volume'],
                basic_features['buy_volume'],
                basic_features['sell_volume'],
                basic_features['buy_volume'] - basic_features['sell_volume'],  # net_volume
                sol_features['largest_sol_buy_amount'],
                sol_features['largest_sol_sell_amount'],
                sol_features['total_sol_buy_amount'],
                sol_features['total_sol_sell_amount'],
                sol_features['sol_buy_count'],
                sol_features['sol_sell_count'],
                sol_features['net_sol_change'],
                parser_version or '2.0.0'
            ))
            
            processed_count += 1
            
            if processed_count % 100 == 0:
                conn.commit()
                logger.info(f"Обработано {processed_count}/{len(transactions)} транзакций")
            
        except Exception as e:
            logger.error(f"Ошибка обработки транзакции {signature}: {e}")
            continue
    
    conn.commit()
    
    logger.info(f"""
📊 ОБРАБОТКА ТОКЕНА {token_address[:8]}... ЗАВЕРШЕНА:
✅ Обработано транзакций: {processed_count}
💰 Транзакций с SOL данными: {sol_enriched_count}
📈 Процент SOL обогащения: {sol_enriched_count/processed_count*100:.1f}% (если > 0)
    """)
    
    return processed_count

def build_production_feature_store(days_back: int = 7):
    """
    Строит полную производственную витрину признаков
    
    Args:
        days_back: Количество дней назад для анализа
    """
    
    logger.info("🚀 ПОСТРОЕНИЕ ПРОИЗВОДСТВЕННОЙ FEATURE STORE")
    logger.info("=" * 60)
    
    conn = sqlite3.connect('db/solana_db.sqlite')
    
    try:
        # Создаем таблицу с полным набором признаков
        create_production_feature_store_table(conn)
        
        # Получаем список всех токенов в БД
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT source_query_address
            FROM transactions 
            WHERE source_query_address IS NOT NULL
            AND enriched_data IS NOT NULL
            ORDER BY source_query_address
        """)
        
        tokens = [row[0] for row in cursor.fetchall()]
        logger.info(f"Найдено {len(tokens)} уникальных токенов для обработки")
        
        total_processed = 0
        
        for i, token_address in enumerate(tokens, 1):
            logger.info(f"\n[{i}/{len(tokens)}] Обрабатываем токен: {token_address}")
            
            try:
                processed = build_features_for_token(conn, token_address, days_back)
                total_processed += processed
                
            except Exception as e:
                logger.error(f"Ошибка обработки токена {token_address}: {e}")
                continue
        
        # Финальная статистика
        cursor.execute("SELECT COUNT(*) FROM feature_store_daily")
        total_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM feature_store_daily WHERE sol_buy_count > 0 OR sol_sell_count > 0")
        sol_records = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT token_address) FROM feature_store_daily")
        unique_tokens = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(DISTINCT wallet_address) FROM feature_store_daily")
        unique_wallets = cursor.fetchone()[0]
        
        logger.info(f"""

🎉 ПОСТРОЕНИЕ ПРОИЗВОДСТВЕННОЙ FEATURE STORE ЗАВЕРШЕНО:

📊 ОСНОВНАЯ СТАТИСТИКА:
▫️ Обработано транзакций: {total_processed}
▫️ Записей в feature_store_daily: {total_records}
▫️ Уникальных токенов: {unique_tokens}
▫️ Уникальных кошельков: {unique_wallets}

💰 SOL TRADING ANALYSIS:
▫️ Записей с SOL активностью: {sol_records}
▫️ Процент SOL покрытия: {sol_records/total_records*100:.1f}%

🎯 СИСТЕМА ГОТОВА К АНАЛИЗУ!
        """)
        
    finally:
        conn.close()

def main():
    """Главная функция"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Построение производственной витрины признаков")
    parser.add_argument('--days', type=int, default=7, 
                       help='Количество дней назад для анализа (по умолчанию: 7)')
    parser.add_argument('--token', type=str, default=None,
                       help='Обработать только указанный токен')
    
    args = parser.parse_args()
    
    if args.token:
        logger.info(f"Обработка одного токена: {args.token}")
        conn = sqlite3.connect('db/solana_db.sqlite')
        try:
            create_production_feature_store_table(conn)
            build_features_for_token(conn, args.token, args.days)
        finally:
            conn.close()
    else:
        build_production_feature_store(args.days)

if __name__ == "__main__":
    main() 