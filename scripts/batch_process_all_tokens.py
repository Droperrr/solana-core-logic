#!/usr/bin/env python3
"""
Скрипт для полномасштабной загрузки и обработки транзакций для всех токенов из файла tokens.txt.
Использует SQLite в качестве хранилища данных.
"""
import os
import sys
import time
import logging
import argparse
import concurrent.futures
import json
import datetime
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Импортируем необходимые модули
import sqlite3
from rpc.client import RPCClient
import config.config as app_config
from scripts.processing_utils import (
    get_sqlite_connection,
    create_tables,
    load_token_addresses,
    process_token_batch,
)
# from decoder import process_transaction # Больше не используется напрямую

# Создаем директорию для логов, если её нет
Path("logs").mkdir(exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/batch_process_all.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("batch_all_processor")

def update_wallet_statistics(conn, wallet_addresses, timestamp):
    """
    Обновляет статистику кошельков в таблице wallets.
    
    Args:
        conn: Соединение с базой данных
        wallet_addresses: Список адресов кошельков
        timestamp: Временная метка транзакции
    """
    cursor = conn.cursor()
    
    for address in wallet_addresses:
        if not address:  # Пропускаем пустые адреса
            continue
            
        cursor.execute("""
        INSERT INTO wallets (address, first_seen_timestamp, last_seen_timestamp, token_interaction_count)
        VALUES (?, ?, ?, 1)
        ON CONFLICT(address) DO UPDATE SET
            last_seen_timestamp = ?,
            token_interaction_count = token_interaction_count + 1
        """, (address, timestamp, timestamp, timestamp))
    
    conn.commit()

def analyze_wallet_roles(conn, token_address):
    """
    Анализирует роли кошельков для конкретного токена и обновляет их в БД.
    Использует новую логику семантического анализа SOL торговли из enrichment_data.
    
    Args:
        conn: Соединение с базой данных  
        token_address: Адрес токена для анализа
    """
    cursor = conn.cursor()
    logger.info(f"Анализ ролей кошельков для токена {token_address} (с семантическим анализом SOL)")
    
    try:
        # Находим создателя (Creator) - fee_payer крупнейшей семантической покупки за SOL
        cursor.execute("""
        SELECT signature, enrichment_data
        FROM ml_ready_events 
        WHERE (token_a_mint = ? OR token_b_mint = ?)
        AND enrichment_data IS NOT NULL
        AND enrichment_data != ''
        """, (token_address, token_address))
        
        events = cursor.fetchall()
        creator_wallet = None
        max_sol_buy = 0
        creator_signature = None
        
        dumper_wallet = None
        max_sol_sell = 0
        dumper_signature = None
        
        for signature, enrichment_data in events:
            try:
                # Парсим enrichment_data
                if isinstance(enrichment_data, str):
                    enrichment_data = json.loads(enrichment_data)
                
                sol_trades = enrichment_data.get('sol_trades', {})
                
                # Ищем покупки за SOL
                if (sol_trades.get('trade_type') == 'BUY_WITH_SOL' and 
                    sol_trades.get('primary_token') == token_address):
                    
                    sol_amount = abs(sol_trades.get('net_sol_change_ui', 0))
                    if sol_amount > max_sol_buy:
                        max_sol_buy = sol_amount
                        creator_wallet = sol_trades.get('fee_payer')
                        creator_signature = signature
                
                # Ищем продажи за SOL
                elif (sol_trades.get('trade_type') == 'SELL_FOR_SOL' and 
                      sol_trades.get('primary_token') == token_address):
                    
                    sol_amount = abs(sol_trades.get('net_sol_change_ui', 0))
                    if sol_amount > max_sol_sell:
                        max_sol_sell = sol_amount
                        dumper_wallet = sol_trades.get('fee_payer')
                        dumper_signature = signature
                        
            except Exception as e:
                logger.debug(f"Ошибка при обработке enrichment_data для {signature}: {str(e)}")
                continue
        
        # Обновляем роль создателя
        if creator_wallet and max_sol_buy > 0:
            logger.info(f"Найден создатель токена {token_address}: {creator_wallet} (покупка {max_sol_buy:.6f} SOL)")
            
            cursor.execute("""
            UPDATE wallets 
            SET role = 'creator', 
                notes = COALESCE(notes, '') || 'Creator: largest SOL->token swap (' || ? || ' SOL); '
            WHERE address = ?
            """, (max_sol_buy, creator_wallet))
        
        # Обновляем роль дампера
        if dumper_wallet and max_sol_sell > 0:
            logger.info(f"Найден дампер токена {token_address}: {dumper_wallet} (продажа за {max_sol_sell:.6f} SOL)")
            
            # Проверяем, не является ли дампер также создателем
            if dumper_wallet == creator_wallet:
                cursor.execute("""
                UPDATE wallets 
                SET notes = COALESCE(notes, '') || 'Also dumper: largest token->SOL swap (' || ? || ' SOL); '
                WHERE address = ?
                """, (max_sol_sell, dumper_wallet))
            else:
                cursor.execute("""
                UPDATE wallets 
                SET role = 'dumper',
                    notes = COALESCE(notes, '') || 'Dumper: largest token->SOL swap (' || ? || ' SOL); '
                WHERE address = ?
                """, (max_sol_sell, dumper_wallet))
        
        # Если не найдены семантические торги, используем fallback логику
        if not creator_wallet or not dumper_wallet:
            logger.info(f"Семантические SOL торги не найдены для {token_address}, используем fallback анализ")
            
            # Fallback: ищем по старой логике через token pairings
            if not creator_wallet:
                cursor.execute("""
                SELECT m.signature, m.from_amount
                FROM ml_ready_events m
                WHERE (m.token_a_mint = ? OR m.token_b_mint = ?)
                AND m.event_type = 'SWAP'
                AND m.token_a_mint = 'So11111111111111111111111111111111111111112'  -- SOL
                AND m.token_b_mint = ?
                ORDER BY m.from_amount DESC
                LIMIT 1
                """, (token_address, token_address, token_address))
                
                creator_result = cursor.fetchone()
                if creator_result:
                    signature, from_amount = creator_result
                    fee_payer, _ = extract_wallets_from_transaction(conn, signature)
                    if fee_payer:
                        creator_wallet = fee_payer
                        logger.info(f"Fallback: найден создатель {creator_wallet} (покупка {from_amount} SOL)")
                        
                        cursor.execute("""
                        UPDATE wallets 
                        SET role = 'creator', 
                            notes = COALESCE(notes, '') || 'Creator (fallback): largest SOL->token swap (' || ? || ' SOL); '
                        WHERE address = ?
                        """, (from_amount, creator_wallet))
            
            if not dumper_wallet:
                cursor.execute("""
                SELECT m.signature, m.to_amount
                FROM ml_ready_events m
                WHERE (m.token_a_mint = ? OR m.token_b_mint = ?)
                AND m.event_type = 'SWAP'
                AND m.token_a_mint = ?  -- токен
                AND m.token_b_mint = 'So11111111111111111111111111111111111111112'  -- SOL
                ORDER BY m.to_amount DESC
                LIMIT 1
                """, (token_address, token_address, token_address))
                
                dumper_result = cursor.fetchone()
                if dumper_result:
                    signature, to_amount = dumper_result
                    fee_payer, _ = extract_wallets_from_transaction(conn, signature)
                    if fee_payer:
                        dumper_wallet = fee_payer
                        logger.info(f"Fallback: найден дампер {dumper_wallet} (продажа за {to_amount} SOL)")
                        
                        if dumper_wallet == creator_wallet:
                            cursor.execute("""
                            UPDATE wallets 
                            SET notes = COALESCE(notes, '') || 'Also dumper (fallback): largest token->SOL swap (' || ? || ' SOL); '
                            WHERE address = ?
                            """, (to_amount, dumper_wallet))
                        else:
                            cursor.execute("""
                            UPDATE wallets 
                            SET role = 'dumper',
                                notes = COALESCE(notes, '') || 'Dumper (fallback): largest token->SOL swap (' || ? || ' SOL); '
                            WHERE address = ?
                            """, (to_amount, dumper_wallet))
        
        conn.commit()
        logger.info(f"Анализ ролей для токена {token_address} завершен (Creator: {creator_wallet}, Dumper: {dumper_wallet})")
        
    except Exception as e:
        logger.error(f"Ошибка при анализе ролей кошельков для токена {token_address}: {str(e)}")
        conn.rollback()

def extract_wallets_from_transaction(conn, signature):
    """
    Извлекает адреса кошельков из raw_json транзакции.
    
    Args:
        conn: Соединение с базой данных
        signature: Подпись транзакции
        
    Returns:
        tuple: (fee_payer, involved_accounts)
    """
    cursor = conn.cursor()
    cursor.execute("SELECT raw_json FROM transactions WHERE signature = ?", (signature,))
    result = cursor.fetchone()
    
    if not result or not result[0]:
        return None, []
    
    try:
        parsed = json.loads(result[0])
        
        # Извлекаем fee payer (первый аккаунт в accountKeys)
        fee_payer = None
        involved_accounts = []
        
        if 'transaction' in parsed and 'message' in parsed['transaction']:
            message = parsed['transaction']['message']
            if 'accountKeys' in message:
                accounts = message['accountKeys']
                if accounts:
                    fee_payer = accounts[0]  # Первый аккаунт - fee payer
                    involved_accounts = accounts[:5]  # Берем первые 5 аккаунтов
        
        return fee_payer, involved_accounts
        
    except Exception as e:
        logger.error(f"Ошибка при извлечении кошельков из транзакции {signature}: {str(e)}")
        return None, []

def main():
    """
    Главная функция для запуска процесса.
    """
    parser = argparse.ArgumentParser(description="Загрузка и обработка транзакций для токенов из файла.")
    parser.add_argument('--token-file', type=str, default='tokens.txt', help='Путь к файлу со списком токенов.')
    parser.add_argument('--signatures-limit', type=int, default=None, help='Ограничение на количество транзакций для каждого токена.')
    parser.add_argument('--workers', type=int, default=None, help='Количество параллельных потоков (не используется).')
    parser.add_argument('--batch-size', type=int, default=10, help='Размер пакета (не используется).')
    parser.add_argument('--db-path', type=str, default=None, help='Путь к файлу базы данных SQLite.')
    parser.add_argument('--direction', type=str, default='b', choices=['b', 'e'], help='Направление загрузки: b (от старых к новым), e (от новых к старым).')
    args = parser.parse_args()

    start_time = time.time()
    
    conn = None
    try:
        # Получаем соединение с БД
        conn = get_sqlite_connection(args.db_path)
        # Гарантируем, что все таблицы созданы по эталонной схеме
        create_tables(conn)

        # Загружаем адреса токенов
        logger.info(f"Загрузка токенов из файла: {args.token_file}")
        token_addresses = load_token_addresses(args.token_file)

        if not token_addresses:
            logger.warning("Файл с токенами пуст или не найден. Завершение работы.")
            return

        logger.info(f"Загружено {len(token_addresses)} токенов из файла {args.token_file}")

        total_tokens = len(token_addresses)
        success_count = 0
        error_count = 0
        total_processed_signatures = 0

        # Последовательная обработка токенов
        for i, token_address in enumerate(token_addresses, 1):
            logger.info(f"--- Обработка токена {i}/{total_tokens}: {token_address} ---")
            try:
                # В process_token_batch создается свой RPCClient
                processed_signatures_count = process_token_batch(
                    token_addresses=[token_address],
                    rpc_client=RPCClient(), # Создаем новый клиент для каждого токена
                    conn=conn,
                    signatures_limit=args.signatures_limit,
                    direction=args.direction
                )

                if processed_signatures_count > 0:
                    logger.info(f"Токен {token_address} успешно обработан, {processed_signatures_count} новых транзакций.")
                    success_count += 1
                    total_processed_signatures += processed_signatures_count
                else:
                    logger.info(f"Для токена {token_address} нет новых транзакций или произошла ошибка.")
                    error_count += 1
                
                # Обновление аналитики после каждого токена
                analyze_wallet_roles(conn, token_address)

            except Exception as e:
                logger.error(f"Критическая ошибка при обработке токена {token_address}: {e}", exc_info=True)
                error_count += 1

    except Exception as e:
        logger.error(f"Произошла глобальная ошибка: {e}", exc_info=True)
    finally:
        if conn:
            conn.close()
            logger.info("Соединение с базой данных закрыто.")

    total_duration = time.time() - start_time
    logger.info("--- Завершено ---")
    logger.info(f"Всего токенов: {total_tokens}, успешно: {success_count}, с ошибками/без новых tx: {error_count}")
    logger.info(f"Всего новых подписей обработано: {total_processed_signatures}")
    logger.info(f"Общее время выполнения: {datetime.timedelta(seconds=total_duration)}")

if __name__ == "__main__":
    main() 