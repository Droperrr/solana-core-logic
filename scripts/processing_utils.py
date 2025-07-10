#!/usr/bin/env python3
"""
Утилитарный модуль для обработки транзакций.
Содержит логику, извлеченную из batch_process_transactions_sqlite.py для устранения циклических зависимостей.
"""
import os
import sys
import json
import logging
import sqlite3
from typing import List, Dict, Any, Optional
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from rpc.client import RPCClient
# Заменяем старый декодер на новый функциональный подход
from decoder import process_transaction as decode_and_process_transaction
from utils.signature_handler import fetch_signatures_for_token
from api.services.progress_service import update_token_progress_in_db
from utils.parsing_utils import to_serializable_dict
from db.db_writer import log_failed_transaction

# Настройка логирования
logger = logging.getLogger("processing_utils")
db_error_logger = logging.getLogger("db_error")


def dict_factory(cursor, row):
    """Конвертирует строки результатов запроса в словари"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_sqlite_connection(db_path=None):
    """
    Создает и возвращает соединение с базой данных SQLite.
    По умолчанию использует файл db/solana_db.sqlite в директории проекта.
    """
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')
    
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    return conn

def create_tables(conn):
    """
    Создает все необходимые таблицы в базе данных SQLite по эталонной схеме,
    если они еще не созданы.
    """
    cursor = conn.cursor()

    # Таблица для хранения транзакций
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signature TEXT NOT NULL UNIQUE,
        block_time INTEGER,
        slot INTEGER,
        fee_payer TEXT,
        transaction_type TEXT,
        source_query_type TEXT,
        source_query_address TEXT,
        raw_json TEXT,
        enriched_data TEXT,
        parser_version TEXT,
        updated_at TIMESTAMP,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')

    # Таблица для отслеживания прогресса сбора данных по токенам
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS token_collection_progress (
        token_address TEXT PRIMARY KEY,
        on_chain_tx_count INTEGER,
        db_tx_count INTEGER,
        completeness_ratio REAL,
        status TEXT DEFAULT 'unknown',
        last_checked_at INTEGER,
        last_collection_at INTEGER,
        error_message TEXT,
        created_at INTEGER DEFAULT (strftime('%s', 'now')),
        updated_at INTEGER
    )
    ''')

    # Таблица для хранения информации о кошельках
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS wallets (
        address TEXT PRIMARY KEY,
        role TEXT DEFAULT 'external',
        first_seen_timestamp INTEGER,
        last_seen_timestamp INTEGER,
        token_interaction_count INTEGER DEFAULT 1,
        notes TEXT
    )
    ''')

    # Таблица для хранения аналитических событий
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS ml_ready_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        signature TEXT,
        block_time INTEGER,
        token_a_mint TEXT,
        token_b_mint TEXT,
        event_type TEXT,
        from_amount REAL,
        to_amount REAL,
        from_wallet TEXT,
        to_wallet TEXT,
        platform TEXT,
        wallet_tag TEXT,
        parser_version TEXT,
        enriched_data TEXT,
        program_id TEXT,
        instruction_name TEXT,
        event_data_raw TEXT,
        net_token_changes TEXT,
        involved_accounts TEXT,
        compute_units_consumed INTEGER
    )
    ''')

    # Таблица для хранения жизненного цикла токена
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS token_lifecycle (
        token_address TEXT PRIMARY KEY,
        creation_signature TEXT,
        creation_time INTEGER,
        first_dump_signature TEXT,
        first_dump_time INTEGER,
        first_dump_price_drop_percent REAL,
        last_processed_signature TEXT
    )
    ''')

    # Создание индексов для ускорения запросов
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_signature ON transactions(signature)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_fee_payer ON transactions(fee_payer)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_source_query_address ON transactions(source_query_address)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_ready_events_signature ON ml_ready_events(signature)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_ready_events_block_time ON ml_ready_events(block_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_a_mint ON ml_ready_events(token_a_mint)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_b_mint ON ml_ready_events(token_b_mint)')


    conn.commit()
    logger.info("Все таблицы в базе данных SQLite проверены/созданы в соответствии с эталонной схемой.")

def check_existing_signatures(conn, signature_list: list[str]) -> set[str]:
    """
    Проверяет, какие подписи транзакций уже существуют в базе данных.
    """
    logger.info(f"DEBUG: Checking {len(signature_list)} signatures against the DB.")
    if not signature_list:
        return set()
    
    existing_signatures = set()
    try:
        cursor = conn.cursor()
        batch_size = 500
        for i in range(0, len(signature_list), batch_size):
            batch = signature_list[i:i+batch_size]
            placeholders = ', '.join(['?'] * len(batch))
            query = f"SELECT signature FROM transactions WHERE signature IN ({placeholders})"
            cursor.execute(query, batch)
            for row in cursor.fetchall():
                existing_signatures.add(row['signature'])
    except Exception as error:
        logger.error(f"Ошибка при проверке существующих сигнатур: {error}", exc_info=True)
    
    logger.info(f"DEBUG: Found {len(existing_signatures)} existing signatures in DB.")
    return existing_signatures

def load_token_addresses(file_path: str) -> List[str]:
    """
    Загружает список адресов токенов из текстового файла.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            addresses = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
            logger.info(f"Загружено {len(addresses)} токенов из файла {file_path}")
            return addresses
    except Exception as e:
        logger.error(f"Ошибка при загрузке файла со списком токенов {file_path}: {str(e)}")
        return []

def save_transaction_to_sqlite(conn, data: Dict[str, Any]):
    """
    Сохраняет данные транзакции в базу данных SQLite.
    Транзакция (commit) должна управляться извне.
    
    Args:
        conn: Соединение с базой данных SQLite.
        data: Словарь с данными транзакции.
    """
    signature = data.get('signature', 'UNKNOWN')
    logger.info(f"НАЧАЛО СОХРАНЕНИЯ для signature: {signature}")
    logger.info(f"ПОЛУЧЕНЫ ДАННЫЕ ДЛЯ СОХРАНЕНИЯ: \n{json.dumps(data, indent=2, ensure_ascii=False)}")
    
    try:
        cursor = conn.cursor()
        
        # Преобразуем raw_json и enriched_data в JSON-строки
        if isinstance(data.get('raw_json'), (dict, list)):
            logger.info(f"ПРЕОБРАЗОВАНИЕ raw_json для {signature}")
            data['raw_json'] = json.dumps(data['raw_json'])
        if isinstance(data.get('enriched_data'), (dict, list)):
            logger.info(f"ПРЕОБРАЗОВАНИЕ enriched_data для {signature}")
            data['enriched_data'] = json.dumps(data['enriched_data'])
            
        logger.info(f"ПОДГОТОВКА К ЗАПИСИ В БД для {signature}: enriched_data = {data.get('enriched_data', 'None')[:200]}...")
        
        # Подготовка запроса вставки или обновления
        cursor.execute('''
        INSERT OR REPLACE INTO transactions (
            signature, block_time, slot, fee_payer, transaction_type, 
            source_query_type, source_query_address, raw_json, 
            enriched_data, parser_version, updated_at
        ) VALUES (
            :signature, :block_time, :slot, :fee_payer, :transaction_type, 
            :source_query_type, :source_query_address, :raw_json, 
            :enriched_data, :parser_version, CURRENT_TIMESTAMP
        )
        ''', data)
        
        logger.info(f"SQL-ЗАПРОС ВЫПОЛНЕН для {signature}, rowcount: {cursor.rowcount}")
        # conn.commit() # Commit removed to be handled externally

    except sqlite3.Error as e:
        # Логируем ошибку с полными данными для отладки
        error_message = f"Ошибка записи в SQLite для транзакции {data.get('signature')}: {e}"
        # db_error_logger.error(error_message, exc_info=True)
        # db_error_logger.error(f"Data: {json.dumps(data, indent=2)}")
        logger.error(error_message) # Также дублируем в основной лог для видимости

    except Exception as e:
        error_message = f"Неожиданная ошибка при сохранении транзакции {data.get('signature')}: {e}"
        # db_error_logger.error(error_message, exc_info=True)
        # db_error_logger.error(f"Data: {json.dumps(data, indent=2)}")
        logger.error(error_message)

def save_ml_ready_event(conn, event: 'EnrichedEvent'):
    """Saves a single enriched event to the ml_ready_events table."""
    try:
        cursor = conn.cursor()
        
        # Extract swap-specific data from enrichment if present
        swap_data = getattr(event, 'enrichment_data', {}) or {}
        
        data_to_insert = {
            "signature": event.tx_signature,
            "block_time": event.block_time,
            "token_a_mint": swap_data.get('swap_token_in'),
            "token_b_mint": swap_data.get('swap_token_out'),
            "event_type": event.event_type,
            "from_amount": swap_data.get('swap_amount_in'),
            "to_amount": swap_data.get('swap_amount_out'),
            "from_wallet": None, # Not easily available in this context
            "to_wallet": None, # Not easily available in this context
            "platform": event.protocol,
            "wallet_tag": None, # Placeholder for future use
            "parser_version": '2.0',
            "enriched_data": json.dumps(swap_data),
            "program_id": event.program_id,
            "instruction_name": event.instruction_type,
            "event_data_raw": json.dumps(to_serializable_dict(event.protocol_details)),
            "net_token_changes": json.dumps(event.net_token_changes),
            "involved_accounts": json.dumps(event.involved_accounts),
            "compute_units_consumed": event.compute_units_consumed
        }

        columns = ', '.join(data_to_insert.keys())
        placeholders = ', '.join(f':{k}' for k in data_to_insert.keys())
        
        query = f"INSERT INTO ml_ready_events ({columns}) VALUES ({placeholders})"
        
        cursor.execute(query, data_to_insert)

    except Exception as e:
        logger.error(f"Failed to save ML-ready event for signature {event.tx_signature}: {e}", exc_info=True)


def process_transaction_signatures(
    signatures: List[str], 
    rpc_client: RPCClient, 
    source_query_type: str, 
    source_query_address: str, 
    conn=None
):
    """
    Обрабатывает список подписей транзакций, извлекает данные, декодирует и сохраняет в БД.
    Эта функция была переработана для использования нового функционального декодера.
    """
    if not signatures:
        logger.info("Нет новых подписей для обработки.")
        return

    logger.info(f"Начало обработки {len(signatures)} транзакций для {source_query_address}...")
    
    for signature in signatures:
        try:
            logger.info(f"=== ОБРАБОТКА ТРАНЗАКЦИИ: {signature} ===")
            
            # Шаг 1: Получаем сырую транзакцию
            raw_tx = rpc_client.get_transaction(signature)
            if not raw_tx:
                logger.warning(f"Не удалось получить данные транзакции для подписи: {signature}")
                continue

            # Шаг 2: Используем новый функциональный декодер
            # Он может вернуть одно событие или список событий
            logger.info(f"ВЫЗОВ ДЕКОДЕРА для {signature}")
            processed_events = decode_and_process_transaction(raw_tx)
            logger.info(f"ДЕКОДЕР ВЕРНУЛ для {signature}: {type(processed_events)}, количество: {len(processed_events) if processed_events else 0}")

            if processed_events:
                # Гарантируем, что работаем со списком
                if not isinstance(processed_events, list):
                    processed_events = [processed_events]

                if not processed_events:
                    logger.warning(f"Декодер вернул пустой список для транзакции: {signature}")
                    continue

                # Шаг 3: Агрегируем данные для записи в БД
                first_event = processed_events[0]
                
                # Pydantic модели нужно конвертировать в словари
                first_event_dict = first_event.model_dump()
                logger.info(f"ПЕРВОЕ СОБЫТИЕ для {signature}: {type(first_event)}")

                # Собираем одну запись для БД
                db_record = {
                    'signature': signature,
                    'block_time': first_event_dict.get('block_time'),
                    'slot': first_event_dict.get('slot'),
                    'fee_payer': first_event_dict.get('initiator'),
                    # Объединяем типы событий, если их несколько
                    'transaction_type': ', '.join(event.event_type for event in processed_events),
                    'source_query_type': source_query_type,
                    'source_query_address': source_query_address,
                    'raw_json': raw_tx,
                    # Сохраняем полный список обогащенных событий
                    'enriched_data': [event.model_dump(mode='json') for event in processed_events],
                    'parser_version': '2.0' # Указываем версию нового пайплайна
                }
                
                logger.info(f"ПОДГОТОВЛЕНА ЗАПИСЬ ДЛЯ БД для {signature}: enriched_data содержит {len(db_record['enriched_data'])} событий")
                
                # Проверяем enriched_data перед сохранением
                if not db_record['enriched_data'] or (isinstance(db_record['enriched_data'], list) and len(db_record['enriched_data']) == 0):
                    logger.warning(f"ENRICHED_DATA пуст для {signature}, записываем в DLQ")
                    log_failed_transaction(conn, signature, "ENRICHMENT_EMPTY", raw_tx)
                    continue
                
                # Сохраняем основную запись транзакции
                logger.info(f"ВЫЗОВ save_transaction_to_sqlite для {signature}")
                save_transaction_to_sqlite(conn, db_record)

                # Сохраняем каждое событие в ml_ready_events
                for i, event in enumerate(processed_events):
                    logger.info(f"СОХРАНЕНИЕ ML-события {i+1}/{len(processed_events)} для {signature}")
                    save_ml_ready_event(conn, event)
                
                # Коммитим транзакцию в БД после всех вставок
                logger.info(f"КОММИТ транзакции в БД для {signature}")
                conn.commit()
                
                logger.info(f"Транзакция {signature} успешно обработана и сохранена ({len(processed_events)} событий).")
            else:
                logger.warning(f"Не удалось обработать транзакцию: {signature}. Результат пуст.")
        
        except Exception as e:
            logger.error(f"Критическая ошибка при обработке транзакции {signature}: {e}", exc_info=True)
            conn.rollback()

    return len(signatures)

def process_token_batch(token_addresses: List[str], rpc_client: RPCClient, conn,
                      signatures_limit: int = None, direction: str = 'b', **kwargs) -> int:
    """
    Обрабатывает пакет токенов, собирает для них транзакции и запускает их обработку.
    Возвращает количество успешно обработанных подписей.
    """
    logger.info(f"Начинаем обработку {len(token_addresses)} токенов с максимальным числом подписей {signatures_limit}, направление: {direction}")
    total_processed_count = 0

    for i, token_address in enumerate(token_addresses):
        logger.info(f"--- Обработка токена {i+1}/{len(token_addresses)}: {token_address} ---")
        try:
            # 1. Получаем все сигнатуры для токена
            signatures_info = fetch_signatures_for_token(
                token_mint_address=token_address,
                fetch_limit_per_call=1000,
                total_tx_limit=signatures_limit,
                direction=direction
            )
            all_signatures = [s['signature'] for s in signatures_info]
            logger.info(f"Найдено {len(all_signatures)} всего транзакций для {token_address}.")

            # 2. Проверяем, какие из них уже есть в БД
            existing_signatures = check_existing_signatures(conn, all_signatures)
            signatures_to_process = [sig for sig in all_signatures if sig not in existing_signatures]
            
            logger.info(f"Необходимо обработать {len(signatures_to_process)} новых транзакций.")

            # 3. Обрабатываем новые подписи
            processed_count = process_transaction_signatures(
                signatures=signatures_to_process,
                rpc_client=rpc_client,
                source_query_type='token',
                source_query_address=token_address,
                conn=conn
            )
            
            if processed_count:
                total_processed_count += processed_count

            # 4. Обновляем статус прогресса в БД
            update_token_progress_in_db(
                token_address=token_address,
                on_chain_tx_count=len(all_signatures),
                db_tx_count=len(existing_signatures) + (processed_count or 0),
                status='completed' if not signatures_to_process else 'in_progress'
            )
            logger.info(f"Обработка токена {token_address} завершена.")

        except Exception as e:
            logger.error(f"Ошибка при обработке токена {token_address}: {e}", exc_info=True)
            update_token_progress_in_db(
                token_address=token_address,
                status='error',
                error_message=str(e)
            )
            
    return total_processed_count 

def get_existing_signatures_for_token(conn, token_address: str) -> set:
    """Получает все существующие сигнатуры для данного токена из таблицы transactions."""
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT signature FROM transactions WHERE source_query_address = ?", (token_address,))
        signatures = {row[0] for row in cursor.fetchall()}
        logger.info(f"DEBUG: Found {len(signatures)} existing signatures for {token_address} in DB.")
        return signatures
    except sqlite3.Error as e:
        logger.error(f"Ошибка при получении существующих сигнатур для {token_address}: {e}") 