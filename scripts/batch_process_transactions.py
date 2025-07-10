#!/usr/bin/env python3
"""
Модифицированный скрипт для массовой обработки транзакций с использованием SQLite вместо PostgreSQL.
Решает проблему с кодировкой при работе с кириллическими символами.

Примеры использования:
1. Пилотная загрузка (ограниченное количество транзакций на токен):
   python scripts/batch_process_transactions.py --input tokens.txt --signatures-limit-per-token 20 --is-token-list

2. Полная загрузка всех доступных транзакций:
   python scripts/batch_process_transactions.py --input tokens.txt --is-token-list

3. Обработка списка кошельков:
   python scripts/batch_process_transactions.py --input wallets.txt --query-type wallet
"""
import os
import sys
import json
import time
import logging
import argparse
import concurrent.futures
import sqlite3
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from decoder.decoder import TransactionDecoder
from decoder.enrichers.net_token_change_enricher import NetTokenChangeEnricher
from decoder.enrichers.compute_unit_enricher import ComputeUnitEnricher
from decoder.enrichers.sequence_enricher import SequenceEnricher
from rpc.client import RPCClient
from processing.transaction_processor import process_single_transaction

# Initialize decoder with enrichers
decoder = TransactionDecoder(enrichers=[
    NetTokenChangeEnricher(),
    ComputeUnitEnricher(),
    SequenceEnricher()
])

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/batch_processing.log"),
        logging.StreamHandler()
    ]
)

# Создаём отдельный файл для логирования ошибок записи в БД
db_error_logger = logging.getLogger("db_error")
db_error_handler = logging.FileHandler("logs/db_write_errors.log")
db_error_handler.setLevel(logging.ERROR)
db_error_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
db_error_handler.setFormatter(db_error_formatter)
db_error_logger.addHandler(db_error_handler)
db_error_logger.propagate = False  # Чтобы не дублировать сообщения в основной лог

logger = logging.getLogger("batch_processor")

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
    
    Args:
        db_path (str, optional): Путь к файлу базы данных SQLite.
        
    Returns:
        Connection: Соединение с базой данных SQLite.
    """
    if db_path is None:
        # Используем стандартный путь к базе данных
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')
        
    # Создаем директорию для базы данных, если её нет
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    return conn

def create_tables(conn):
    """
    Создает необходимые таблицы в базе данных SQLite, если они еще не созданы.
    
    Args:
        conn: Соединение с базой данных SQLite.
    """
    cursor = conn.cursor()
    
    # Создаем таблицу transactions
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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Создаем индекс по полю signature
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_signature ON transactions(signature)')
    
    # Создаем индекс по полю block_time
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time)')
    
    # Создаем индекс по полю fee_payer
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_fee_payer ON transactions(fee_payer)')
    
    # Создаем индекс по полю source_query_address
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_source_query_address ON transactions(source_query_address)')
    
    # Фиксируем изменения
    conn.commit()
    logger.info("Таблицы в базе данных SQLite созданы или уже существуют.")

def check_existing_signatures(conn, signature_list: list[str]) -> set[str]:
    """
    Проверяет, какие подписи транзакций уже существуют в базе данных.
    
    Args:
        conn: Соединение с базой данных SQLite.
        signature_list: Список подписей транзакций для проверки.
        
    Returns:
        set[str]: Множество подписей транзакций, которые уже существуют в базе данных.
    """
    if not signature_list:
        return set()
    
    existing_signatures = set()
    try:
        cursor = conn.cursor()
        
        # SQLite не поддерживает оператор ANY, поэтому используем оператор IN
        # Но IN имеет ограничение на количество параметров, поэтому разбиваем на пакеты
        batch_size = 500
        for i in range(0, len(signature_list), batch_size):
            batch = signature_list[i:i+batch_size]
            placeholders = ', '.join(['?'] * len(batch))
            query = f"SELECT signature FROM transactions WHERE signature IN ({placeholders})"
            cursor.execute(query, batch)
            for row in cursor.fetchall():
                existing_signatures.add(row['signature'])
    except Exception as error:
        logger.error(f"Ошибка при проверке существующих сигнатур: {error}")
        import traceback
        traceback.print_exc()
    
    return existing_signatures

def load_token_addresses(file_path: str) -> List[str]:
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

def save_transaction_to_sqlite(conn, data):
    """
    Сохраняет данные транзакции в базу данных SQLite.
    
    Args:
        conn: Соединение с базой данных SQLite.
        data: Словарь с данными транзакции.
    """
    try:
        cursor = conn.cursor()
        
        # Преобразуем raw_json и enriched_data в JSON-строки
        if isinstance(data.get('raw_json'), (dict, list)):
            data['raw_json'] = json.dumps(data['raw_json'])
        if isinstance(data.get('enriched_data'), (dict, list)):
            data['enriched_data'] = json.dumps(data['enriched_data'])
            
        # Добавляем версию парсера, если она есть
        if not data.get('parser_version') and data.get('enriched_data'):
            data['parser_version'] = 'v1.0-enrichers'
            
        # Подготовка запроса вставки или обновления
        cursor.execute('''
        INSERT OR REPLACE INTO transactions 
        (signature, block_time, slot, fee_payer, transaction_type, source_query_type, source_query_address, raw_json, enriched_data, parser_version, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('signature'),
            data.get('block_time'),
            data.get('slot'),
            data.get('fee_payer'),
            data.get('transaction_type'),
            data.get('source_query_type'),
            data.get('source_query_address'),
            data.get('raw_json'),
            data.get('enriched_data'),
            data.get('parser_version'),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        ))
        
        conn.commit()
    except Exception as e:
        logger.error(f"[DB_WRITER_ERROR][{data.get('signature')[:6]}] Ошибка при сохранении транзакции: {e}")
        db_error_logger.error(f"[{data.get('signature')}] Ошибка: {e}")
        conn.rollback()
        import traceback
        traceback.print_exc()
        raise

def process_transaction_signatures(signatures: List[Dict[str, Any]], rpc_client: RPCClient, 
                               source_query_type: str, source_query_address: str, db_path: str):
    """
    Обрабатывает список подписей транзакций, загружая для каждой подписи полные данные транзакции.
    
    Args:
        signatures: Список словарей с данными подписей
        rpc_client: Клиент RPC для запроса данных транзакций
        source_query_type: Тип источника запроса ('token' или 'wallet')
        source_query_address: Адрес источника запроса
        db_path: Путь к файлу базы данных SQLite
    """
    logger.info(f"Обрабатываем {len(signatures)} подписей транзакций")
    
    # Создаем отдельное соединение для потока
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # Инициализируем TransactionDecoder со всеми необходимыми обогатителями
    enrichers = [
        NetTokenChangeEnricher(),
        ComputeUnitEnricher(),
        SequenceEnricher()
    ]
    decoder = TransactionDecoder(enrichers=enrichers)
    
    try:
        for sig_data in signatures:
            signature = sig_data.get('signature')
            if not signature:
                logger.warning("Пропуск записи без подписи")
                continue
            
            # Проверяем, существует ли уже такая транзакция в БД
            cursor = conn.cursor()
            cursor.execute("SELECT raw_json FROM transactions WHERE signature = ?", (signature,))
            existing_tx = cursor.fetchone()
            
            if existing_tx:
                logger.info(f"Транзакция {signature[:10]}... уже есть в базе, используем существующие данные (экономия кредитов Helius)")
                # Используем существующие raw_json данные вместо повторного запроса к Helius
                try:
                    tx_data = json.loads(existing_tx[0]) if existing_tx[0] else None
                    if not tx_data:
                        logger.warning(f"Некорректные raw_json данные для транзакции {signature[:10]}..., пропускаем")
                        continue
                except json.JSONDecodeError as e:
                    logger.error(f"Ошибка декодирования raw_json для транзакции {signature[:10]}...: {e}")
                    continue
            else:
                logger.info(f"Запрашиваем данные для транзакции {signature[:10]}... из Helius")
                try:
                    # Получаем полные данные транзакции
                    tx_data = rpc_client.get_transaction(signature)
                    
                    if not tx_data:
                        logger.warning(f"Не удалось получить данные для транзакции {signature[:10]}...")
                        continue
                except Exception as e:
                    logger.error(f"Ошибка при запросе транзакции {signature[:10]}... из Helius: {e}")
                    continue
            
            # Теперь у нас есть tx_data (либо из базы, либо из Helius)
            try:
                
                # Декодируем и обогащаем данные транзакции
                enriched_data = decoder.decode_transaction(tx_data)
                
                # Преобразуем в формат, подходящий для сохранения
                prepared_data = {
                    'signature': signature,
                    'block_time': tx_data.get('blockTime'),
                    'slot': tx_data.get('slot'),
                    'fee_payer': tx_data.get('transaction', {}).get('message', {}).get('accountKeys', [])[0] 
                                if tx_data.get('transaction', {}).get('message', {}).get('accountKeys', []) else None,
                    'transaction_type': 'unknown',  # Мы установим тип позже, после парсинга
                    'raw_json': tx_data,
                    'enriched_data': enriched_data,  # Используем обогащенные данные
                    'source_query_type': source_query_type,
                    'source_query_address': source_query_address
                }
                
                # Используем UPSERT для атомарной операции вставки/обновления
                from db.db_writer import upsert_transaction_sqlite
                
                success = upsert_transaction_sqlite(
                    conn=conn,
                    signature=prepared_data['signature'],
                    block_time=prepared_data['block_time'],
                    slot=prepared_data['slot'],
                    fee_payer=prepared_data['fee_payer'],
                    transaction_type=prepared_data['transaction_type'],
                    raw_json=prepared_data['raw_json'],
                    enriched_data=prepared_data['enriched_data'],
                    source_query_type=prepared_data['source_query_type'],
                    source_query_address=prepared_data['source_query_address'],
                    parser_version='2.0.0'  # Новая версия парсера с улучшенной логикой
                )
                
                if success:
                    if existing_tx:
                        logger.info(f"Транзакция {signature[:10]}... успешно обновлена с новыми обогащенными данными")
                    else:
                        logger.info(f"Транзакция {signature[:10]}... успешно сохранена")
                else:
                    logger.error(f"Ошибка при сохранении/обновлении транзакции {signature[:10]}...")
            except Exception as e:
                logger.error(f"Ошибка при обработке транзакции {signature[:10]}...: {e}")
                import traceback
                traceback.print_exc()
                conn.rollback()
                continue
    finally:
        # Закрываем соединение после завершения работы потока
        conn.close()

def process_token_batch(token_addresses: List[str], rpc_client: RPCClient, db_path: str, 
                      workers: int = 4, batch_size: int = 20, signatures_limit: int = 100):
    """
    Обрабатывает пакет токенов, загружая их транзакции через RPC.
    
    Args:
        token_addresses: Список адресов токенов для обработки
        rpc_client: Клиент RPC для запроса данных транзакций
        db_path: Путь к файлу базы данных SQLite
        workers: Количество одновременных рабочих потоков для загрузки
        batch_size: Размер пакета адресов для обработки в одном потоке
        signatures_limit: Максимальное количество подписей для загрузки для каждого токена
                         (используется для ограничения количества транзакций при пилотной загрузке)
    """
    logger.info(f"Начинаем обработку {len(token_addresses)} токенов с максимальным числом подписей на токен: {signatures_limit}")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for i in range(0, len(token_addresses), batch_size):
            batch = token_addresses[i:i+batch_size]
            futures = []
            
            for token_address in batch:
                logger.info(f"Запрашиваем подписи для токена {token_address} (лимит: {signatures_limit})")
                try:
                    # Используем метод get_signatures_for_address вместо несуществующего get_signatures_for_token
                    # Токен-адрес используется как адрес аккаунта для запроса связанных транзакций
                    signatures = rpc_client.get_signatures_for_address(token_address, limit=signatures_limit)
                    if not signatures:
                        logger.warning(f"Не найдено подписей для токена {token_address}")
                        continue
                    
                    logger.info(f"Получено {len(signatures)} подписей для токена {token_address}")
                    
                    # Создаем задачу на обработку подписей для токена
                    future = executor.submit(
                        process_transaction_signatures,
                        signatures=signatures,
                        rpc_client=rpc_client,
                        source_query_type='token',
                        source_query_address=token_address,
                        db_path=db_path
                    )
                    futures.append(future)
                except Exception as e:
                    logger.error(f"Ошибка при обработке токена {token_address}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            # Ждем завершения всех задач в текущем пакете
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка в задаче: {e}")
            
            logger.info(f"Пакет токенов {i+1}-{min(i+batch_size, len(token_addresses))} из {len(token_addresses)} обработан")

def process_wallet_batch(wallet_addresses: List[str], rpc_client: RPCClient, db_path: str,
                       workers: int = 4, batch_size: int = 20, signatures_limit: int = 100):
    """
    Обрабатывает пакет адресов кошельков, загружая их транзакции через RPC.
    
    Args:
        wallet_addresses: Список адресов кошельков для обработки
        rpc_client: Клиент RPC для запроса данных транзакций
        db_path: Путь к файлу базы данных SQLite
        workers: Количество одновременных рабочих потоков для загрузки
        batch_size: Размер пакета адресов для обработки в одном потоке
        signatures_limit: Максимальное количество подписей для загрузки для каждого кошелька
    """
    logger.info(f"Начинаем обработку {len(wallet_addresses)} адресов кошельков с максимальным числом подписей {signatures_limit}")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for i in range(0, len(wallet_addresses), batch_size):
            batch = wallet_addresses[i:i+batch_size]
            futures = []
            
            for wallet_address in batch:
                logger.info(f"Запрашиваем подписи для кошелька {wallet_address}")
                try:
                    signatures = rpc_client.get_signatures_for_address(wallet_address, limit=signatures_limit)
                    if not signatures:
                        logger.warning(f"Не найдено подписей для кошелька {wallet_address}")
                        continue
                    
                    logger.info(f"Получено {len(signatures)} подписей для кошелька {wallet_address}")
                    
                    # Создаем задачу на обработку подписей для кошелька
                    future = executor.submit(
                        process_transaction_signatures,
                        signatures=signatures,
                        rpc_client=rpc_client,
                        source_query_type='wallet',
                        source_query_address=wallet_address,
                        db_path=db_path
                    )
                    futures.append(future)
                except Exception as e:
                    logger.error(f"Ошибка при обработке кошелька {wallet_address}: {e}")
                    import traceback
                    traceback.print_exc()
                    continue
            
            # Ждем завершения всех задач в текущем пакете
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Ошибка в задаче: {e}")
            
            logger.info(f"Пакет кошельков {i+1}-{min(i+batch_size, len(wallet_addresses))} из {len(wallet_addresses)} обработан")

def ensure_tables_exist(conn):
    """
    Создает необходимые таблицы в базе данных SQLite, если они еще не созданы.
    
    Args:
        conn: Соединение с базой данных SQLite.
    """
    cursor = conn.cursor()
    
    # Создаем таблицу transactions
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
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    
    # Создаем индекс по полю signature
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_signature ON transactions(signature)')
    
    # Создаем индекс по полю block_time
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time)')
    
    # Создаем индекс по полю fee_payer
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_fee_payer ON transactions(fee_payer)')
    
    # Создаем индекс по полю source_query_address
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_source_query_address ON transactions(source_query_address)')
    
    # Фиксируем изменения
    conn.commit()

def read_address_file(file_path: str) -> List[str]:
    """
    Читает файл с адресами (кошельков или токенов), по одному адресу на строку.
    
    Args:
        file_path: Путь к файлу с адресами
        
    Returns:
        List[str]: Список адресов
    """
    addresses = []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                # Удаляем пробелы и переводы строки
                address = line.strip()
                if address and not address.startswith('#'):
                    addresses.append(address)
    except Exception as e:
        logger.error(f"Ошибка при чтении файла {file_path}: {e}")
        sys.exit(1)
    
    return addresses

def main():
    # Настраиваем аргументы командной строки
    parser = argparse.ArgumentParser(description='Пакетная обработка транзакций токенов или кошельков')
    parser.add_argument('--input', required=True, help='Путь к файлу со списком адресов кошельков или токенов')
    parser.add_argument('--is-token-list', action='store_true', help='Если указан, входной файл содержит адреса токенов')
    parser.add_argument('--workers', type=int, default=4, help='Количество одновременных рабочих потоков')
    parser.add_argument('--batch-size', type=int, default=10, help='Размер пакета адресов для обработки за один раз')
    parser.add_argument('--query-type', choices=['token', 'wallet'], default='token', help='Тип запроса')
    parser.add_argument('--signatures-limit', type=int, default=50, help='Максимальное количество подписей на один адрес')
    parser.add_argument('--signatures-limit-per-token', type=int, default=None, 
                      help='Максимальное количество транзакций для каждого токена (для пилотной загрузки)')
    parser.add_argument('--token-limit', type=int, default=None, help='Ограничение количества обрабатываемых токенов/кошельков')
    parser.add_argument('--verbose', action='store_true', help='Включает подробный вывод отладочной информации')
    
    args = parser.parse_args()
    
    # Включаем подробный вывод, если нужно
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Читаем файл с адресами
    addresses = read_address_file(args.input)
    
    # Применяем ограничение, если указано
    if args.token_limit and len(addresses) > args.token_limit:
        logger.info(f"Применяем ограничение: обрабатываем только первые {args.token_limit} адресов из {len(addresses)}")
        addresses = addresses[:args.token_limit]
    
    # Определяем тип запроса
    query_type = 'token' if args.is_token_list else 'wallet'
    
    # Вычисляем путь к файлу базы данных
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'db', 'solana_db.sqlite')
    
    # Создаем директорию для базы данных, если её нет
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    
    # Инициализируем RPC клиент
    logger.info("Инициализация RPC клиента...")
    rpc_client = RPCClient()
    
    # Создаем соединение только для инициализации таблиц
    logger.info(f"Инициализация базы данных SQLite: {db_path}...")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    
    # Создаем таблицы, если они еще не созданы
    ensure_tables_exist(conn)
    
    # Закрываем соединение - каждый поток будет создавать свое
    conn.close()
    
    try:
        # Засекаем время начала обработки
        start_time = time.time()
        
        if query_type == 'token':
            # Запускаем обработку токенов
            logger.info(f"Запускаем обработку {len(addresses)} токенов...")
            # Используем signatures_limit_per_token, если он указан, иначе используем signatures_limit
            limit_per_token = args.signatures_limit_per_token if args.signatures_limit_per_token is not None else args.signatures_limit
            process_token_batch(
                addresses, rpc_client, db_path, 
                workers=args.workers, 
                batch_size=args.batch_size, 
                signatures_limit=limit_per_token
            )
        else:
            # Запускаем обработку кошельков
            logger.info(f"Запускаем обработку {len(addresses)} кошельков...")
            process_wallet_batch(
                addresses, rpc_client, db_path, 
                workers=args.workers, 
                batch_size=args.batch_size, 
                signatures_limit=args.signatures_limit
            )
        
        # Выводим статистику
        elapsed_time = time.time() - start_time
        logger.info(f"Обработка завершена за {elapsed_time:.2f} секунд")
        
        # Создаем новое соединение для подсчета статистики
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # Подсчитываем количество сохраненных транзакций
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) as count FROM transactions")
        count = cursor.fetchone()['count']
        logger.info(f"Всего записей в базе данных: {count}")
        
        # Закрываем соединение
        conn.close()
        
    except KeyboardInterrupt:
        logger.info("Обработка прервана пользователем")
    except Exception as e:
        logger.error(f"Произошла ошибка: {e}")
        import traceback
        traceback.print_exc()
    finally:
        logger.info("Обработка завершена")

if __name__ == "__main__":
    main() 