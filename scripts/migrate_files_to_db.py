#!/usr/bin/env python3
"""
Скрипт для миграции существующих данных из файловой системы в базу данных SQLite.
Часть плана перехода к SSOT-архитектуре.

Этот скрипт:
1. Рекурсивно обходит директорию data/transactions/
2. Читает каждый JSON-файл с транзакцией
3. Обрабатывает транзакции с новой логикой обогащения (включая SOL analysis)
4. Использует UPSERT для атомарного обновления/вставки записей
"""
import os
import sys
import json
import logging
import argparse
import sqlite3
import time
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from decoder.decoder import TransactionDecoder
from decoder.enrichers.net_token_change_enricher import NetTokenChangeEnricher
from decoder.enrichers.compute_unit_enricher import ComputeUnitEnricher
from decoder.enrichers.sequence_enricher import SequenceEnricher
from db.db_writer import upsert_transaction_sqlite

# Директория с транзакциями
TRANSACTIONS_DIR = "data/transactions"

# Настройка логирования
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/migration.log"),
        logging.StreamHandler()
    ]
)

# Отдельный лог для ошибок БД
db_error_logger = logging.getLogger("db_error")
db_error_handler = logging.FileHandler("logs/db_migration_errors.log")
db_error_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
db_error_handler.setFormatter(db_error_formatter)
db_error_logger.addHandler(db_error_handler)
db_error_logger.propagate = False

logger = logging.getLogger("migration")

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

def check_transaction_exists(conn, signature: str) -> bool:
    """
    Проверяет, существует ли транзакция в БД
    
    Args:
        conn: соединение с БД
        signature: сигнатура транзакции
        
    Returns:
        True если транзакция уже есть в БД, иначе False
    """
    if not signature:
        logger.warning("Попытка проверить транзакцию с пустой сигнатурой")
        return False
        
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM transactions WHERE signature = ? LIMIT 1", (signature,))
        result = cursor.fetchone()
        return result is not None
    except Exception as e:
        logger.error(f"Ошибка при проверке наличия транзакции {signature}: {str(e)}")
        return False

def find_all_transaction_files() -> List[Tuple[str, str]]:
    """
    Находит все файлы с транзакциями в директории data/transactions/
    
    Returns:
        Список кортежей (путь к файлу, токен)
    """
    transaction_files = []
    
    if not os.path.exists(TRANSACTIONS_DIR):
        logger.error(f"Директория {TRANSACTIONS_DIR} не существует")
        return transaction_files
    
    # Обходим все поддиректории (токены)
    for token_dir in os.listdir(TRANSACTIONS_DIR):
        token_path = os.path.join(TRANSACTIONS_DIR, token_dir)
        
        if not os.path.isdir(token_path):
            continue
        
        # Получаем список JSON-файлов в директории токена
        for file_name in os.listdir(token_path):
            # Пропускаем signatures.json и all_transactions.json
            if file_name == "signatures.json" or file_name == "all_transactions.json":
                continue
                
            if file_name.endswith(".json"):
                file_path = os.path.join(token_path, file_name)
                transaction_files.append((file_path, token_dir))
    
    return transaction_files

def process_single_transaction(tx_data: Dict[str, Any], decoder: TransactionDecoder, token_mint: str, conn) -> Dict[str, Any]:
    """
    Обрабатывает одну транзакцию и сохраняет её в БД используя UPSERT
    
    Args:
        tx_data: данные транзакции
        decoder: экземпляр декодера
        token_mint: адрес токена
        conn: соединение с БД
        
    Returns:
        Словарь с результатом обработки
    """
    try:
        # Получаем сигнатуру транзакции
        signature = tx_data.get("transaction", {}).get("signatures", ["unknown"])[0]
        if not signature or signature == "unknown":
            signature = tx_data.get("signature", "unknown")
        
        # Проверяем, существует ли транзакция в БД
        transaction_exists = signature and check_transaction_exists(conn, signature)
        
        # Декодируем транзакцию с новой логикой обогащения (включая SOL analysis)
        enriched_data = decoder.decode_transaction(tx_data)
        
        # Определяем тип транзакции на основе обогащенных данных
        transaction_type = "unknown"
        if isinstance(enriched_data, list):
            for event in enriched_data:
                if isinstance(event, dict):
                    event_type = event.get('type', '')
                    if event_type == 'swap':
                        transaction_type = 'swap'
                        break
                    elif event_type == 'transfer':
                        transaction_type = 'transfer'
                elif hasattr(event, 'event_type'):
                    if event.event_type == 'swap':
                        transaction_type = 'swap'
                        break
                    elif event.event_type == 'transfer':
                        transaction_type = 'transfer'
        
        # Извлекаем основные поля транзакции
        block_time = tx_data.get("blockTime")
        slot = tx_data.get("slot")
        fee_payer = None
        
        # Определяем fee_payer
        if tx_data.get("transaction", {}).get("message", {}).get("accountKeys"):
            fee_payer = tx_data["transaction"]["message"]["accountKeys"][0]
        
        # Используем UPSERT для атомарной вставки/обновления
        try:
            success = upsert_transaction_sqlite(
                conn=conn,
                signature=signature,
                block_time=block_time,
                slot=slot,
                fee_payer=fee_payer,
                transaction_type=transaction_type,
                raw_json=tx_data,
                enriched_data=enriched_data,
                source_query_type="file_migration",
                source_query_address=token_mint,
                parser_version="2.0.0"  # Новая версия с SOL analysis
            )
            
            if success:
                if transaction_exists:
                    logger.info(f"Транзакция {signature[:10]}... успешно обновлена с новой SOL-логикой")
                    return {"signature": signature, "status": "updated"}
                else:
                    logger.info(f"Транзакция {signature[:10]}... успешно мигрирована")
                    return {"signature": signature, "status": "migrated"}
            else:
                error_message = f"UPSERT вернул False для транзакции {signature}"
                logger.error(error_message)
                return {"signature": signature, "status": "error", "error": error_message}
                
        except Exception as e:
            error_message = f"Ошибка при UPSERT транзакции {signature}: {str(e)}"
            logger.error(error_message)
            db_error_logger.critical(f"SIGNATURE: {signature}\nERROR: {str(e)}\n{'-'*50}")
            return {"signature": signature, "status": "error", "error": str(e)}
        
    except Exception as e:
        logger.error(f"Ошибка при обработке транзакции: {str(e)}")
        return {"status": "error", "error": str(e)}

def load_transaction_from_file(file_path: str) -> Dict[str, Any]:
    """
    Загружает транзакцию из файла
    
    Args:
        file_path: путь к файлу
        
    Returns:
        Словарь с данными транзакции или None в случае ошибки
    """
    encodings = ['utf-8', 'utf-8-sig', 'latin1', 'cp1251']
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return json.load(f)
        except UnicodeDecodeError:
            # Пробуем следующую кодировку
            continue
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON в файле {file_path}: {str(e)}")
            return None
        except Exception as e:
            logger.error(f"Ошибка при загрузке файла {file_path}: {str(e)}")
            return None
    
    # Если не удалось прочитать ни с одной кодировкой, пробуем бинарный режим
    try:
        with open(file_path, 'rb') as f:
            return json.loads(f.read().decode('utf-8', errors='ignore'))
    except Exception as e:
        logger.error(f"Не удалось прочитать файл {file_path} ни с одной кодировкой: {str(e)}")
        return None

def create_tables_if_not_exist(conn):
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
    
    # Создаем индексы
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_signature ON transactions(signature)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_fee_payer ON transactions(fee_payer)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_transactions_source_query_address ON transactions(source_query_address)')
    
    conn.commit()
    logger.info("Таблицы в базе данных SQLite созданы или уже существуют.")

def migrate_transactions(limit: Optional[int] = None, dry_run: bool = False):
    """
    Выполняет миграцию транзакций из файловой системы в БД с использованием UPSERT
    
    Args:
        limit: максимальное количество транзакций для обработки (None - без ограничений)
        dry_run: если True, не выполняет реальную запись в БД
    """
    start_time = time.time()
    
    # Находим все файлы с транзакциями
    transaction_files = find_all_transaction_files()
    file_count = len(transaction_files)
    logger.info(f"Найдено {file_count} файлов с транзакциями")
    
    if not transaction_files:
        logger.info("Нет файлов для миграции")
        return
    
    # Ограничиваем количество файлов, если указан limit
    if limit is not None and limit > 0 and limit < file_count:
        transaction_files = transaction_files[:limit]
        logger.info(f"Ограничение: будет обработано только {limit} файлов")
    
    # Создаем декодер с новыми enrichers (включая SOL analysis)
    enrichers = [
        NetTokenChangeEnricher(),  # Включает SOL analysis
        ComputeUnitEnricher(),
        SequenceEnricher()
    ]
    decoder = TransactionDecoder(enrichers=enrichers)
    
    # Счетчики для статистики
    migrated_count = 0
    updated_count = 0
    error_count = 0
    
    # Подключаемся к БД
    try:
        conn = get_sqlite_connection() if not dry_run else None
        
        if not dry_run:
            logger.info("Успешное подключение к SQLite БД")
            
            # Создаем таблицы, если их нет
            create_tables_if_not_exist(conn)
        else:
            logger.info("Режим dry run: данные не будут записаны в БД")
        
        # Обрабатываем файлы
        for i, (file_path, token_mint) in enumerate(transaction_files):
            logger.info(f"[{i+1}/{len(transaction_files)}] Обработка файла {file_path}")
            
            # Загружаем транзакцию
            tx_data = load_transaction_from_file(file_path)
            if tx_data is None:
                error_count += 1
                continue
            
            if not dry_run:
                # Обрабатываем транзакцию с UPSERT
                result = process_single_transaction(tx_data, decoder, token_mint, conn)
                
                # Обновляем статистику
                if result.get("status") == "migrated":
                    migrated_count += 1
                    logger.info(f"Транзакция {result.get('signature')[:10]}... успешно мигрирована")
                elif result.get("status") == "updated":
                    updated_count += 1
                    logger.info(f"Транзакция {result.get('signature')[:10]}... обновлена с новой SOL-логикой")
                else:
                    error_count += 1
                    logger.error(f"Ошибка при миграции транзакции: {result.get('error')}")
            else:
                # В режиме dry run просто проверяем формат данных
                signature = tx_data.get("transaction", {}).get("signatures", ["unknown"])[0]
                if not signature or signature == "unknown":
                    signature = tx_data.get("signature", "unknown")
                logger.info(f"[DRY RUN] Транзакция {signature[:10]}... готова к миграции")
            
            # Выводим промежуточную статистику каждые 10 файлов
            if (i + 1) % 10 == 0:
                elapsed_time = time.time() - start_time
                files_per_second = (i + 1) / elapsed_time if elapsed_time > 0 else 0
                logger.info(f"Прогресс: {i + 1}/{len(transaction_files)} файлов "
                          f"({(i + 1) / len(transaction_files) * 100:.2f}%). "
                          f"Скорость: {files_per_second:.2f} файлов/с")
                logger.info(f"Статистика: мигрировано - {migrated_count}, "
                          f"обновлено - {updated_count}, ошибки - {error_count}")
        
        # Итоговая статистика
        elapsed_time = time.time() - start_time
        files_per_second = len(transaction_files) / elapsed_time if elapsed_time > 0 else 0
        
        logger.info("=== Итоги миграции ===")
        logger.info(f"Всего файлов: {len(transaction_files)}")
        logger.info(f"Мигрировано новых транзакций: {migrated_count}")
        logger.info(f"Обновлено существующих транзакций: {updated_count}")
        logger.info(f"Ошибки: {error_count}")
        logger.info(f"Время выполнения: {elapsed_time:.2f} с")
        logger.info(f"Скорость: {files_per_second:.2f} файлов/с")
        
    except Exception as e:
        logger.error(f"Ошибка при работе с БД: {str(e)}")
    finally:
        if 'conn' in locals() and conn is not None:
            conn.close()
            logger.info("Соединение с БД закрыто")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Миграция транзакций из файловой системы в базу данных SQLite с UPSERT")
    parser.add_argument("--limit", "-l", type=int, help="Максимальное количество транзакций для обработки")
    parser.add_argument("--dry-run", "-d", action="store_true", help="Не выполнять реальную запись в БД")
    parser.add_argument("--verify-only", "-v", action="store_true", help="Только проверить наличие транзакций в БД, без миграции")
    
    args = parser.parse_args()
    
    if args.verify_only:
        print("Режим проверки: только проверка наличия транзакций в БД")
        # Здесь можно добавить код для проверки наличия транзакций в БД
        sys.exit(0)
    
    migrate_transactions(limit=args.limit, dry_run=args.dry_run)
