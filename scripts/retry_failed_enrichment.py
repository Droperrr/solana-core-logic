#!/usr/bin/env python3
"""
Скрипт для повторной обработки сбойных транзакций из Dead-Letter Queue.
Позволяет извлекать транзакции из failed_processing_log, переобрабатывать их
и удалять из DLQ в случае успеха.
"""
import sys
import os
import json
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Добавляем корневую директорию проекта в sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.processing_utils import get_sqlite_connection, create_tables
from decoder import process_transaction as decode_and_process_transaction
from db.db_writer import upsert_transaction_sqlite, log_failed_transaction

# Константы
MAX_RETRIES = 5

# Настройка логирования
def setup_logging():
    """Настраивает логирование в файл и консоль"""
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Настройка для файла
    file_handler = logging.FileHandler(log_dir / "reprocessing.log")
    file_handler.setLevel(logging.INFO)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    
    # Настройка для консоли
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    
    # Настройка логгера
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

def process_single_transaction(conn, signature: str, raw_json: dict, logger):
    """
    Обрабатывает одну транзакцию и сохраняет результат в БД.
    
    Args:
        conn: Соединение с БД
        signature: Подпись транзакции
        raw_json: Сырые данные транзакции
        logger: Логгер
    
    Returns:
        bool: True если обработка успешна, False в противном случае
    """
    try:
        # Декодируем транзакцию
        processed_events = decode_and_process_transaction(raw_json)
        
        if not processed_events:
            logger.warning(f"Декодер вернул пустой результат для {signature}")
            return False
        
        # Гарантируем, что работаем со списком
        if not isinstance(processed_events, list):
            processed_events = [processed_events]
        
        if not processed_events:
            logger.warning(f"Декодер вернул пустой список для {signature}")
            return False
        
        # Агрегируем данные для записи в БД
        first_event = processed_events[0]
        first_event_dict = first_event.model_dump()
        
        # Собираем запись для БД
        db_record = {
            'signature': signature,
            'block_time': first_event_dict.get('block_time'),
            'slot': first_event_dict.get('slot'),
            'fee_payer': first_event_dict.get('initiator'),
            'transaction_type': ', '.join(event.event_type for event in processed_events),
            'source_query_type': 'retry_from_dlq',
            'source_query_address': 'retry_processing',
            'raw_json': raw_json,
            'enriched_data': [event.model_dump(mode='json') for event in processed_events],
            'parser_version': '2.0'
        }
        
        # Проверяем enriched_data перед сохранением
        if not db_record['enriched_data'] or (isinstance(db_record['enriched_data'], list) and len(db_record['enriched_data']) == 0):
            logger.warning(f"ENRICHED_DATA пуст для {signature}")
            return False
        
        # Сохраняем в основную таблицу
        success = upsert_transaction_sqlite(
            conn=conn,
            signature=db_record['signature'],
            block_time=db_record['block_time'],
            slot=db_record['slot'],
            fee_payer=db_record['fee_payer'],
            transaction_type=db_record['transaction_type'],
            raw_json=db_record['raw_json'],
            enriched_data=db_record['enriched_data'],
            source_query_type=db_record['source_query_type'],
            source_query_address=db_record['source_query_address'],
            parser_version=db_record['parser_version']
        )
        
        if success:
            logger.info(f"Транзакция {signature} успешно обработана и сохранена")
            return True
        else:
            logger.error(f"Не удалось сохранить транзакцию {signature} в основную таблицу")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при обработке транзакции {signature}: {e}")
        return False

def retry_failed_transactions(conn, limit: int, reason_filter: str = None, dry_run: bool = False, force: bool = False, logger = None):
    """
    Извлекает и переобрабатывает сбойные транзакции из DLQ.
    
    Args:
        conn: Соединение с БД
        limit: Максимальное количество транзакций для обработки
        reason_filter: Фильтр по причине сбоя (опционально)
        dry_run: Режим предварительного просмотра
        force: Обрабатывать записи со статусом FAILED_PERMANENTLY
        logger: Логгер
    
    Returns:
        dict: Статистика обработки
    """
    cursor = conn.cursor()
    
    # Формируем запрос для извлечения записей
    if force:
        status_condition = ""
    else:
        status_condition = "AND status = 'RETRYABLE'"
    
    if reason_filter:
        query = f"""
        SELECT id, signature, payload, retry_count, reason 
        FROM failed_processing_log 
        WHERE reason LIKE ? {status_condition}
        ORDER BY failed_at ASC 
        LIMIT ?
        """
        params = (f"%{reason_filter}%", limit)
    else:
        query = f"""
        SELECT id, signature, payload, retry_count, reason 
        FROM failed_processing_log 
        WHERE 1=1 {status_condition}
        ORDER BY failed_at ASC 
        LIMIT ?
        """
        params = (limit,)
    
    cursor.execute(query, params)
    records = cursor.fetchall()
    
    if dry_run:
        logger.info(f"DRY RUN: Найдено {len(records)} записей для обработки")
        for record in records:
            logger.info(f"  - {record['signature']} (retry_count: {record['retry_count']}, reason: {record['reason']})")
        return {
            'found': len(records),
            'successful': 0,
            'failed': 0
        }
    
    logger.info(f"Найдено {len(records)} записей для обработки")
    
    successful = 0
    failed = 0
    
    for record in records:
        record_id = record['id']
        signature = record['signature']
        payload = record['payload']
        retry_count = record['retry_count']
        current_reason = record['reason']
        
        logger.info(f"Обработка записи {record_id}: {signature} (попытка {retry_count + 1})")
        
        try:
            # Десериализуем payload
            if payload:
                raw_json = json.loads(payload)
            else:
                logger.warning(f"Payload пуст для {signature}")
                raw_json = None
            
            if not raw_json:
                logger.warning(f"Не удалось десериализовать payload для {signature}")
                failed += 1
                continue
            
            # Начинаем транзакцию для атомарности операций
            conn.execute("BEGIN TRANSACTION")
            
            # Пытаемся обработать транзакцию
            success = process_single_transaction(conn, signature, raw_json, logger)
            
            if success:
                # Успешно обработано - удаляем из DLQ
                cursor.execute("DELETE FROM failed_processing_log WHERE id = ?", (record_id,))
                logger.info(f"Signature {signature} successfully reprocessed and removed from DLQ.")
                successful += 1
            else:
                # Не удалось обработать - обновляем запись в DLQ
                new_retry_count = retry_count + 1
                new_status = 'FAILED_PERMANENTLY' if new_retry_count >= MAX_RETRIES else 'RETRYABLE'
                new_reason = f"RETRY_FAILED: Attempt {new_retry_count} failed"
                
                cursor.execute("""
                    UPDATE failed_processing_log 
                    SET retry_count = ?, status = ?, reason = ?, failed_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                """, (new_retry_count, new_status, new_reason, record_id))
                
                logger.error(f"Signature {signature} failed to reprocess. Retry count increased to {new_retry_count}.")
                failed += 1
            
            # Завершаем транзакцию
            conn.commit()
            
        except Exception as e:
            # Откатываем транзакцию при ошибке
            conn.rollback()
            logger.error(f"Критическая ошибка при обработке {signature}: {e}")
            failed += 1
    
    return {
        'found': len(records),
        'successful': successful,
        'failed': failed
    }

def main():
    """Основная функция CLI"""
    parser = argparse.ArgumentParser(description='Повторная обработка сбойных транзакций из DLQ')
    parser.add_argument('--limit', type=int, required=True, help='Максимальное количество записей для обработки')
    parser.add_argument('--reason', type=str, help='Фильтр по причине сбоя')
    parser.add_argument('--dry-run', action='store_true', help='Предварительный просмотр без выполнения')
    parser.add_argument('--force', action='store_true', help='Обрабатывать записи со статусом FAILED_PERMANENTLY')
    
    args = parser.parse_args()
    
    # Настраиваем логирование
    logger = setup_logging()
    
    logger.info("="*80)
    logger.info("ЗАПУСК СКРИПТА ПОВТОРНОЙ ОБРАБОТКИ DLQ")
    logger.info("="*80)
    logger.info(f"Параметры: limit={args.limit}, reason={args.reason}, dry_run={args.dry_run}, force={args.force}")
    
    try:
        # Подключаемся к БД
        conn = get_sqlite_connection()
        create_tables(conn)
        
        # Выполняем повторную обработку
        stats = retry_failed_transactions(
            conn=conn,
            limit=args.limit,
            reason_filter=args.reason,
            dry_run=args.dry_run,
            force=args.force,
            logger=logger
        )
        
        # Выводим статистику
        logger.info("="*80)
        logger.info("СТАТИСТИКА ОБРАБОТКИ:")
        logger.info(f"  Найдено для обработки: {stats['found']}")
        logger.info(f"  Успешно переобработано и удалено из DLQ: {stats['successful']}")
        logger.info(f"  Не удалось переобработать (увеличен retry_count): {stats['failed']}")
        logger.info("="*80)
        
        conn.close()
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 