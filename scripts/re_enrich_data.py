#!/usr/bin/env python3
"""
Скрипт переобработки данных (Re-Enrichment Pipeline)
Применяет последнюю версию декодера к существующим транзакциям в БД
"""

import argparse
import json
import sqlite3
import sys
import os
from pathlib import Path
from typing import List, Optional
import logging

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from decoder.decoder import TransactionDecoder
from db.db_writer import upsert_transaction_sqlite

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CURRENT_PARSER_VERSION = "2.1.0"

def get_transactions_for_reprocessing(
    conn: sqlite3.Connection,
    signatures: Optional[List[str]] = None,
    parser_version_from: Optional[str] = None,
    limit: Optional[int] = None
) -> List[dict]:
    """
    Получает транзакции для переобработки из базы данных
    
    Args:
        conn: Соединение с БД
        signatures: Список конкретных подписей для обработки
        parser_version_from: Переобработать транзакции с версией не выше указанной
        limit: Максимальное количество транзакций
    
    Returns:
        Список словарей с данными транзакций
    """
    cursor = conn.cursor()
    
    if signatures:
        # Обработка конкретных подписей
        placeholders = ','.join(['?' for _ in signatures])
        query = f"""
            SELECT signature, raw_json, parser_version, source_query_type, source_query_address
            FROM transactions 
            WHERE signature IN ({placeholders})
            ORDER BY block_time DESC
        """
        params = signatures
    else:
        # Обработка по версии парсера
        if parser_version_from:
            query = """
                SELECT signature, raw_json, parser_version, source_query_type, source_query_address
                FROM transactions 
                WHERE parser_version <= ?
                ORDER BY block_time DESC
            """
            params = [parser_version_from]
        else:
            query = """
                SELECT signature, raw_json, parser_version, source_query_type, source_query_address
                FROM transactions 
                ORDER BY block_time DESC
            """
            params = []
    
    if limit:
        query += f" LIMIT {limit}"
    
    cursor.execute(query, params)
    results = cursor.fetchall()
    
    transactions = []
    for row in results:
        signature, raw_json_str, current_version, source_query_type, source_query_address = row
        try:
            raw_json = json.loads(raw_json_str) if raw_json_str else {}
            transactions.append({
                'signature': signature,
                'raw_json': raw_json,
                'current_parser_version': current_version,
                'original_source_query_type': source_query_type,
                'original_source_query_address': source_query_address
            })
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка декодирования JSON для транзакции {signature}: {e}")
            continue
    
    return transactions

def reprocess_transaction(
    decoder: TransactionDecoder,
    conn: sqlite3.Connection,
    transaction_data: dict,
    dry_run: bool = False
) -> bool:
    """
    Переобрабатывает одну транзакцию
    
    Args:
        decoder: Экземпляр декодера
        conn: Соединение с БД  
        transaction_data: Данные транзакции
        dry_run: Режим тестирования без записи в БД
    
    Returns:
        True если успешно, False если ошибка
    """
    signature = transaction_data['signature']
    raw_json = transaction_data['raw_json']
    current_version = transaction_data['current_parser_version']
    
    try:
        # Декодируем транзакцию с использованием последней версии
        logger.info(f"Переобрабатываем {signature[:16]}... (текущая версия: {current_version})")
        
        # Применяем новую логику декодера
        decoded_result = decoder.decode_transaction(raw_json)
        
        if decoded_result is None:
            logger.warning(f"Декодер вернул None для транзакции {signature}")
            return False
        
        if dry_run:
            logger.info(f"[DRY RUN] Транзакция {signature[:16]}... переобработана успешно")
            return True
        
        # --- КЛЮЧЕВОЕ ИСПРАВЛЕНИЕ: Сохраняем оригинальные source_query_* ---
        original_source_type = transaction_data.get('original_source_query_type', 'reprocessing')
        original_source_address = transaction_data.get('original_source_query_address', 'batch')
        
        # Сохраняем с новой версией парсера, но оригинальными source_query значениями
        success = upsert_transaction_sqlite(
            conn=conn,
            signature=signature,
            block_time=raw_json.get('blockTime'),
            slot=raw_json.get('slot'),
            fee_payer=None,  # Извлечем из raw_json если необходимо
            transaction_type=None,  # Определим из декодированных данных
            raw_json=raw_json,
            enriched_data=decoded_result,
            source_query_type=original_source_type,
            source_query_address=original_source_address,
            parser_version=CURRENT_PARSER_VERSION
        )
        
        if success:
            logger.info(f"✅ Транзакция {signature[:16]}... обновлена до версии {CURRENT_PARSER_VERSION}")
        else:
            logger.error(f"❌ Ошибка обновления транзакции {signature[:16]}...")
        
        return success
        
    except Exception as e:
        logger.error(f"Ошибка переобработки транзакции {signature}: {e}")
        return False

def load_signatures_from_file(file_path: str) -> List[str]:
    """Загружает список подписей из файла"""
    signatures = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#'):
                    signatures.append(line)
        return signatures
    except Exception as e:
        logger.error(f"Ошибка чтения файла подписей {file_path}: {e}")
        return []

def main():
    parser = argparse.ArgumentParser(description="Переобработка транзакций с новой логикой декодера")
    
    # Опции для выбора данных
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true', 
                      help='Переобработать все транзакции')
    group.add_argument('--signatures-file', type=str,
                      help='Путь к файлу со списком подписей для переобработки')
    group.add_argument('--parser-version-from', type=str,
                      help='Переобработать транзакции с версией парсера не выше указанной')
    
    # Дополнительные опции
    parser.add_argument('--limit', type=int, default=None,
                       help='Максимальное количество транзакций для обработки')
    parser.add_argument('--dry-run', action='store_true',
                       help='Режим тестирования без записи в БД')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Размер пакета для коммитов в БД')
    parser.add_argument('--db-path', type=str, default='db/solana_db.sqlite',
                       help='Путь к базе данных')
    
    args = parser.parse_args()
    
    # Проверяем существование БД
    if not os.path.exists(args.db_path):
        logger.error(f"База данных не найдена: {args.db_path}")
        sys.exit(1)
    
    # Инициализируем декодер
    logger.info("Инициализация декодера...")
    from decoder.enrichers.net_token_change_enricher import NetTokenChangeEnricher
    from decoder.enrichers.compute_unit_enricher import ComputeUnitEnricher  
    from decoder.enrichers.sequence_enricher import SequenceEnricher
    
    decoder = TransactionDecoder(enrichers=[
        NetTokenChangeEnricher(),
        ComputeUnitEnricher(),
        SequenceEnricher()
    ])
    
    # Подключаемся к БД
    conn = sqlite3.connect(args.db_path)
    
    try:
        # Определяем список транзакций для обработки
        if args.signatures_file:
            signatures = load_signatures_from_file(args.signatures_file)
            if not signatures:
                logger.error("Не удалось загрузить подписи из файла")
                sys.exit(1)
            transactions = get_transactions_for_reprocessing(conn, signatures=signatures)
        elif args.parser_version_from:
            transactions = get_transactions_for_reprocessing(
                conn, parser_version_from=args.parser_version_from, limit=args.limit
            )
        else:  # --all
            transactions = get_transactions_for_reprocessing(conn, limit=args.limit)
        
        if not transactions:
            logger.info("Нет транзакций для переобработки")
            return
        
        logger.info(f"Найдено {len(transactions)} транзакций для переобработки")
        
        if args.dry_run:
            logger.info("🧪 РЕЖИМ ТЕСТИРОВАНИЯ - изменения в БД не будут сохранены")
        
        # Обрабатываем транзакции пакетами
        success_count = 0
        error_count = 0
        
        for i, transaction_data in enumerate(transactions, 1):
            success = reprocess_transaction(decoder, conn, transaction_data, args.dry_run)
            
            if success:
                success_count += 1
            else:
                error_count += 1
            
            # Коммитим каждый N записей для производительности
            if not args.dry_run and i % args.batch_size == 0:
                conn.commit()
                logger.info(f"Обработано {i}/{len(transactions)} транзакций")
        
        # Финальный коммит
        if not args.dry_run:
            conn.commit()
        
        # Отчет о результатах
        logger.info(f"""
🎉 ПЕРЕОБРАБОТКА ЗАВЕРШЕНА:
✅ Успешно: {success_count}
❌ Ошибок: {error_count}
📊 Общий процент успеха: {success_count/(success_count+error_count)*100:.1f}%
        """)
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        sys.exit(1)
    finally:
        conn.close()

if __name__ == "__main__":
    main() 