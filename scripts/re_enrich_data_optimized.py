#!/usr/bin/env python3
"""
Оптимизированный скрипт переобработки данных (Re-Enrichment Pipeline)
Решает проблемы:
1. Конкурентного доступа к БД
2. Медленной записи (пакетная обработка)
3. Блокировок SQLite
"""

import argparse
import json
import sqlite3
import sys
import os
import time
from pathlib import Path
from typing import List, Optional, Dict, Any
import logging
from contextlib import contextmanager

# Добавляем корневую директорию проекта в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from decoder.decoder import TransactionDecoder

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

CURRENT_PARSER_VERSION = "2.1.0"
DEFAULT_BATCH_SIZE = 1000  # Увеличен размер батча
MAX_RETRIES = 3
RETRY_DELAY = 0.5

@contextmanager
def get_optimized_db_connection(db_path: str):
    """
    Создает оптимизированное соединение с SQLite для массовых операций
    """
    conn = sqlite3.connect(db_path, timeout=30.0)
    
    # Оптимизации для массовых записей
    conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging
    conn.execute("PRAGMA synchronous=NORMAL")  # Быстрая синхронизация
    conn.execute("PRAGMA cache_size=10000")    # Увеличенный кэш
    conn.execute("PRAGMA temp_store=MEMORY")   # Временные таблицы в памяти
    conn.execute("PRAGMA mmap_size=268435456") # 256MB memory mapping
    
    try:
        yield conn
    finally:
        conn.close()

def get_transactions_for_reprocessing(
    conn: sqlite3.Connection,
    signatures: Optional[List[str]] = None,
    parser_version_from: Optional[str] = None,
    limit: Optional[int] = None
) -> List[dict]:
    """
    Получает транзакции для переобработки из базы данных
    """
    cursor = conn.cursor()
    
    if signatures:
        placeholders = ','.join(['?' for _ in signatures])
        query = f"""
            SELECT signature, raw_json, parser_version, source_query_type, source_query_address
            FROM transactions 
            WHERE signature IN ({placeholders})
            ORDER BY block_time DESC
        """
        params = signatures
    else:
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

def reprocess_batch_optimized(
    decoder: TransactionDecoder,
    conn: sqlite3.Connection,
    batch: List[dict],
    dry_run: bool = False
) -> Dict[str, int]:
    """
    Переобрабатывает пакет транзакций с оптимизацией
    
    Returns:
        Dict с ключами 'success', 'errors'
    """
    if dry_run:
        logger.info(f"[DRY RUN] Обработка пакета из {len(batch)} транзакций")
        return {'success': len(batch), 'errors': 0}
    
    cursor = conn.cursor()
    success_count = 0
    error_count = 0
    
    # Подготавливаем данные для массовой вставки
    updates = []
    
    for transaction_data in batch:
        signature = transaction_data['signature']
        raw_json = transaction_data['raw_json']
        current_version = transaction_data['current_parser_version']
        
        try:
            # Декодируем транзакцию
            decoded_result = decoder.decode_transaction(raw_json)
            
            if decoded_result is None:
                logger.warning(f"Декодер вернул None для транзакции {signature}")
                error_count += 1
                continue
            
            # Подготавливаем данные для обновления
            original_source_type = transaction_data.get('original_source_query_type', 'reprocessing')
            original_source_address = transaction_data.get('original_source_query_address', 'batch')
            
            enriched_data_str = json.dumps(decoded_result, ensure_ascii=False)
            
            updates.append((
                enriched_data_str,
                CURRENT_PARSER_VERSION,
                signature
            ))
            
            success_count += 1
            
        except Exception as e:
            logger.error(f"Ошибка переобработки транзакции {signature}: {e}")
            error_count += 1
    
    # Массовое обновление
    if updates:
        try:
            # Оптимизированный UPDATE запрос
            cursor.executemany("""
                UPDATE transactions 
                SET enriched_data = ?, 
                    parser_version = ?, 
                    updated_at = CURRENT_TIMESTAMP
                WHERE signature = ?
            """, updates)
            
            logger.info(f"Обновлено {len(updates)} транзакций в пакете")
            
        except Exception as e:
            logger.error(f"Ошибка массового обновления: {e}")
            error_count += len(updates)
            success_count = 0
    
    return {'success': success_count, 'errors': error_count}

def wait_for_db_unlock(db_path: str, max_wait: int = 30) -> bool:
    """
    Ожидает освобождения базы данных
    """
    for attempt in range(max_wait):
        try:
            with sqlite3.connect(db_path, timeout=1.0) as test_conn:
                test_conn.execute("SELECT 1").fetchone()
                return True
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e):
                logger.info(f"БД заблокирована, ожидание... ({attempt+1}/{max_wait})")
                time.sleep(1)
            else:
                raise
    return False

def main():
    parser = argparse.ArgumentParser(description="Оптимизированная переобработка транзакций")
    
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
    parser.add_argument('--batch-size', type=int, default=DEFAULT_BATCH_SIZE,
                       help=f'Размер пакета для массовых операций (по умолчанию: {DEFAULT_BATCH_SIZE})')
    parser.add_argument('--db-path', type=str, default='db/solana_db.sqlite',
                       help='Путь к базе данных')
    parser.add_argument('--wait-for-unlock', action='store_true',
                       help='Ожидать освобождения заблокированной БД')
    
    args = parser.parse_args()
    
    # Проверяем существование БД
    if not os.path.exists(args.db_path):
        logger.error(f"База данных не найдена: {args.db_path}")
        sys.exit(1)
    
    # Ожидаем освобождения БД если нужно
    if args.wait_for_unlock:
        if not wait_for_db_unlock(args.db_path):
            logger.error("Не удалось дождаться освобождения БД")
            sys.exit(1)
    
    # Инициализируем декодер
    logger.info("Инициализация оптимизированного декодера...")
    from decoder.enrichers.net_token_change_enricher import NetTokenChangeEnricher
    from decoder.enrichers.compute_unit_enricher import ComputeUnitEnricher  
    from decoder.enrichers.sequence_enricher import SequenceEnricher
    
    decoder = TransactionDecoder(enrichers=[
        NetTokenChangeEnricher(),
        ComputeUnitEnricher(),
        SequenceEnricher()
    ])
    
    # Используем оптимизированное соединение
    with get_optimized_db_connection(args.db_path) as conn:
        try:
            # Определяем список транзакций для обработки
            if args.signatures_file:
                with open(args.signatures_file, 'r') as f:
                    signatures = [line.strip() for line in f if line.strip() and not line.startswith('#')]
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
            logger.info(f"Размер пакета: {args.batch_size}")
            
            if args.dry_run:
                logger.info("🧪 РЕЖИМ ТЕСТИРОВАНИЯ - изменения в БД не будут сохранены")
            
            # Обрабатываем пакетами
            total_success = 0
            total_errors = 0
            
            for i in range(0, len(transactions), args.batch_size):
                batch = transactions[i:i + args.batch_size]
                batch_num = i // args.batch_size + 1
                total_batches = (len(transactions) + args.batch_size - 1) // args.batch_size
                
                logger.info(f"Обработка пакета {batch_num}/{total_batches} ({len(batch)} транзакций)")
                
                # Обрабатываем пакет
                results = reprocess_batch_optimized(decoder, conn, batch, args.dry_run)
                
                total_success += results['success']
                total_errors += results['errors']
                
                # Коммитим пакет (вместо каждой транзакции)
                if not args.dry_run:
                    conn.commit()
                
                # Прогресс
                processed = min(i + args.batch_size, len(transactions))
                logger.info(f"Прогресс: {processed}/{len(transactions)} ({processed/len(transactions)*100:.1f}%)")
            
            # Итоговый отчет
            logger.info(f"""
🎉 ОПТИМИЗИРОВАННАЯ ПЕРЕОБРАБОТКА ЗАВЕРШЕНА:
✅ Успешно: {total_success}
❌ Ошибок: {total_errors}
📊 Процент успеха: {total_success/(total_success+total_errors)*100:.1f}%
⚡ Оптимизации применены: WAL режим, пакетная обработка, увеличенный кэш
            """)
            
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            sys.exit(1)

if __name__ == "__main__":
    main() 