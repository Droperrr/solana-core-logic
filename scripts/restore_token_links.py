#!/usr/bin/env python3
"""
Скрипт восстановления корректных source_query_address для транзакций.

Анализирует raw_json каждой транзакции с source_query_address='batch' 
и восстанавливает правильную привязку к токенам из tokens.txt.

Автор: BANT System
Дата: 2025-06-23
"""

import sqlite3
import json
import logging
from typing import Set, List, Dict
import sys
import os

# Настройка логирования (совместимо с Windows)
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('logs/restore_token_links.log', encoding='utf-8')
    ]
)
logger = logging.getLogger("restore_links")

def load_target_tokens(tokens_file: str = 'tokens.txt') -> Set[str]:
    """Загружает целевые токены из файла tokens.txt"""
    try:
        with open(tokens_file, 'r', encoding='utf-8') as f:
            tokens = {line.strip() for line in f if line.strip() and not line.strip().startswith('#')}
        logger.info(f"Загружено {len(tokens)} целевых токенов из {tokens_file}")
        return tokens
    except Exception as e:
        logger.error(f"Ошибка загрузки файла токенов {tokens_file}: {e}")
        return set()

def analyze_transaction_tokens(raw_json: dict) -> Set[str]:
    """
    Извлекает все адреса токенов (mint) из полей pre/postTokenBalances транзакции.
    
    Args:
        raw_json: Данные транзакции из RPC
        
    Returns:
        Множество адресов токенов, найденных в транзакции
    """
    tokens = set()
    if not isinstance(raw_json, dict):
        return tokens
        
    meta = raw_json.get('meta', {})
    if not isinstance(meta, dict):
        return tokens
        
    # Проверяем preTokenBalances и postTokenBalances
    for balance_type in ['preTokenBalances', 'postTokenBalances']:
        balances = meta.get(balance_type, [])
        if not isinstance(balances, list):
            continue
            
        for balance in balances:
            if isinstance(balance, dict) and 'mint' in balance:
                mint = balance['mint']
                if mint and isinstance(mint, str):
                    tokens.add(mint)
    
    return tokens

def restore_source_query_addresses(dry_run: bool = False) -> Dict[str, int]:
    """
    Восстанавливает правильные source_query_address в таблице transactions.
    
    Args:
        dry_run: Если True, только показывает что будет сделано без изменений БД
        
    Returns:
        Словарь со статистикой восстановления
    """
    logger.info("НАЧАЛО: Восстановление привязки транзакций к токенам...")
    
    # Загружаем целевые токены из tokens.txt
    target_tokens = load_target_tokens()
    if not target_tokens:
        logger.error("Не удалось загрузить целевые токены. Остановка.")
        return {}
    
    logger.info(f"Целевые токены: {sorted(target_tokens)}")
    
    # Подключение к БД
    db_path = 'db/solana_db.sqlite'
    if not os.path.exists(db_path):
        logger.error(f"База данных не найдена: {db_path}")
        return {}
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    try:
        # Шаг 1: Получение всех транзакций с source_query_address = 'batch'
        logger.info("ШАГ 1: Получение транзакций для исправления...")
        cursor.execute("""
            SELECT signature, raw_json, source_query_address, source_query_type
            FROM transactions 
            WHERE source_query_address = 'batch'
            ORDER BY block_time DESC
        """)
        transactions_to_fix = cursor.fetchall()
        logger.info(f"Найдено {len(transactions_to_fix)} транзакций с некорректной привязкой.")
        
        if not transactions_to_fix:
            logger.info("Нет транзакций для исправления.")
            return {'total_processed': 0}
        
        # Шаг 2: Анализ транзакций и подготовка обновлений
        logger.info("ШАГ 2: Анализ транзакций и определение правильных токенов...")
        
        updates_to_apply = []
        stats = {
            'total_processed': len(transactions_to_fix),
            'single_token_match': 0,
            'multiple_token_match': 0,
            'no_token_match': 0,
            'json_errors': 0
        }
        
        token_distribution = {}
        
        for i, tx in enumerate(transactions_to_fix, 1):
            if i % 500 == 0:
                logger.info(f"Обработано {i}/{len(transactions_to_fix)} транзакций...")
                
            try:
                raw_json = json.loads(tx['raw_json'])
                tokens_in_tx = analyze_transaction_tokens(raw_json)
                
                # Находим пересечение с нашими целевыми токенами
                matching_tokens = tokens_in_tx.intersection(target_tokens)
                
                if len(matching_tokens) == 1:
                    # Идеальный случай: транзакция относится к одному из наших токенов
                    correct_token = matching_tokens.pop()
                    updates_to_apply.append((correct_token, tx['source_query_type'], tx['signature']))
                    stats['single_token_match'] += 1
                    
                    # Ведем статистику по токенам
                    token_distribution[correct_token] = token_distribution.get(correct_token, 0) + 1
                    
                elif len(matching_tokens) > 1:
                    # Транзакция затрагивает несколько наших токенов
                    # Выбираем первый в алфавитном порядке для консистентности
                    correct_token = sorted(matching_tokens)[0]
                    updates_to_apply.append((correct_token, tx['source_query_type'], tx['signature']))
                    stats['multiple_token_match'] += 1
                    
                    token_distribution[correct_token] = token_distribution.get(correct_token, 0) + 1
                    
                    logger.debug(f"Транзакция {tx['signature'][:10]}... затрагивает несколько целевых токенов: {matching_tokens}, выбран: {correct_token}")
                    
                else:
                    # Транзакция не затрагивает наши целевые токены
                    stats['no_token_match'] += 1
                    logger.debug(f"Транзакция {tx['signature'][:10]}... не затрагивает целевые токены. Найденные токены: {tokens_in_tx}")
                    
            except json.JSONDecodeError as e:
                stats['json_errors'] += 1
                logger.error(f"Ошибка декодирования JSON для транзакции {tx['signature'][:10]}...: {e}")
            except Exception as e:
                stats['json_errors'] += 1
                logger.error(f"Ошибка анализа транзакции {tx['signature'][:10]}...: {e}")
        
        # Шаг 3: Применение обновлений
        logger.info(f"СТАТИСТИКА анализа:")
        logger.info(f"  * Точное совпадение (1 токен): {stats['single_token_match']}")
        logger.info(f"  * Множественное совпадение (>1 токена): {stats['multiple_token_match']}")
        logger.info(f"  * Нет совпадений с целевыми токенами: {stats['no_token_match']}")
        logger.info(f"  * Ошибки JSON: {stats['json_errors']}")
        
        if token_distribution:
            logger.info(f"РАСПРЕДЕЛЕНИЕ по токенам:")
            for token, count in sorted(token_distribution.items()):
                logger.info(f"  * {token}: {count} транзакций")
        
        if updates_to_apply:
            logger.info(f"ШАГ 3: Подготовлено {len(updates_to_apply)} обновлений.")
            
            if dry_run:
                logger.info("РЕЖИМ DRY RUN: Изменения НЕ будут применены к базе данных.")
                logger.info("Примеры обновлений:")
                for i, (token, query_type, signature) in enumerate(updates_to_apply[:5]):
                    logger.info(f"  {i+1}. {signature[:16]}... -> {token}")
                if len(updates_to_apply) > 5:
                    logger.info(f"  ... и еще {len(updates_to_apply) - 5} обновлений")
            else:
                logger.info("ПРИМЕНЕНИЕ обновлений к базе данных...")
                cursor.executemany(
                    "UPDATE transactions SET source_query_address = ?, source_query_type = ? WHERE signature = ?", 
                    updates_to_apply
                )
                conn.commit()
                logger.info(f"УСПЕШНО обновлено {cursor.rowcount} записей.")
                
                # Проверяем результат
                cursor.execute("""
                    SELECT source_query_address, COUNT(*) as count
                    FROM transactions 
                    GROUP BY source_query_address
                    ORDER BY count DESC
                """)
                final_distribution = cursor.fetchall()
                
                logger.info("ФИНАЛЬНОЕ распределение транзакций по source_query_address:")
                for row in final_distribution:
                    logger.info(f"  * {row['source_query_address']}: {row['count']} транзакций")
        else:
            logger.info("ОБНОВЛЕНИЯ не требуются.")
            
        stats['updated_count'] = len(updates_to_apply)
        return stats
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        conn.rollback()
        raise
    finally:
        conn.close()
        logger.info("ВОССТАНОВЛЕНИЕ завершено.")

def main():
    """Главная функция скрипта"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Восстановление корректных source_query_address")
    parser.add_argument('--dry-run', action='store_true',
                      help='Режим тестирования без изменений БД')
    
    args = parser.parse_args()
    
    try:
        stats = restore_source_query_addresses(dry_run=args.dry_run)
        
        if args.dry_run:
            logger.info("DRY RUN завершен. Для применения изменений запустите без флага --dry-run")
        else:
            logger.info("ВОССТАНОВЛЕНИЕ успешно завершено!")
            
        logger.info(f"ИТОГОВАЯ статистика: {stats}")
        
    except Exception as e:
        logger.error(f"Ошибка выполнения скрипта: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 