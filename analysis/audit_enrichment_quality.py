#!/usr/bin/env python3
"""
Скрипт для аудита качества обогащенных (enriched) данных транзакций.
Проверяет наличие и полноту данных после применения обогатителей.

Примеры использования:
1. Базовая проверка:
   python analysis/audit_enrichment_quality.py

2. Проверка с дополнительными отладочными данными:
   python analysis/audit_enrichment_quality.py --verbose
"""
import os
import sys
import json
import sqlite3
import argparse
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from collections import Counter

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/audit.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("enrichment_audit")

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
        
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    return conn

def get_transaction_sample(conn, limit=100):
    """
    Получает выборку транзакций из базы данных для аудита.
    
    Args:
        conn: Соединение с базой данных SQLite.
        limit: Максимальное количество транзакций для выборки.
        
    Returns:
        List[Dict]: Список словарей с данными транзакций.
    """
    cursor = conn.cursor()
    cursor.execute("""
    SELECT signature, block_time, slot, raw_json, enriched_data, parser_version
    FROM transactions
    ORDER BY id DESC
    LIMIT ?
    """, (limit,))
    
    return cursor.fetchall()

def check_enrichment_quality(transactions, verbose=False):
    """
    Анализирует качество обогащения (enrichment) транзакций.
    
    Args:
        transactions: Список транзакций для анализа.
        verbose: Флаг вывода подробной информации.
        
    Returns:
        Dict: Отчет о результатах аудита.
    """
    total = len(transactions)
    if total == 0:
        logger.warning("Нет данных для аудита! База данных пуста?")
        return {"status": "error", "message": "Нет данных для аудита"}
    
    enrichment_stats = {
        "total": total,
        "with_enriched_data": 0,
        "empty_enriched_data": 0,
        "enrichers_found": set(),
        "field_coverage": Counter(),
        "event_types": Counter(),
        "issues": []
    }
    
    for tx in transactions:
        # Проверяем наличие обогащенных данных
        try:
            if tx.get('enriched_data'):
                enriched_data = json.loads(tx['enriched_data']) if isinstance(tx['enriched_data'], str) else tx['enriched_data']
                
                if enriched_data:
                    enrichment_stats["with_enriched_data"] += 1
                    
                    # Проверяем все ключи в enrichment_data
                    if isinstance(enriched_data, list):
                        for event in enriched_data:
                            # Подсчитываем типы событий
                            event_type = event.get('type', 'unknown')
                            enrichment_stats["event_types"][event_type] += 1
                            
                            # Проверяем наличие enrichment_data в каждом событии
                            if 'enrichment_data' in event:
                                # Считаем встречаемость каждого обогатителя
                                for enricher_key in event['enrichment_data'].keys():
                                    enrichment_stats["enrichers_found"].add(enricher_key)
                                    enrichment_stats["field_coverage"][enricher_key] += 1
                    else:
                        # Если не список, предполагаем, что это напрямую enrichment_data
                        for enricher_key in enriched_data.keys():
                            enrichment_stats["enrichers_found"].add(enricher_key)
                            enrichment_stats["field_coverage"][enricher_key] += 1
                else:
                    enrichment_stats["empty_enriched_data"] += 1
                    enrichment_stats["issues"].append({
                        "signature": tx['signature'],
                        "issue": "empty_enriched_data",
                        "details": "Поле enriched_data существует, но пусто"
                    })
            else:
                enrichment_stats["empty_enriched_data"] += 1
                enrichment_stats["issues"].append({
                    "signature": tx['signature'],
                    "issue": "missing_enriched_data",
                    "details": "Поле enriched_data отсутствует"
                })
                
            # Если включен verbose, показываем детали первых нескольких транзакций
            if verbose and len(enrichment_stats["issues"]) <= 5:
                print(f"\nТранзакция: {tx['signature']}")
                print(f"- Parser Version: {tx.get('parser_version', 'Not set')}")
                if tx.get('enriched_data'):
                    enriched = json.loads(tx['enriched_data']) if isinstance(tx['enriched_data'], str) else tx['enriched_data']
                    print(f"- Enriched Data: {json.dumps(enriched, indent=2)[:500]}...")
                else:
                    print("- Enriched Data: None")
                    
        except Exception as e:
            enrichment_stats["issues"].append({
                "signature": tx['signature'],
                "issue": "exception",
                "details": str(e)
            })
            
    # Конвертируем set в список для сериализации в JSON
    enrichment_stats["enrichers_found"] = list(enrichment_stats["enrichers_found"])
    
    # Процент успешно обогащенных транзакций
    if total > 0:
        enrichment_stats["success_rate"] = enrichment_stats["with_enriched_data"] / total
    else:
        enrichment_stats["success_rate"] = 0
        
    return enrichment_stats

def print_audit_report(report):
    """
    Выводит отчет об аудите в читаемом формате.
    
    Args:
        report: Отчет, полученный из check_enrichment_quality.
    """
    print("\n" + "="*50)
    print("ОТЧЕТ ОБ АУДИТЕ КАЧЕСТВА ENRICHMENT")
    print("="*50)
    
    print(f"\nПроанализировано транзакций: {report['total']}")
    print(f"Транзакций с обогащенными данными: {report['with_enriched_data']} ({report['success_rate']*100:.1f}%)")
    print(f"Транзакций без обогащенных данных: {report['empty_enriched_data']}")
    
    print("\nОбнаруженные обогатители:")
    for enricher in sorted(report['enrichers_found']):
        print(f"- {enricher}")
    
    print("\nПокрытие полями:")
    for field, count in report['field_coverage'].most_common():
        print(f"- {field}: {count} ({count/report['total']*100:.1f}%)")
    
    print("\nТипы событий:")
    for event_type, count in report['event_types'].most_common():
        print(f"- {event_type}: {count}")
    
    if report['issues']:
        print(f"\nОбнаружено проблем: {len(report['issues'])}")
        print("Примеры проблем:")
        for issue in report['issues'][:5]:  # Показываем только первые 5 проблем
            print(f"- {issue['signature']}: {issue['issue']} ({issue['details']})")
        if len(report['issues']) > 5:
            print(f"... и еще {len(report['issues']) - 5} проблем (см. полный отчет)")
    else:
        print("\nПроблем не обнаружено!")
        
    print("\nРЕКОМЕНДАЦИИ:")
    if report['success_rate'] < 0.9:
        print("! Низкий уровень успешного обогащения. Проверьте конфигурацию обогатителей.")
    if not report['enrichers_found']:
        print("X Обогатители не обнаружены! Убедитесь, что обогатители правильно настроены.")
    elif len(report['enrichers_found']) < 3:
        print("! Обнаружены не все обогатители. Проверьте инициализацию обогатителей.")
    else:
        print("V Все обогатители работают корректно.")
        
    print("="*50)

def main():
    parser = argparse.ArgumentParser(description="Аудит качества enriched данных транзакций")
    parser.add_argument("--db-path", help="Путь к файлу базы данных SQLite")
    parser.add_argument("--limit", type=int, default=100, help="Максимальное количество транзакций для анализа")
    parser.add_argument("--output", help="Путь для сохранения JSON-отчета")
    parser.add_argument("--verbose", action="store_true", help="Подробный вывод с деталями транзакций")
    
    args = parser.parse_args()
    
    conn = get_sqlite_connection(args.db_path)
    transactions = get_transaction_sample(conn, args.limit)
    conn.close()
    
    logger.info(f"Начало аудита {len(transactions)} транзакций")
    audit_report = check_enrichment_quality(transactions, args.verbose)
    
    print_audit_report(audit_report)
    
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(audit_report, f, indent=2)
            print(f"\nОтчет сохранен в {args.output}")
    
    logger.info("Аудит завершен")

if __name__ == "__main__":
    main() 