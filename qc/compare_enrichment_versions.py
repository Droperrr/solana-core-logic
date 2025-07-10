#!/usr/bin/env python3
"""
Скрипт для сравнения обогащенных данных транзакции до и после переобработки.
Позволяет проверить влияние изменений в логике парсинга/обогащения.

Примеры использования:
1. Сравнение конкретной транзакции:
   python qc/compare_enrichment_versions.py --signature 2LgJdAtoBJU63EVLVyoGfJM5FAcsPQpYNi3N8ZgvBozbsTEWG7X9pFprjhoHG6b3zSNtxtP2EMQQ1NgFfZSKPd8t

2. Сравнение с сохранением отчета:
   python qc/compare_enrichment_versions.py --signature <signature> --output-file comparison_report.json

3. Подробный анализ с детализацией:
   python qc/compare_enrichment_versions.py --signature <signature> --detailed
"""

import os
import sys
import json
import sqlite3
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
import difflib

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from decoder.decoder import TransactionDecoder
from decoder.enrichers.net_token_change_enricher import NetTokenChangeEnricher
from decoder.enrichers.compute_unit_enricher import ComputeUnitEnricher
from decoder.enrichers.sequence_enricher import SequenceEnricher

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger("enrichment_comparison")

def get_sqlite_connection(db_path: str = None) -> sqlite3.Connection:
    """Создает соединение с базой данных SQLite."""
    if db_path is None:
        db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def get_transaction_data(conn: sqlite3.Connection, signature: str) -> Optional[Dict[str, Any]]:
    """
    Получает данные транзакции из базы данных.
    
    Args:
        conn: Соединение с базой данных
        signature: Сигнатура транзакции
        
    Returns:
        Данные транзакции или None если не найдена
    """
    cursor = conn.cursor()
    cursor.execute("""
        SELECT signature, raw_json, enriched_data, parser_version, 
               block_time, slot, fee_payer, transaction_type,
               created_at, updated_at
        FROM transactions 
        WHERE signature = ?
    """, (signature,))
    
    result = cursor.fetchone()
    if result:
        return dict(result)
    return None

def generate_new_enrichment(raw_json_str: str) -> Tuple[List[Dict], str]:
    """
    Генерирует новые обогащенные данные используя текущую логику декодера.
    
    Args:
        raw_json_str: Строка с raw_json данными
        
    Returns:
        Кортеж (новые_обогащенные_данные, сообщение_об_ошибке)
    """
    try:
        # Создаем декодер с актуальными обогатителями
        decoder = TransactionDecoder(enrichers=[
            NetTokenChangeEnricher(),
            ComputeUnitEnricher(),
            SequenceEnricher()
        ])
        
        raw_json = json.loads(raw_json_str)
        enriched_events = decoder.decode_transaction(raw_json)
        
        # Преобразуем события в словари
        enriched_data = []
        for event in enriched_events:
            if hasattr(event, 'to_dict'):
                enriched_data.append(event.to_dict())
            else:
                enriched_data.append(event)
        
        return enriched_data, None
        
    except json.JSONDecodeError as e:
        return [], f"Ошибка декодирования JSON: {e}"
    except Exception as e:
        return [], f"Ошибка при обогащении: {e}"

def deep_compare_dicts(old_data: Any, new_data: Any, path: str = "") -> List[Dict[str, Any]]:
    """
    Глубоко сравнивает два объекта и возвращает список различий.
    
    Args:
        old_data: Старые данные
        new_data: Новые данные
        path: Текущий путь в структуре данных
        
    Returns:
        Список различий
    """
    differences = []
    
    # Сравниваем типы
    if type(old_data) != type(new_data):
        differences.append({
            "type": "type_change",
            "path": path,
            "old_type": str(type(old_data).__name__),
            "new_type": str(type(new_data).__name__),
            "old_value": old_data,
            "new_value": new_data
        })
        return differences
    
    if isinstance(old_data, dict) and isinstance(new_data, dict):
        # Сравниваем словари
        all_keys = set(old_data.keys()) | set(new_data.keys())
        
        for key in all_keys:
            new_path = f"{path}.{key}" if path else key
            
            if key not in old_data:
                differences.append({
                    "type": "added",
                    "path": new_path,
                    "value": new_data[key]
                })
            elif key not in new_data:
                differences.append({
                    "type": "removed",
                    "path": new_path,
                    "value": old_data[key]
                })
            else:
                differences.extend(deep_compare_dicts(old_data[key], new_data[key], new_path))
    
    elif isinstance(old_data, list) and isinstance(new_data, list):
        # Сравниваем списки
        max_len = max(len(old_data), len(new_data))
        
        for i in range(max_len):
            new_path = f"{path}[{i}]"
            
            if i >= len(old_data):
                differences.append({
                    "type": "added",
                    "path": new_path,
                    "value": new_data[i]
                })
            elif i >= len(new_data):
                differences.append({
                    "type": "removed",
                    "path": new_path,
                    "value": old_data[i]
                })
            else:
                differences.extend(deep_compare_dicts(old_data[i], new_data[i], new_path))
    
    else:
        # Сравниваем примитивные значения
        if old_data != new_data:
            differences.append({
                "type": "changed",
                "path": path,
                "old_value": old_data,
                "new_value": new_data
            })
    
    return differences

def analyze_enrichment_changes(old_enriched: List[Dict], new_enriched: List[Dict]) -> Dict[str, Any]:
    """
    Анализирует изменения в обогащенных данных.
    
    Args:
        old_enriched: Старые обогащенные данные
        new_enriched: Новые обогащенные данные
        
    Returns:
        Анализ изменений
    """
    analysis = {
        "summary": {
            "old_events_count": len(old_enriched),
            "new_events_count": len(new_enriched),
            "events_count_changed": len(old_enriched) != len(new_enriched)
        },
        "differences": [],
        "event_types_comparison": {
            "old_types": {},
            "new_types": {},
            "added_types": set(),
            "removed_types": set()
        }
    }
    
    # Анализируем типы событий
    for event in old_enriched:
        event_type = event.get('event_type', 'unknown')
        analysis["event_types_comparison"]["old_types"][event_type] = \
            analysis["event_types_comparison"]["old_types"].get(event_type, 0) + 1
    
    for event in new_enriched:
        event_type = event.get('event_type', 'unknown')
        analysis["event_types_comparison"]["new_types"][event_type] = \
            analysis["event_types_comparison"]["new_types"].get(event_type, 0) + 1
    
    old_types = set(analysis["event_types_comparison"]["old_types"].keys())
    new_types = set(analysis["event_types_comparison"]["new_types"].keys())
    
    analysis["event_types_comparison"]["added_types"] = list(new_types - old_types)
    analysis["event_types_comparison"]["removed_types"] = list(old_types - new_types)
    
    # Детальное сравнение данных
    analysis["differences"] = deep_compare_dicts(old_enriched, new_enriched, "enriched_data")
    
    return analysis

def format_comparison_report(
    transaction_data: Dict[str, Any],
    old_enriched: List[Dict],
    new_enriched: List[Dict],
    analysis: Dict[str, Any],
    detailed: bool = False
) -> str:
    """
    Форматирует отчет сравнения в читаемый вид.
    
    Args:
        transaction_data: Данные транзакции
        old_enriched: Старые обогащенные данные
        new_enriched: Новые обогащенные данные
        analysis: Анализ изменений
        detailed: Включать детальную информацию
        
    Returns:
        Форматированный отчет
    """
    report = []
    report.append("=" * 80)
    report.append("ОТЧЕТ СРАВНЕНИЯ ОБОГАЩЕННЫХ ДАННЫХ")
    report.append("=" * 80)
    report.append(f"Сигнатура транзакции: {transaction_data['signature']}")
    report.append(f"Текущая версия парсера: {transaction_data['parser_version']}")
    report.append(f"Время создания: {transaction_data['created_at']}")
    report.append(f"Время обновления: {transaction_data['updated_at']}")
    report.append("")
    
    # Основная статистика
    report.append("ОСНОВНАЯ СТАТИСТИКА:")
    report.append(f"  Старое количество событий: {analysis['summary']['old_events_count']}")
    report.append(f"  Новое количество событий: {analysis['summary']['new_events_count']}")
    report.append(f"  Изменилось количество событий: {'Да' if analysis['summary']['events_count_changed'] else 'Нет'}")
    report.append("")
    
    # Типы событий
    report.append("ТИПЫ СОБЫТИЙ:")
    report.append("  Старые типы:")
    for event_type, count in analysis["event_types_comparison"]["old_types"].items():
        report.append(f"    {event_type}: {count}")
    
    report.append("  Новые типы:")
    for event_type, count in analysis["event_types_comparison"]["new_types"].items():
        report.append(f"    {event_type}: {count}")
    
    if analysis["event_types_comparison"]["added_types"]:
        report.append(f"  Добавленные типы: {', '.join(analysis['event_types_comparison']['added_types'])}")
    
    if analysis["event_types_comparison"]["removed_types"]:
        report.append(f"  Удаленные типы: {', '.join(analysis['event_types_comparison']['removed_types'])}")
    
    report.append("")
    
    # Различия
    differences = analysis["differences"]
    if differences:
        report.append(f"НАЙДЕНО {len(differences)} РАЗЛИЧИЙ:")
        
        for i, diff in enumerate(differences[:20]):  # Ограничиваем вывод первыми 20 различиями
            report.append(f"  {i+1}. {diff['type'].upper()} в {diff['path']}")
            
            if diff['type'] == 'changed':
                report.append(f"     Старое значение: {diff['old_value']}")
                report.append(f"     Новое значение: {diff['new_value']}")
            elif diff['type'] == 'added':
                report.append(f"     Добавленное значение: {diff['value']}")
            elif diff['type'] == 'removed':
                report.append(f"     Удаленное значение: {diff['value']}")
            elif diff['type'] == 'type_change':
                report.append(f"     Старый тип: {diff['old_type']} -> Новый тип: {diff['new_type']}")
        
        if len(differences) > 20:
            report.append(f"  ... и еще {len(differences) - 20} различий")
    else:
        report.append("РАЗЛИЧИЙ НЕ НАЙДЕНО - данные идентичны")
    
    report.append("")
    
    if detailed:
        report.append("ДЕТАЛЬНЫЕ ДАННЫЕ:")
        report.append("")
        report.append("Старые обогащенные данные:")
        report.append(json.dumps(old_enriched, indent=2, ensure_ascii=False))
        report.append("")
        report.append("Новые обогащенные данные:")
        report.append(json.dumps(new_enriched, indent=2, ensure_ascii=False))
    
    report.append("=" * 80)
    
    return "\n".join(report)

def main():
    """Основная функция скрипта."""
    parser = argparse.ArgumentParser(
        description="Сравнение обогащенных данных транзакции до и после переобработки",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--signature",
        type=str,
        required=True,
        help="Сигнатура транзакции для сравнения"
    )
    
    parser.add_argument(
        "--db-path",
        type=str,
        help="Путь к файлу базы данных SQLite"
    )
    
    parser.add_argument(
        "--output-file",
        type=str,
        help="Файл для сохранения отчета сравнения"
    )
    
    parser.add_argument(
        "--detailed",
        action="store_true",
        help="Включить детальную информацию в отчет"
    )
    
    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Вывести результат в формате JSON"
    )
    
    args = parser.parse_args()
    
    try:
        # Подключаемся к базе данных
        conn = get_sqlite_connection(args.db_path)
        
        # Получаем данные транзакции
        transaction_data = get_transaction_data(conn, args.signature)
        if not transaction_data:
            logger.error(f"Транзакция с сигнатурой {args.signature} не найдена в базе данных")
            return 1
        
        logger.info(f"Найдена транзакция: {args.signature}")
        logger.info(f"Текущая версия парсера: {transaction_data['parser_version']}")
        
        # Получаем старые обогащенные данные
        old_enriched = json.loads(transaction_data['enriched_data']) if transaction_data['enriched_data'] else []
        
        # Генерируем новые обогащенные данные
        logger.info("Генерируем новые обогащенные данные...")
        new_enriched, error = generate_new_enrichment(transaction_data['raw_json'])
        
        if error:
            logger.error(f"Ошибка при генерации новых обогащенных данных: {error}")
            return 1
        
        # Анализируем различия
        logger.info("Анализируем различия...")
        analysis = analyze_enrichment_changes(old_enriched, new_enriched)
        
        # Формируем результат
        result = {
            "signature": args.signature,
            "comparison_timestamp": datetime.now().isoformat(),
            "transaction_info": {
                "parser_version": transaction_data['parser_version'],
                "block_time": transaction_data['block_time'],
                "slot": transaction_data['slot'],
                "fee_payer": transaction_data['fee_payer']
            },
            "analysis": analysis,
            "old_enriched_data": old_enriched if args.detailed else None,
            "new_enriched_data": new_enriched if args.detailed else None
        }
        
        if args.json_output:
            # Выводим JSON
            print(json.dumps(result, indent=2, ensure_ascii=False))
        else:
            # Выводим форматированный отчет
            report = format_comparison_report(
                transaction_data, old_enriched, new_enriched, analysis, args.detailed
            )
            print(report)
        
        # Сохраняем в файл если указан
        if args.output_file:
            with open(args.output_file, 'w', encoding='utf-8') as f:
                if args.json_output:
                    json.dump(result, f, indent=2, ensure_ascii=False)
                else:
                    f.write(report)
            logger.info(f"Отчет сохранен в файл: {args.output_file}")
        
        return 0
        
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code) 