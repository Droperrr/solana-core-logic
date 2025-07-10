#!/usr/bin/env python3
"""
Скрипт для просмотра деталей отдельной транзакции в базе данных.
Полезен для отладки проблем с обогащением данных (enrichment).

Примеры использования:
1. Базовый просмотр транзакции по сигнатуре:
   python scripts/show_transaction.py --signature 5KKsT7...

2. Подробный просмотр с сырыми данными:
   python scripts/show_transaction.py --signature 5KKsT7... --show-raw
"""
import os
import sys
import json
import sqlite3
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/debugging.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("transaction_inspector")

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

def get_transaction(conn, signature: str) -> Optional[Dict[str, Any]]:
    """
    Получает данные транзакции по сигнатуре.
    
    Args:
        conn: Соединение с базой данных SQLite.
        signature: Сигнатура транзакции.
        
    Returns:
        Dict[str, Any] or None: Данные транзакции или None, если не найдена.
    """
    cursor = conn.cursor()
    cursor.execute("""
    SELECT * FROM transactions WHERE signature = ?
    """, (signature,))
    
    return cursor.fetchone()

def format_transaction_details(tx, show_raw=False, max_width=100):
    """
    Форматирует данные транзакции для удобного просмотра.
    
    Args:
        tx: Данные транзакции.
        show_raw: Показывать ли сырые данные.
        max_width: Максимальная ширина вывода для форматирования JSON.
        
    Returns:
        str: Форматированный вывод данных транзакции.
    """
    if not tx:
        return "Транзакция не найдена"
    
    output = []
    output.append(f"{'='*max_width}")
    output.append(f"ТРАНЗАКЦИЯ: {tx['signature']}")
    output.append(f"{'='*max_width}")
    
    output.append(f"\nОСНОВНЫЕ ДАННЫЕ:")
    output.append(f"ID: {tx.get('id')}")
    output.append(f"Слот: {tx.get('slot')}")
    output.append(f"Время блока: {tx.get('block_time')} " + 
                 (f"({datetime.fromtimestamp(tx['block_time']).strftime('%Y-%m-%d %H:%M:%S')})" if tx.get('block_time') else ""))
    output.append(f"Fee Payer: {tx.get('fee_payer')}")
    output.append(f"Тип транзакции: {tx.get('transaction_type')}")
    output.append(f"Источник запроса: {tx.get('source_query_type')} / {tx.get('source_query_address')}")
    
    output.append(f"\nДАННЫЕ ОБОГАЩЕНИЯ:")
    output.append(f"Версия парсера: {tx.get('parser_version', 'Не указана')}")
    
    if tx.get('enriched_data'):
        try:
            enriched_data = json.loads(tx['enriched_data']) if isinstance(tx['enriched_data'], str) else tx['enriched_data']
            if isinstance(enriched_data, list):
                output.append(f"Найдено событий: {len(enriched_data)}")
                
                # Выводим краткую информацию о каждом событии
                for i, event in enumerate(enriched_data):
                    output.append(f"\n  Событие #{i+1}:")
                    output.append(f"  Тип: {event.get('type', 'unknown')}")
                    
                    # Если есть enrichment_data, выводим его
                    if 'enrichment_data' in event:
                        output.append(f"  Данные обогащения:")
                        for enricher, data in event['enrichment_data'].items():
                            output.append(f"    - {enricher}: {json.dumps(data, ensure_ascii=False)[:50]}...")
                            
                if len(enriched_data) > 0:
                    # Выводим полный JSON первого события для детального анализа
                    output.append(f"\nСТРУКТУРА ПЕРВОГО СОБЫТИЯ:")
                    formatted_json = json.dumps(enriched_data[0], indent=2, ensure_ascii=False)
                    # Разбиваем длинные строки для удобства чтения
                    for line in formatted_json.split("\n"):
                        if len(line) > max_width:
                            line = line[:max_width-3] + "..."
                        output.append(line)
            else:
                output.append("Enriched data не является списком событий.")
                formatted_json = json.dumps(enriched_data, indent=2, ensure_ascii=False)
                for line in formatted_json.split("\n")[:20]:
                    if len(line) > max_width:
                        line = line[:max_width-3] + "..."
                    output.append(line)
                if len(formatted_json.split("\n")) > 20:
                    output.append("...")
        except Exception as e:
            output.append(f"Ошибка при разборе enriched_data: {str(e)}")
    else:
        output.append("Данные обогащения отсутствуют.")
    
    # Если запрошены сырые данные и они есть
    if show_raw and tx.get('raw_json'):
        output.append(f"\n{'='*20} СЫРЫЕ ДАННЫЕ {'='*20}")
        try:
            raw_data = json.loads(tx['raw_json']) if isinstance(tx['raw_json'], str) else tx['raw_json']
            formatted_json = json.dumps(raw_data, indent=2, ensure_ascii=False)
            for line in formatted_json.split("\n")[:50]:  # Ограничиваем до 50 строк для читаемости
                if len(line) > max_width:
                    line = line[:max_width-3] + "..."
                output.append(line)
            if len(formatted_json.split("\n")) > 50:
                output.append("...")
        except Exception as e:
            output.append(f"Ошибка при разборе raw_json: {str(e)}")
    
    return "\n".join(output)

def main():
    parser = argparse.ArgumentParser(description="Просмотр деталей транзакции из базы данных")
    parser.add_argument("--signature", required=True, help="Сигнатура транзакции для просмотра")
    parser.add_argument("--db-path", help="Путь к файлу базы данных SQLite")
    parser.add_argument("--show-raw", action="store_true", help="Показать также сырые данные транзакции")
    parser.add_argument("--output", help="Путь для сохранения вывода в файл")
    
    args = parser.parse_args()
    
    conn = get_sqlite_connection(args.db_path)
    transaction = get_transaction(conn, args.signature)
    conn.close()
    
    if not transaction:
        print(f"Транзакция с сигнатурой {args.signature} не найдена в базе данных.")
        return
    
    from datetime import datetime  # Импортируем здесь для использования в format_transaction_details
    
    output = format_transaction_details(transaction, args.show_raw)
    
    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(output)
        print(f"Результат сохранен в файл: {args.output}")
    else:
        print(output)

if __name__ == "__main__":
    main() 