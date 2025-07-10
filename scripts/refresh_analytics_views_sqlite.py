#!/usr/bin/env python3
"""
Скрипт для создания и обновления аналитических витрин (ml_ready_events) в SQLite.
В SQLite нет материализованных представлений, поэтому мы создаем обычную таблицу и наполняем ее данными.
"""
import os
import sys
import time
import logging
import argparse
import json
import sqlite3
from pathlib import Path

# Настраиваем пути для импорта
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Создаем директорию для логов, если её нет
Path("logs").mkdir(exist_ok=True)

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/refresh_views_sqlite.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_views_sqlite")

def dict_factory(cursor, row):
    """Конвертирует строки результатов запроса в словари"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_sqlite_db_path():
    """
    Возвращает путь к файлу базы данных SQLite.
    По умолчанию использует файл db/solana_db.sqlite в директории проекта.
    """
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')

def get_db_connection():
    """
    Создает и возвращает соединение с базой данных SQLite.
    
    Returns:
        Connection: Соединение с базой данных SQLite.
    """
    db_path = get_sqlite_db_path()
    logger.info(f"Подключение к SQLite: {db_path}")
    
    # Создаем директорию для базы данных, если её нет
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    return conn

def create_ml_ready_events_table(conn):
    """
    Создает таблицу ml_ready_events, если она не существует.
    В SQLite нет материализованных представлений, поэтому мы используем обычную таблицу.
    
    Args:
        conn: Соединение с базой данных SQLite.
    """
    try:
        logger.info("Создание таблицы ml_ready_events")
        cursor = conn.cursor()
        
        # Сначала проверим, существует ли таблица, и удалим её, если необходимо
        cursor.execute("""
        DROP TABLE IF EXISTS ml_ready_events;
        """)
        
        # Создаем таблицу ml_ready_events
        cursor.execute("""
        CREATE TABLE ml_ready_events (
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
            -- ДОБАВЛЕННЫЕ ПОЛЯ для совместимости с data_provider --
            program_id TEXT,
            instruction_name TEXT,
            event_data_raw TEXT
        );
        """)
        
        # Создаем индексы для быстрого доступа
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_a ON ml_ready_events(token_a_mint);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_b ON ml_ready_events(token_b_mint);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_ready_events_event_type ON ml_ready_events(event_type);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_ready_events_wallet_tag ON ml_ready_events(wallet_tag);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_ready_events_block_time ON ml_ready_events(block_time);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ml_ready_events_signature ON ml_ready_events(signature);")
        
        conn.commit()
        logger.info("Таблица ml_ready_events успешно создана")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при создании таблицы ml_ready_events: {str(e)}", exc_info=True)
        raise

def extract_value_from_json(json_data, key_path, default=None):
    """
    Извлекает значение из JSON по указанному пути ключей.
    
    Args:
        json_data: JSON данные (словарь)
        key_path: Путь к ключу (например, 'token_a.mint')
        default: Значение по умолчанию
        
    Returns:
        Значение из JSON или default, если значение не найдено
    """
    if not json_data:
        return default
    
    # Разбиваем путь на составляющие
    keys = key_path.split('.')
    
    # Рекурсивно извлекаем значение
    result = json_data
    for key in keys:
        if isinstance(result, dict) and key in result:
            result = result[key]
        else:
            return default
    
    return result

def refresh_ml_ready_events(conn):
    """
    Обновляет таблицу ml_ready_events данными из таблицы transactions.
    
    Args:
        conn: Соединение с базой данных SQLite.
    """
    try:
        logger.info("Обновление таблицы ml_ready_events")
        start_time = time.time()
        cursor = conn.cursor()
        
        # Очищаем таблицу перед обновлением
        cursor.execute("DELETE FROM ml_ready_events;")
        
        # Получаем данные из таблицы transactions
        cursor.execute("SELECT signature, block_time, enriched_data FROM transactions WHERE enriched_data IS NOT NULL;")
        transactions = cursor.fetchall()
        
        # Подсчитываем количество обрабатываемых транзакций
        total_transactions = len(transactions)
        logger.info(f"Найдено {total_transactions} транзакций для обработки")
        
        # Счетчик для отслеживания прогресса
        processed_count = 0
        events_count = 0
        
        # Перебираем транзакции и извлекаем данные
        for tx in transactions:
            try:
                # Парсим enriched_data
                if isinstance(tx['enriched_data'], str):
                    enriched_data = json.loads(tx['enriched_data'])
                else:
                    enriched_data = tx['enriched_data']
                
                # Получаем базовую информацию
                signature = tx['signature']
                block_time = tx['block_time']
                
                # Наши enriched_data представляют собой список объектов с ключами 'type' и 'data'
                events = enriched_data if isinstance(enriched_data, list) else [enriched_data]
                
                # Обрабатываем каждое событие
                for event in events:
                    # Инициализация переменных по умолчанию
                    token_a_mint = None
                    token_b_mint = None
                    from_amount = None
                    to_amount = None
                    from_wallet = None
                    to_wallet = None
                    platform = 'unknown'
                    parser_version = ''
                    wallet_tags = []
                    
                    # Инициализация новых полей
                    program_id = None
                    instruction_name = None
                    event_data_raw = None
                    
                                        # Проверяем структуру события
                    if isinstance(event, dict) and 'type' in event:
                        event_type = event.get('type', 'unknown')
                        
                        # Проверяем, является ли это прямым объектом события (новый формат)
                        if event_type in ['SWAP', 'Unknown'] and 'data' not in event:
                            # Прямой объект события (новый формат CPI)
                            if event_type == 'SWAP':
                                # Извлекаем данные из SWAP события напрямую
                                token_a_mint = event.get('token_in_mint') or event.get('token_a_mint') or event.get('from_token')
                                token_b_mint = event.get('token_out_mint') or event.get('token_b_mint') or event.get('to_token')
                                from_amount = event.get('token_in_amount') or event.get('from_amount')
                                to_amount = event.get('token_out_amount') or event.get('to_amount')
                                from_wallet = event.get('from_wallet') or event.get('initiator')
                                to_wallet = event.get('to_wallet')
                                platform = event.get('program_id') or event.get('platform') or event.get('protocol', 'unknown')
                                
                                # Новые поля
                                program_id = event.get('program_id')
                                instruction_name = event.get('instruction_name', 'SWAP')
                                event_data_raw = json.dumps(event.get('details', {}))
                                
                                # Логируем для отладки
                                logger.debug(f"SWAP событие (прямой формат): token_in={token_a_mint}, token_out={token_b_mint}, amount_in={from_amount}, amount_out={to_amount}, program={platform}")
                            elif event_type == 'Unknown':
                                # Для Unknown событий извлекаем доступную информацию напрямую
                                platform = event.get('program_id', 'unknown')
                                from_wallet = event.get('initiator')
                                involved_accounts = event.get('involved_accounts', [])
                                if involved_accounts and not from_wallet:
                                    from_wallet = involved_accounts[0]
                                
                                # Новые поля
                                program_id = event.get('program_id')
                                instruction_name = event.get('instruction_name', 'Unknown')
                                event_data_raw = json.dumps(event.get('details', {}))
                                
                                # Логируем для отладки
                                logger.debug(f"Unknown событие (прямой формат): program_id={platform}, involved_accounts={involved_accounts}")
                        
                        elif 'data' in event:
                            # Структура: {'type': 'Unknown'/'SWAP', 'data': '<объект>'}
                            event_data_content = event.get('data', '')
                            
                            # Обрабатываем данные события
                            if isinstance(event_data_content, dict):
                                # Данные в нормальном формате (словарь)
                                if event_type == 'SWAP':
                                    token_a_mint = event_data_content.get('token_in_mint') or event_data_content.get('token_a_mint') or event_data_content.get('from_token')
                                    token_b_mint = event_data_content.get('token_out_mint') or event_data_content.get('token_b_mint') or event_data_content.get('to_token')
                                    from_amount = event_data_content.get('token_in_amount') or event_data_content.get('from_amount')
                                    to_amount = event_data_content.get('token_out_amount') or event_data_content.get('to_amount')
                                    from_wallet = event_data_content.get('from_wallet') or event_data_content.get('initiator')
                                    to_wallet = event_data_content.get('to_wallet')
                                    platform = event_data_content.get('program_id') or event_data_content.get('platform') or event_data_content.get('protocol', 'unknown')
                                    
                                    # Новые поля
                                    program_id = event_data_content.get('program_id')
                                    instruction_name = event_data_content.get('instruction_name', 'SWAP')
                                    
                                elif event_type == 'Unknown':
                                    platform = event_data_content.get('program_id', 'unknown')
                                    from_wallet = event_data_content.get('initiator')
                                    involved_accounts = event_data_content.get('involved_accounts', [])
                                    if involved_accounts and not from_wallet:
                                        from_wallet = involved_accounts[0]
                                    
                                    # Новые поля
                                    program_id = event_data_content.get('program_id')
                                    instruction_name = event_data_content.get('instruction_name', 'Unknown')
                                
                                parser_version = event_data_content.get('parser_version', '')
                                wallet_tags = event_data_content.get('wallet_tags', [])
                                event_data_raw = json.dumps(event_data_content)
                            elif isinstance(event_data_content, str) and 'object at 0x' in event_data_content:
                                # Старые данные с неправильной сериализацией
                                logger.debug(f"Обнаружен сериализованный объект (старый формат): {event_data_content[:50]}...")
                                if event_type == 'Unknown':
                                    platform = 'unknown_legacy'
                                    program_id = 'unknown_legacy'
                                    instruction_name = 'Unknown_Legacy'
                                elif event_type == 'SWAP':
                                    platform = 'swap_legacy'
                                    program_id = 'swap_legacy'
                                    instruction_name = 'SWAP_Legacy'
                                event_data_raw = event_data_content
                            else:
                                # Неизвестный формат данных
                                logger.warning(f"Неизвестный формат данных события: {type(event_data_content)}")
                                platform = 'unknown_format'
                                program_id = 'unknown_format'
                                instruction_name = 'Unknown_Format'
                                event_data_raw = str(event_data_content)
                    else:
                        # Старая структура (если есть)
                        event_type = event.get('event_type', 'unknown')
                        token_a_mint = event.get('token_a_mint')
                        token_b_mint = event.get('token_b_mint')
                        from_amount = event.get('from_amount')
                        to_amount = event.get('to_amount')
                        from_wallet = event.get('from_wallet')
                        to_wallet = event.get('to_wallet')
                        platform = event.get('platform', 'unknown')
                        parser_version = event.get('parser_version', '')
                        wallet_tags = event.get('wallet_tags', [])
                        
                        # Новые поля для старой структуры
                        program_id = event.get('program_id', platform)
                        instruction_name = event.get('instruction_name', event_type)
                        event_data_raw = json.dumps(event)
                    
                    if isinstance(wallet_tags, list):
                        # Для каждого тега создаем отдельную запись
                        if wallet_tags:
                            for tag in wallet_tags:
                                cursor.execute("""
                                INSERT INTO ml_ready_events 
                                (signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, from_wallet, to_wallet, platform, wallet_tag, parser_version, enriched_data, program_id, instruction_name, event_data_raw)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """, (
                                    signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, from_wallet, to_wallet, platform, tag, parser_version, json.dumps(event), program_id, instruction_name, event_data_raw
                                ))
                                events_count += 1
                        else:
                            # Если тегов нет, добавляем одну запись без тега
                            cursor.execute("""
                            INSERT INTO ml_ready_events 
                            (signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, from_wallet, to_wallet, platform, wallet_tag, parser_version, enriched_data, program_id, instruction_name, event_data_raw)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """, (
                                signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, from_wallet, to_wallet, platform, None, parser_version, json.dumps(event), program_id, instruction_name, event_data_raw
                            ))
                            events_count += 1
                    else:
                        # Если wallet_tags не список, добавляем одну запись без тега
                        cursor.execute("""
                        INSERT INTO ml_ready_events 
                        (signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, from_wallet, to_wallet, platform, wallet_tag, parser_version, enriched_data, program_id, instruction_name, event_data_raw)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, from_wallet, to_wallet, platform, None, parser_version, json.dumps(event), program_id, instruction_name, event_data_raw
                        ))
                        events_count += 1
                
                # Увеличиваем счетчик обработанных транзакций
                processed_count += 1
                
                # Выводим прогресс каждые 100 транзакций
                if processed_count % 100 == 0:
                    logger.info(f"Обработано {processed_count}/{total_transactions} транзакций ({processed_count/total_transactions*100:.1f}%)")
            except Exception as e:
                logger.error(f"Ошибка при обработке транзакции {tx['signature']}: {str(e)}")
                continue
        
        # Фиксируем изменения
        conn.commit()
        duration = time.time() - start_time
        logger.info(f"Обновление завершено: обработано {processed_count} транзакций, создано {events_count} событий за {duration:.2f} сек.")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при обновлении таблицы ml_ready_events: {str(e)}", exc_info=True)
        raise

def main():
    """
    Главная функция скрипта.
    """
    parser = argparse.ArgumentParser(description="Управление аналитическими витринами для ML (SQLite версия)")
    parser.add_argument("--create", "-c", action="store_true", 
                      help="Создать таблицу ml_ready_events, если она не существует")
    parser.add_argument("--refresh", "-r", action="store_true", 
                      help="Обновить таблицу ml_ready_events")
    args = parser.parse_args()
    
    if not args.create and not args.refresh:
        logger.info("Не указаны действия. Укажите --create для создания или --refresh для обновления.")
        return
    
    try:
        conn = get_db_connection()
        
        if args.create:
            create_ml_ready_events_table(conn)
        
        if args.refresh:
            refresh_ml_ready_events(conn)
            
        conn.close()
        logger.info("Операции с аналитическими витринами завершены")
    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении операций: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 