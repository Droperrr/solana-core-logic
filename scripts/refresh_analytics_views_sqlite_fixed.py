#!/usr/bin/env python3
"""
ИСПРАВЛЕННАЯ ВЕРСИЯ: Скрипт для создания и обновления аналитических витрин (ml_ready_events) в SQLite.
Корректно обрабатывает структуру enriched_data и извлекает максимальную информацию.
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
        logging.FileHandler("logs/refresh_views_sqlite_fixed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_views_sqlite_fixed")

def dict_factory(cursor, row):
    """Конвертирует строки результатов запроса в словари"""
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

def get_sqlite_db_path():
    """Возвращает путь к файлу базы данных SQLite."""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), 'db', 'solana_db.sqlite')

def get_db_connection():
    """Создает и возвращает соединение с базой данных SQLite."""
    db_path = get_sqlite_db_path()
    logger.info(f"Подключение к SQLite: {db_path}")
    
    # Создаем директорию для базы данных, если её нет
    Path(os.path.dirname(db_path)).mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(db_path)
    conn.row_factory = dict_factory
    return conn

def create_ml_ready_events_table(conn):
    """Создает таблицу ml_ready_events с поддержкой --rebuild опции."""
    try:
        logger.info("Создание таблицы ml_ready_events")
        cursor = conn.cursor()
        
        # Удаляем таблицу если существует (для --rebuild)
        cursor.execute("DROP TABLE IF EXISTS ml_ready_events;")
        
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
            program_id TEXT,
            instruction_name TEXT,
            event_data_raw TEXT,
            -- НОВЫЕ ПОЛЯ для улучшенного анализа
            net_token_changes TEXT,
            involved_accounts TEXT,
            compute_units_consumed INTEGER
        );
        """)
        
        # Создаем индексы для быстрого доступа
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_a ON ml_ready_events(token_a_mint);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_token_b ON ml_ready_events(token_b_mint);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_event_type ON ml_ready_events(event_type);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_wallet_tag ON ml_ready_events(wallet_tag);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_block_time ON ml_ready_events(block_time);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_signature ON ml_ready_events(signature);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_from_wallet ON ml_ready_events(from_wallet);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_to_wallet ON ml_ready_events(to_wallet);",
            "CREATE INDEX IF NOT EXISTS idx_ml_ready_events_platform ON ml_ready_events(platform);"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        conn.commit()
        logger.info("Таблица ml_ready_events успешно создана")
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при создании таблицы ml_ready_events: {str(e)}", exc_info=True)
        raise

def extract_token_info_from_changes(net_token_changes):
    """
    Извлекает информацию о токенах и количествах из net_token_changes.
    
    Args:
        net_token_changes: Словарь с изменениями токенов
        
    Returns:
        tuple: (token_a_mint, token_b_mint, from_amount, to_amount)
    """
    if not isinstance(net_token_changes, dict) or not net_token_changes:
        return None, None, None, None
    
    tokens = list(net_token_changes.keys())
    amounts = list(net_token_changes.values())
    
    # Ищем токены с положительными и отрицательными изменениями
    positive_tokens = [(token, amount) for token, amount in net_token_changes.items() if amount > 0]
    negative_tokens = [(token, amount) for token, amount in net_token_changes.items() if amount < 0]
    
    token_a_mint = None
    token_b_mint = None
    from_amount = None
    to_amount = None
    
    # Если есть и положительные, и отрицательные изменения - это похоже на SWAP
    if positive_tokens and negative_tokens:
        # Берем первый отрицательный (продаваемый токен)
        token_a_mint = negative_tokens[0][0]
        from_amount = abs(negative_tokens[0][1])
        
        # Берем первый положительный (покупаемый токен)
        token_b_mint = positive_tokens[0][0]
        to_amount = positive_tokens[0][1]
    
    # Если только положительные изменения - это может быть входящий трансфер
    elif positive_tokens and not negative_tokens:
        token_b_mint = positive_tokens[0][0]
        to_amount = positive_tokens[0][1]
    
    # Если только отрицательные изменения - это может быть исходящий трансфер
    elif negative_tokens and not positive_tokens:
        token_a_mint = negative_tokens[0][0]
        from_amount = abs(negative_tokens[0][1])
    
    return token_a_mint, token_b_mint, from_amount, to_amount

def classify_event_type(details, enrichment_data):
    """
    Классифицирует тип события на основе доступных данных.
    
    Args:
        details: Детали события из enriched_data
        enrichment_data: Данные обогащения
        
    Returns:
        str: Классифицированный тип события
    """
    if not isinstance(enrichment_data, dict):
        return 'UNKNOWN'
    
    net_token_changes = enrichment_data.get('net_token_changes', {})
    
    if not isinstance(net_token_changes, dict):
        return 'UNKNOWN'
    
    # Подсчитываем количество токенов с изменениями
    tokens_with_changes = len([amount for amount in net_token_changes.values() if amount != 0])
    positive_changes = len([amount for amount in net_token_changes.values() if amount > 0])
    negative_changes = len([amount for amount in net_token_changes.values() if amount < 0])
    
    # Если есть и положительные, и отрицательные изменения - скорее всего SWAP
    if positive_changes > 0 and negative_changes > 0:
        return 'SWAP'
    
    # Если только положительные или только отрицательные - скорее всего TRANSFER
    elif tokens_with_changes > 0:
        return 'TRANSFER'
    
    # Проверяем программу
    program_id = details.get('program_id', '')
    if program_id:
        # Известные программы DEX
        if any(dex in program_id for dex in ['Jupiter', 'Raydium', 'Orca', 'Serum']):
            return 'SWAP'
        # SPL Token программа
        elif 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA' in program_id:
            return 'TRANSFER'
    
    return 'UNKNOWN'

def refresh_ml_ready_events(conn, rebuild=False):
    """
    ИСПРАВЛЕННАЯ ВЕРСИЯ: Обновляет таблицу ml_ready_events данными из таблицы transactions.
    Корректно обрабатывает текущую структуру enriched_data.
    
    Args:
        conn: Соединение с базой данных SQLite.
        rebuild: Если True, полностью пересоздает таблицу.
    """
    try:
        logger.info("Обновление таблицы ml_ready_events (ИСПРАВЛЕННАЯ ВЕРСИЯ)")
        start_time = time.time()
        cursor = conn.cursor()
        
        # Если rebuild, пересоздаем таблицу
        if rebuild:
            create_ml_ready_events_table(conn)
        else:
            # Очищаем таблицу перед обновлением
            cursor.execute("DELETE FROM ml_ready_events;")
        
        # Получаем данные из таблицы transactions
        cursor.execute("SELECT signature, block_time, enriched_data FROM transactions WHERE enriched_data IS NOT NULL;")
        transactions = cursor.fetchall()
        
        # Подсчитываем количество обрабатываемых транзакций
        total_transactions = len(transactions)
        logger.info(f"Найдено {total_transactions} транзакций для обработки")
        
        # Счетчики для отслеживания прогресса
        processed_count = 0
        events_count = 0
        swap_events = 0
        transfer_events = 0
        unknown_events = 0
        
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
                
                # enriched_data может быть списком событий или одним событием
                events = enriched_data if isinstance(enriched_data, list) else [enriched_data]
                
                # Обрабатываем каждое событие
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    
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
                    net_token_changes = None
                    involved_accounts = None
                    compute_units_consumed = None
                    
                    # Извлекаем основную информацию
                    event_type = event.get('type', 'UNKNOWN')
                    details = event.get('details', {})
                    enrichment_data = event.get('enrichment_data', {})
                    
                    # Извлекаем данные из details
                    if isinstance(details, dict):
                        program_id = details.get('program_id')
                        instruction_name = details.get('instruction_name')
                        involved_accounts = details.get('involved_accounts', [])
                        
                        # Определяем кошельки из involved_accounts
                        if isinstance(involved_accounts, list) and involved_accounts:
                            from_wallet = involved_accounts[0] if len(involved_accounts) > 0 else None
                            to_wallet = involved_accounts[1] if len(involved_accounts) > 1 else from_wallet
                    
                    # Извлекаем данные из enrichment_data
                    if isinstance(enrichment_data, dict):
                        net_token_changes_data = enrichment_data.get('net_token_changes', {})
                        compute_units_consumed = enrichment_data.get('compute_units_consumed')
                        
                        if isinstance(net_token_changes_data, dict):
                            # Извлекаем информацию о токенах из net_token_changes
                            token_a_mint, token_b_mint, from_amount, to_amount = extract_token_info_from_changes(net_token_changes_data)
                            net_token_changes = json.dumps(net_token_changes_data)
                            
                            # Переклассифицируем событие на основе данных
                            if event_type == 'UNKNOWN':
                                event_type = classify_event_type(details, enrichment_data)
                    
                    # Устанавливаем платформу
                    if program_id:
                        platform = program_id
                    
                    # Конвертируем involved_accounts в JSON строку
                    if isinstance(involved_accounts, list):
                        involved_accounts = json.dumps(involved_accounts)
                    
                    # Сериализуем event_data_raw
                    event_data_raw = json.dumps(event)
                    
                    # Добавляем запись в ml_ready_events
                    cursor.execute("""
                    INSERT INTO ml_ready_events 
                    (signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, 
                     from_wallet, to_wallet, platform, wallet_tag, parser_version, enriched_data, 
                     program_id, instruction_name, event_data_raw, net_token_changes, involved_accounts, 
                     compute_units_consumed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, 
                        from_wallet, to_wallet, platform, None, parser_version, json.dumps(event), 
                        program_id, instruction_name, event_data_raw, net_token_changes, involved_accounts, 
                        compute_units_consumed
                    ))
                    
                    events_count += 1
                    
                    # Подсчитываем типы событий
                    if event_type == 'SWAP':
                        swap_events += 1
                    elif event_type == 'TRANSFER':
                        transfer_events += 1
                    else:
                        unknown_events += 1
                
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
        
        # Детальная статистика
        logger.info(f"=== РЕЗУЛЬТАТЫ ОБРАБОТКИ ===")
        logger.info(f"Обработано транзакций: {processed_count}")
        logger.info(f"Создано событий: {events_count}")
        logger.info(f"  - SWAP событий: {swap_events}")
        logger.info(f"  - TRANSFER событий: {transfer_events}")
        logger.info(f"  - UNKNOWN событий: {unknown_events}")
        logger.info(f"Время выполнения: {duration:.2f} сек.")
        logger.info(f"Конверсия: {events_count/total_transactions*100:.1f}%")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Ошибка при обновлении таблицы ml_ready_events: {str(e)}", exc_info=True)
        raise

def main():
    """Главная функция скрипта."""
    parser = argparse.ArgumentParser(description="ИСПРАВЛЕННОЕ управление аналитическими витринами для ML (SQLite версия)")
    parser.add_argument("--create", "-c", action="store_true", 
                      help="Создать таблицу ml_ready_events, если она не существует")
    parser.add_argument("--refresh", "-r", action="store_true", 
                      help="Обновить таблицу ml_ready_events")
    parser.add_argument("--rebuild", action="store_true", 
                      help="Полностью пересоздать таблицу ml_ready_events")
    args = parser.parse_args()
    
    if not args.create and not args.refresh:
        logger.info("Не указаны действия. Укажите --create для создания или --refresh для обновления.")
        return
    
    try:
        conn = get_db_connection()
        
        if args.create or args.rebuild:
            create_ml_ready_events_table(conn)
        
        if args.refresh:
            refresh_ml_ready_events(conn, rebuild=args.rebuild)
            
        conn.close()
        logger.info("Операции с аналитическими витринами завершены")
    except Exception as e:
        logger.error(f"Произошла ошибка при выполнении операций: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main() 