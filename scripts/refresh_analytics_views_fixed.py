#!/usr/bin/env python3
"""
ИСПРАВЛЕННАЯ ВЕРСИЯ ETL: Скрипт для создания и обновления аналитических витрин (ml_ready_events).
Корректно обрабатывает структуру enriched_data и извлекает максимальную информацию из событий.
"""
import os
import sys
import time
import logging
import argparse
import json
import sqlite3
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logs/refresh_views_fixed.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ml_views_fixed")

def get_db_connection():
    """Создает соединение с базой данных SQLite."""
    db_path = "db/solana_db.sqlite"
    logger.info(f"Подключение к SQLite: {db_path}")
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def extract_token_info_from_changes(net_token_changes):
    """
    Извлекает информацию о токенах и количествах из net_token_changes.
    """
    if not isinstance(net_token_changes, dict) or not net_token_changes:
        return None, None, None, None
    
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

def rebuild_ml_ready_events():
    """
    ИСПРАВЛЕННАЯ ВЕРСИЯ: Полностью пересоздает таблицу ml_ready_events.
    """
    try:
        logger.info("🚀 ЗАПУСК ИСПРАВЛЕННОГО ETL ПАЙПЛАЙНА")
        start_time = time.time()
        
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # 1. Удаляем и пересоздаем таблицу
        logger.info("Пересоздание таблицы ml_ready_events...")
        cursor.execute("DROP TABLE IF EXISTS ml_ready_events;")
        
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
            net_token_changes TEXT,
            involved_accounts TEXT,
            compute_units_consumed INTEGER
        );
        """)
        
        # Создаем индексы
        indexes = [
            "CREATE INDEX idx_ml_events_token_a ON ml_ready_events(token_a_mint);",
            "CREATE INDEX idx_ml_events_token_b ON ml_ready_events(token_b_mint);",
            "CREATE INDEX idx_ml_events_event_type ON ml_ready_events(event_type);",
            "CREATE INDEX idx_ml_events_block_time ON ml_ready_events(block_time);",
            "CREATE INDEX idx_ml_events_signature ON ml_ready_events(signature);",
            "CREATE INDEX idx_ml_events_from_wallet ON ml_ready_events(from_wallet);",
            "CREATE INDEX idx_ml_events_platform ON ml_ready_events(platform);"
        ]
        
        for index_sql in indexes:
            cursor.execute(index_sql)
        
        # 2. Получаем все транзакции с enriched_data
        logger.info("Загрузка транзакций...")
        cursor.execute("SELECT signature, block_time, enriched_data FROM transactions WHERE enriched_data IS NOT NULL;")
        transactions = cursor.fetchall()
        
        total_transactions = len(transactions)
        logger.info(f"Найдено {total_transactions} транзакций для обработки")
        
        # Счетчики
        processed_count = 0
        events_count = 0
        swap_events = 0
        transfer_events = 0
        unknown_events = 0
        errors_count = 0
        
        # 3. Обрабатываем каждую транзакцию
        for tx in transactions:
            try:
                # Парсим enriched_data
                if isinstance(tx['enriched_data'], str):
                    enriched_data = json.loads(tx['enriched_data'])
                else:
                    enriched_data = tx['enriched_data']
                
                signature = tx['signature']
                block_time = tx['block_time']
                
                # enriched_data может быть списком событий или одним событием
                events = enriched_data if isinstance(enriched_data, list) else [enriched_data]
                
                # Обрабатываем каждое событие
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    
                    # Инициализация полей
                    token_a_mint = None
                    token_b_mint = None
                    from_amount = None
                    to_amount = None
                    from_wallet = None
                    to_wallet = None
                    platform = 'unknown'
                    program_id = None
                    instruction_name = None
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
                        involved_accounts_list = details.get('involved_accounts', [])
                        
                        # Определяем кошельки
                        if isinstance(involved_accounts_list, list) and involved_accounts_list:
                            from_wallet = involved_accounts_list[0] if len(involved_accounts_list) > 0 else None
                            to_wallet = involved_accounts_list[1] if len(involved_accounts_list) > 1 else from_wallet
                    
                    # Извлекаем данные из enrichment_data
                    if isinstance(enrichment_data, dict):
                        net_token_changes_data = enrichment_data.get('net_token_changes', {})
                        compute_units_consumed = enrichment_data.get('compute_units_consumed')
                        
                        if isinstance(net_token_changes_data, dict) and net_token_changes_data:
                            # Извлекаем информацию о токенах
                            token_a_mint, token_b_mint, from_amount, to_amount = extract_token_info_from_changes(net_token_changes_data)
                            net_token_changes = json.dumps(net_token_changes_data)
                            
                            # Переклассифицируем событие
                            if event_type == 'UNKNOWN':
                                event_type = classify_event_type(details, enrichment_data)
                    
                    # Устанавливаем платформу
                    if program_id:
                        platform = program_id
                    
                    # Сериализуем данные
                    if isinstance(involved_accounts_list, list):
                        involved_accounts = json.dumps(involved_accounts_list)
                    
                    event_data_raw = json.dumps(event)
                    
                    # Вставляем в базу
                    cursor.execute("""
                    INSERT INTO ml_ready_events 
                    (signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, 
                     from_wallet, to_wallet, platform, wallet_tag, parser_version, enriched_data, 
                     program_id, instruction_name, event_data_raw, net_token_changes, involved_accounts, 
                     compute_units_consumed)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        signature, block_time, token_a_mint, token_b_mint, event_type, from_amount, to_amount, 
                        from_wallet, to_wallet, platform, None, '', json.dumps(event), 
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
                
                processed_count += 1
                
                # Показываем прогресс
                if processed_count % 100 == 0:
                    logger.info(f"Обработано {processed_count}/{total_transactions} транзакций ({processed_count/total_transactions*100:.1f}%)")
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке транзакции {tx['signature']}: {str(e)}")
                errors_count += 1
                continue
        
        # Фиксируем изменения
        conn.commit()
        conn.close()
        
        duration = time.time() - start_time
        
        # Итоговая статистика
        logger.info("🎯 === РЕЗУЛЬТАТЫ ИСПРАВЛЕННОГО ETL ===")
        logger.info(f"📊 Обработано транзакций: {processed_count}")
        logger.info(f"✅ Создано событий: {events_count}")
        logger.info(f"   🔄 SWAP событий: {swap_events}")
        logger.info(f"   📤 TRANSFER событий: {transfer_events}")
        logger.info(f"   ❓ UNKNOWN событий: {unknown_events}")
        logger.info(f"❌ Ошибок: {errors_count}")
        logger.info(f"⏱️  Время выполнения: {duration:.2f} сек.")
        logger.info(f"📈 Конверсия: {events_count/total_transactions*100:.1f}%")
        
        print("\n" + "="*60)
        print("🚀 ИСПРАВЛЕННЫЙ ETL ЗАВЕРШЕН!")
        print(f"📊 Конверсия улучшена с ~15% до {events_count/total_transactions*100:.1f}%")
        print(f"✅ Создано {events_count} ML событий из {total_transactions} транзакций")
        print(f"🔄 SWAP: {swap_events}, 📤 TRANSFER: {transfer_events}, ❓ UNKNOWN: {unknown_events}")
        print("="*60)
        
    except Exception as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА в ETL: {str(e)}", exc_info=True)
        raise

def main():
    """Главная функция скрипта."""
    parser = argparse.ArgumentParser(description="ИСПРАВЛЕННЫЙ ETL для ml_ready_events")
    parser.add_argument("--rebuild", action="store_true", help="Полностью пересоздать таблицу ml_ready_events")
    args = parser.parse_args()
    
    if args.rebuild:
        rebuild_ml_ready_events()
    else:
        print("Используйте --rebuild для запуска исправленного ETL")

if __name__ == "__main__":
    main() 