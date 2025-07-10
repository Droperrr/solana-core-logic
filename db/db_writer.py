# db/db_writer.py

import json
import logging
import time
import sqlite3
from typing import Dict, Any, List, Optional, Union
from db.db_manager import get_connection, check_existing_signatures, database_transaction

logger = logging.getLogger(__name__)

def upsert_transaction_sqlite(
    conn,
    signature: str,
    block_time: Optional[int],
    slot: Optional[int],
    fee_payer: Optional[str],
    transaction_type: Optional[str],
    raw_json: Dict[str, Any],
    enriched_data: Dict[str, Any],
    source_query_type: str,
    source_query_address: str,
    parser_version: str = "1.0.0"
) -> bool:
    """
    Атомарно вставляет новую транзакцию или обновляет существующую в SQLite.
    Использует конструкцию INSERT ... ON CONFLICT ... DO UPDATE для обеспечения атомарности.
    
    Args:
        conn: Соединение с базой данных SQLite.
        signature: Подпись транзакции.
        block_time: Время блока.
        slot: Номер слота.
        fee_payer: Адрес плательщика комиссии.
        transaction_type: Тип транзакции.
        raw_json: Сырые данные транзакции.
        enriched_data: Обогащенные данные транзакции.
        source_query_type: Тип источника запроса (например, 'wallet', 'token').
        source_query_address: Адрес источника запроса.
        parser_version: Версия парсера.
        
    Returns:
        bool: True, если операция была успешной, False в противном случае.
    """
    try:
        cursor = conn.cursor()
        
        # Преобразуем JSON в строку
        raw_json_str = json.dumps(raw_json) if raw_json else None
        enriched_data_str = json.dumps(enriched_data) if enriched_data else None
        
        # Используем UPSERT (INSERT ... ON CONFLICT ... DO UPDATE)
        cursor.execute('''
        INSERT INTO transactions 
        (signature, block_time, slot, fee_payer, transaction_type, source_query_type, source_query_address, raw_json, enriched_data, parser_version, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT(signature) DO UPDATE SET
            block_time = COALESCE(excluded.block_time, block_time),
            slot = COALESCE(excluded.slot, slot),
            fee_payer = COALESCE(excluded.fee_payer, fee_payer),
            transaction_type = COALESCE(excluded.transaction_type, transaction_type),
            source_query_type = COALESCE(excluded.source_query_type, source_query_type),
            source_query_address = COALESCE(excluded.source_query_address, source_query_address),
            raw_json = COALESCE(excluded.raw_json, raw_json),
            enriched_data = excluded.enriched_data,
            parser_version = excluded.parser_version,
            updated_at = CURRENT_TIMESTAMP
        ''', (
            signature,
            block_time,
            slot,
            fee_payer,
            transaction_type,
            source_query_type,
            source_query_address,
            raw_json_str,
            enriched_data_str,
            parser_version
        ))
        
        # Проверяем, была ли операция вставкой или обновлением
        if cursor.rowcount > 0:
            # Определяем, была ли это вставка или обновление
            cursor.execute("SELECT changes()")
            changes = cursor.fetchone()[0]
            
            if changes == 1:
                # Проверяем, была ли это вставка (новая запись)
                cursor.execute("SELECT COUNT(*) FROM transactions WHERE signature = ? AND created_at = updated_at", (signature,))
                is_new = cursor.fetchone()[0] > 0
                
                if is_new:
                    logger.debug(f"Транзакция {signature} успешно вставлена")
                else:
                    logger.debug(f"Транзакция {signature} успешно обновлена")
            
            conn.commit()
            return True
        else:
            logger.warning(f"Операция UPSERT для транзакции {signature} не внесла изменений")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при UPSERT транзакции {signature}: {e}")
        conn.rollback()
        return False

def log_failed_transaction(conn, signature: str, reason: str, payload=None):
    """
    Записывает сбойную транзакцию в таблицу failed_processing_log (Dead-Letter Queue).
    
    Args:
        conn: Соединение с базой данных SQLite.
        signature: Подпись транзакции.
        reason: Причина сбоя.
        payload: Сырые данные транзакции (raw_json) в виде JSON-строки.
    """
    try:
        cursor = conn.cursor()
        
        # Сериализуем payload если он не None
        payload_str = None
        if payload is not None:
            if isinstance(payload, str):
                payload_str = payload
            else:
                payload_str = json.dumps(payload, ensure_ascii=False)
        
        cursor.execute('''
        INSERT INTO failed_processing_log (signature, reason, payload)
        VALUES (?, ?, ?)
        ''', (signature, reason, payload_str))
        
        conn.commit()
        logger.warning(f"Транзакция {signature} записана в DLQ. Причина: {reason}")
        
    except Exception as e:
        logger.critical(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось записать в DLQ транзакцию {signature}. Ошибка: {e}")
        # Не делаем rollback, так как это может быть частью другой транзакции
        # Просто логируем критическую ошибку и продолжаем работу

def upsert_transaction_sqlite(
    conn,
    signature: str,
    block_time: Optional[int],
    slot: Optional[int],
    fee_payer: Optional[str],
    transaction_type: Optional[str],
    raw_json: Dict[str, Any],
    enriched_data: Dict[str, Any],
    source_query_type: str,
    source_query_address: str,
    parser_version: str = "2.0.0"
) -> bool:
    """
    Выполняет UPSERT операцию для транзакции в базу данных SQLite.
    Создает новую запись или обновляет существующую атомарным образом.
    
    Args:
        conn: Соединение с базой данных SQLite.
        signature: Подпись транзакции (уникальный ключ).
        block_time: Время блока.
        slot: Номер слота.
        fee_payer: Адрес плательщика комиссии.
        transaction_type: Тип транзакции.
        raw_json: Сырые данные транзакции.
        enriched_data: Обогащенные данные транзакции.
        source_query_type: Тип источника запроса.
        source_query_address: Адрес источника запроса.
        parser_version: Версия парсера для отслеживания обновлений.
        
    Returns:
        bool: True, если операция прошла успешно, False в противном случае.
    """
    logger.info(f"НАЧАЛО UPSERT для signature: {signature}")
    logger.info(f"ПОЛУЧЕНЫ ENRICHED_DATA для {signature}: \n{json.dumps(enriched_data, indent=2, ensure_ascii=False)}")
    
    try:
        cursor = conn.cursor()
        
        # Сериализуем JSON данные
        raw_json_str = json.dumps(raw_json, ensure_ascii=False) if raw_json else None
        enriched_data_str = json.dumps(enriched_data, ensure_ascii=False) if enriched_data else None
        
        logger.info(f"ПОДГОТОВКА К ЗАПИСИ В БД для {signature}: enriched_data_str = {enriched_data_str[:200] if enriched_data_str else 'None'}...")
        
        # UPSERT запрос - вставка с обновлением при конфликте
        query = """
            INSERT INTO transactions (
                signature, block_time, slot, fee_payer, transaction_type,
                raw_json, enriched_data, source_query_type, source_query_address,
                parser_version, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(signature) DO UPDATE SET
                enriched_data = excluded.enriched_data,
                parser_version = excluded.parser_version,
                updated_at = CURRENT_TIMESTAMP,
                -- Обновляем только если это новые данные
                block_time = COALESCE(excluded.block_time, block_time),
                slot = COALESCE(excluded.slot, slot),
                fee_payer = COALESCE(excluded.fee_payer, fee_payer),
                transaction_type = COALESCE(excluded.transaction_type, transaction_type),
                raw_json = COALESCE(excluded.raw_json, raw_json),
                source_query_type = COALESCE(excluded.source_query_type, source_query_type),
                source_query_address = COALESCE(excluded.source_query_address, source_query_address)
        """
        
        params = (
            signature, block_time, slot, fee_payer, transaction_type,
            raw_json_str, enriched_data_str, source_query_type, source_query_address,
            parser_version
        )
        
        logger.info(f"ВЫПОЛНЕНИЕ SQL-ЗАПРОСА для {signature}:\nQuery: {query}\nParams: {params}")
        
        cursor.execute(query, params)
        
        logger.info(f"SQL-ЗАПРОС ВЫПОЛНЕН для {signature}, rowcount: {cursor.rowcount}")
        
        conn.commit()
        
        # Логируем результат операции
        if cursor.rowcount > 0:
            logger.info(f"UPSERT: Транзакция {signature[:16]}... обработана (версия {parser_version})")
        
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при UPSERT транзакции {signature}: {str(e)}")
        
        # Записываем в Dead-Letter Queue
        reason = f"DB_WRITE_ERROR: {str(e)}"
        log_failed_transaction(conn, signature, reason, raw_json)
        
        conn.rollback()
        return False


def save_transaction_sqlite(
    conn,
    signature: str,
    block_time: Optional[int],
    slot: Optional[int],
    fee_payer: Optional[str],
    transaction_type: Optional[str],
    raw_json: Dict[str, Any],
    enriched_data: Dict[str, Any],
    source_query_type: str,
    source_query_address: str,
    parser_version: str = "1.0.0"
) -> bool:
    """
    DEPRECATED: Используйте upsert_transaction_sqlite для более надежной работы.
    """
    logger.warning("save_transaction_sqlite is deprecated. Use upsert_transaction_sqlite instead.")
    return upsert_transaction_sqlite(
        conn, signature, block_time, slot, fee_payer, transaction_type,
        raw_json, enriched_data, source_query_type, source_query_address, parser_version
    )
    
    try:
        cursor = conn.cursor()
        
        # Преобразуем JSON в строку
        raw_json_str = json.dumps(raw_json) if raw_json else None
        enriched_data_str = json.dumps(enriched_data) if enriched_data else None
        
        # Выполняем вставку данных
        cursor.execute('''
        INSERT INTO transactions 
        (signature, block_time, slot, fee_payer, transaction_type, source_query_type, source_query_address, raw_json, enriched_data, parser_version)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            signature,
            block_time,
            slot,
            fee_payer,
            transaction_type,
            source_query_type,
            source_query_address,
            raw_json_str,
            enriched_data_str,
            parser_version
        ))
        conn.commit()
        
        return True
    except sqlite3.IntegrityError:
        # Запись с таким signature уже существует
        logger.warning(f"Транзакция с подписью {signature} уже существует в базе данных.")
        return False
    except Exception as e:
        logger.error(f"Ошибка при сохранении транзакции {signature}: {e}")
        conn.rollback()
        return False

def update_transaction_enriched_data_sqlite(
    conn,
    signature: str,
    enriched_data: Dict[str, Any],
    parser_version: str = "1.0.0"
) -> bool:
    """
    Обновляет обогащенные данные для существующей транзакции в SQLite.
    
    Args:
        conn: Соединение с базой данных SQLite.
        signature: Подпись транзакции.
        enriched_data: Новые обогащенные данные транзакции.
        parser_version: Версия парсера.
        
    Returns:
        bool: True, если транзакция была успешно обновлена, False в противном случае.
    """
    try:
        cursor = conn.cursor()
        
        # Преобразуем JSON в строку
        enriched_data_str = json.dumps(enriched_data) if enriched_data else None
        
        # Выполняем обновление данных
        cursor.execute('''
        UPDATE transactions 
        SET enriched_data = ?, parser_version = ?, updated_at = CURRENT_TIMESTAMP
        WHERE signature = ?
        ''', (
            enriched_data_str,
            parser_version,
            signature
        ))
        
        # Проверяем, была ли обновлена хотя бы одна строка
        if cursor.rowcount == 0:
            logger.warning(f"Транзакция с подписью {signature} не найдена для обновления.")
            return False
            
        conn.commit()
        logger.info(f"Обогащенные данные для транзакции {signature} успешно обновлены.")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка при обновлении транзакции {signature}: {e}")
        conn.rollback()
        return False

def save_transactions_batch(
    conn,
    transactions: List[Dict[str, Any]],
    source_query_type: str,
    source_query_address: str,
    parser_version: str = "1.0.0",
    use_upsert: bool = True
) -> Dict[str, int]:
    """
    Сохраняет пакет транзакций в базу данных SQLite.
    
    Args:
        conn: Соединение с базой данных SQLite.
        transactions: Список транзакций для сохранения.
        source_query_type: Тип источника запроса.
        source_query_address: Адрес источника запроса.
        parser_version: Версия парсера.
        use_upsert: Использовать UPSERT вместо отдельных INSERT/UPDATE операций.
        
    Returns:
        Dict[str, int]: Статистика операции.
    """
    if use_upsert:
        stats = {"success": 0, "failed": 0}
    else:
        stats = {"success": 0, "failed": 0, "duplicates": 0, "updated": 0}
    
    if not transactions:
        return stats
    
    if not use_upsert:
        # Старая логика с предварительной проверкой дубликатов
        signatures = [tx.get('signature') for tx in transactions if tx.get('signature')]
        existing_signatures = check_existing_signatures(conn, signatures)
    
    with database_transaction(conn) as transaction_conn:
        for tx_data in transactions:
            signature = tx_data.get('signature')
            
            if not signature:
                logger.warning("Пропуск транзакции без подписи")
                stats["failed"] += 1
                continue
            
            try:
                if use_upsert:
                    # Используем новую UPSERT логику
                    success = upsert_transaction_sqlite(
                        transaction_conn,
                        signature=signature,
                        block_time=tx_data.get('block_time'),
                        slot=tx_data.get('slot'),
                        fee_payer=tx_data.get('fee_payer'),
                        transaction_type=tx_data.get('transaction_type'),
                        raw_json=tx_data.get('raw_json', {}),
                        enriched_data=tx_data.get('enriched_data', {}),
                        source_query_type=source_query_type,
                        source_query_address=source_query_address,
                        parser_version=parser_version
                    )
                    
                    if success:
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1
                        
                else:
                    # Старая логика с отдельными операциями
                    if signature in existing_signatures:
                        # Обновляем существующую транзакцию с новыми обогащенными данными
                        try:
                            update_success = update_transaction_enriched_data_sqlite(
                                transaction_conn,
                                signature=signature,
                                enriched_data=tx_data.get('enriched_data', {}),
                                parser_version=parser_version
                            )
                            if update_success:
                                stats["updated"] += 1
                                logger.debug(f"Транзакция {signature} обновлена с новыми обогащенными данными")
                            else:
                                stats["failed"] += 1
                        except Exception as e:
                            logger.error(f"Ошибка при обновлении транзакции {signature}: {e}")
                            stats["failed"] += 1
                        continue
                    
                    success = save_transaction_sqlite(
                        transaction_conn,
                        signature=signature,
                        block_time=tx_data.get('block_time'),
                        slot=tx_data.get('slot'),
                        fee_payer=tx_data.get('fee_payer'),
                        transaction_type=tx_data.get('transaction_type'),
                        raw_json=tx_data.get('raw_json', {}),
                        enriched_data=tx_data.get('enriched_data', {}),
                        source_query_type=source_query_type,
                        source_query_address=source_query_address,
                        parser_version=parser_version
                    )
                    
                    if success:
                        stats["success"] += 1
                    else:
                        stats["failed"] += 1
                        
            except Exception as e:
                logger.error(f"Ошибка при сохранении транзакции {signature}: {e}")
                stats["failed"] += 1
    
    return stats

# Для совместимости оставляем оригинальное имя функции
def save_transaction(*args, **kwargs) -> bool:
    """
    Совместимая функция для сохранения транзакции.
    Перенаправляет вызов на upsert_transaction_sqlite.
    """
    return upsert_transaction_sqlite(*args, **kwargs)

def upsert_dex_pool_registry(
    conn,
    pool_address: str,
    dex_name: str,
    token_a_mint: str,
    token_b_mint: str,
    token_a_vault: str,
    token_b_vault: str,
    lp_mint: str = None,
    initial_liquidity_provider: str = None,
    last_updated: int = None
) -> bool:
    """
    UPSERT (insert or update) a DEX pool record in dex_pools_registry (SQLite).
    Args:
        conn: SQLite connection
        pool_address: Pool address (primary key)
        dex_name: DEX name (e.g., 'Raydium')
        token_a_mint: First token mint
        token_b_mint: Second token mint
        token_a_vault: Vault for token A
        token_b_vault: Vault for token B
        lp_mint: LP token mint address
        initial_liquidity_provider: First liquidity provider wallet
        last_updated: Unix timestamp (int)
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO dex_pools_registry (
                pool_address, dex_name, token_a_mint, token_b_mint, token_a_vault, token_b_vault, lp_mint, initial_liquidity_provider, last_updated
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(pool_address) DO UPDATE SET
                dex_name=excluded.dex_name,
                token_a_mint=excluded.token_a_mint,
                token_b_mint=excluded.token_b_mint,
                token_a_vault=excluded.token_a_vault,
                token_b_vault=excluded.token_b_vault,
                lp_mint=excluded.lp_mint,
                initial_liquidity_provider=excluded.initial_liquidity_provider,
                last_updated=excluded.last_updated
        ''', (
            pool_address, dex_name, token_a_mint, token_b_mint, token_a_vault, token_b_vault, lp_mint, initial_liquidity_provider, last_updated
        ))
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Ошибка UPSERT в dex_pools_registry для пула {pool_address}: {e}")
        conn.rollback()
        return False

def insert_transaction_instructions(conn, signature, instructions):
    """
    Пакетная вставка инструкций для одной транзакции в PostgreSQL.
    :param conn: psycopg2 connection
    :param signature: строка, идентификатор транзакции
    :param instructions: список dict-ов с полями:
        - instruction_index
        - outer_instruction_index
        - inner_instruction_index
        - is_inner
        - program_id
        - instruction_type (по стратегии: человекочитаемый, UNKNOWN_{PROGRAM}, UNKNOWN)
        - details (dict, будет сериализован в JSONB)
    """
    values = []
    for instr in instructions:
        # Формируем instruction_type по согласованной стратегии
        if instr.get('instruction_type'):
            instruction_type = instr['instruction_type']
        elif instr.get('program_id'):
            program = instr['program_id']
            # Обрезаем program_id для UNKNOWN_{PROGRAM}, если длинный
            program_short = program[:8] if len(program) > 8 else program
            instruction_type = f"UNKNOWN_{program_short}"
        else:
            instruction_type = "UNKNOWN"
        values.append((
            signature,
            instr['instruction_index'],
            instr['outer_instruction_index'],
            instr.get('inner_instruction_index'),
            instr['is_inner'],
            instr['program_id'],
            instruction_type,
            json.dumps(instr.get('details', {}), ensure_ascii=False)
        ))
    try:
        with conn.cursor() as cur:
            cur.executemany(
                '''
                INSERT INTO transaction_instructions (
                    signature, instruction_index, outer_instruction_index, inner_instruction_index,
                    is_inner, program_id, instruction_type, details
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ''',
                values
            )
        conn.commit()
        logging.info(f"Вставлено {len(values)} инструкций для транзакции {signature}")
    except Exception as e:
        logging.error(f"Ошибка при вставке инструкций для транзакции {signature}: {e}")
        conn.rollback()

def upsert_event(conn, event: Dict[str, Any]):
    """
    Выполняет UPSERT для одного события в таблицу 'events'.
    """
    # Преобразуем JSON в строку
    details_str = json.dumps(event.get('details')) if event.get('details') else None
    enrichment_data_str = json.dumps(event.get('enrichment_data')) if event.get('enrichment_data') else None

    query = """
        INSERT INTO events (
            event_id, 
            signature, 
            block_time, 
            source, 
            event_type, 
            details, 
            enrichment_data
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(event_id) DO UPDATE SET
            signature=excluded.signature,
            block_time=excluded.block_time,
            source=excluded.source,
            event_type=excluded.event_type,
            details=excluded.details,
            enrichment_data=excluded.enrichment_data,
            updated_at=CURRENT_TIMESTAMP
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (
        event.get('event_id'),
        event.get('signature'),
        event.get('block_time'),
        event.get('source'),
        event.get('type'), # В схеме события это поле называется 'type'
        details_str,
        enrichment_data_str
    ))


@database_transaction
def save_events_to_db(conn, events: List[Dict[str, Any]]):
    """
    Сохраняет список событий в базу данных.
    """
    for event in events:
        try:
            upsert_event(conn, event)
        except Exception as e:
            logger.error(f"Ошибка при сохранении события {event.get('event_id')}: {e}")
    logger.info(f"Сохранено/обновлено {len(events)} событий.")

# Тестирование модуля
if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(level=logging.INFO,
                       format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    print("--- Тестирование DB Writer (SQLite) с UPSERT ---")
    
    # Получаем соединение с базой данных
    conn = get_connection()
    
    try:
        # Тестовые данные
        test_signature = f"test_upsert_tx_{int(time.time())}"
        test_tx = {
            "signature": test_signature,
            "block_time": int(time.time()),
            "slot": 100000000,
            "fee_payer": "test_fee_payer",
            "transaction_type": "test_transaction",
            "raw_json": {"test": "raw_data"},
            "enriched_data": {"test": "enriched_data"}
        }
        
        # Тест UPSERT (первая вставка)
        print("Тест UPSERT (первая вставка)...")
        result1 = upsert_transaction_sqlite(
            conn,
            signature=test_tx["signature"],
            block_time=test_tx["block_time"],
            slot=test_tx["slot"],
            fee_payer=test_tx["fee_payer"],
            transaction_type=test_tx["transaction_type"],
            raw_json=test_tx["raw_json"],
            enriched_data=test_tx["enriched_data"],
            source_query_type="test",
            source_query_address="test_address"
        )
        print(f"Результат первой вставки: {'успешно' if result1 else 'неудачно'}")
        
        # Тест UPSERT (обновление существующей записи)
        print("\nТест UPSERT (обновление)...")
        updated_enriched_data = {"test": "updated_enriched_data", "version": "2.0"}
        result2 = upsert_transaction_sqlite(
            conn,
            signature=test_tx["signature"],
            block_time=test_tx["block_time"],
            slot=test_tx["slot"],
            fee_payer=test_tx["fee_payer"],
            transaction_type=test_tx["transaction_type"],
            raw_json=test_tx["raw_json"],
            enriched_data=updated_enriched_data,
            source_query_type="test",
            source_query_address="test_address",
            parser_version="2.0.0"
        )
        print(f"Результат обновления: {'успешно' if result2 else 'неудачно'}")
        
        # Проверка результата
        cursor = conn.cursor()
        cursor.execute("SELECT enriched_data, parser_version FROM transactions WHERE signature = ?", (test_signature,))
        result = cursor.fetchone()
        if result:
            enriched_data = json.loads(result[0])
            parser_version = result[1]
            print(f"Проверка: enriched_data = {enriched_data}")
            print(f"Проверка: parser_version = {parser_version}")
        
        # Тест пакетной обработки с UPSERT
        print("\nТест пакетной обработки с UPSERT...")
        batch_transactions = [
            {
                "signature": f"batch_upsert_test_{i}_{int(time.time())}",
                "block_time": int(time.time()),
                "slot": 100000000 + i,
                "fee_payer": "test_batch_fee_payer",
                "transaction_type": "test_batch_transaction",
                "raw_json": {"batch_test": f"raw_data_{i}"},
                "enriched_data": {"batch_test": f"enriched_data_{i}"}
            }
            for i in range(3)
        ]
        
        batch_result = save_transactions_batch(
            conn,
            transactions=batch_transactions,
            source_query_type="batch_test",
            source_query_address="batch_test_address",
            use_upsert=True
        )
        print(f"Результаты пакетной обработки с UPSERT: {batch_result}")
        
        # Проверка наличия вставленных записей
        cursor.execute("SELECT COUNT(*) as count FROM transactions WHERE signature LIKE 'test%' OR signature LIKE 'batch_upsert_test%'")
        count = cursor.fetchone()['count']
        print(f"\nНайдено {count} тестовых записей в базе данных")
        
    except Exception as e:
        print(f"Ошибка при тестировании: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Закрываем соединение
        conn.close()
        print("Соединение с базой данных закрыто")