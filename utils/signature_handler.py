# utils/signature_handler.py

import logging
import time
from typing import List, Optional, Dict, Any, Set

# Используем относительные импорты для модулей внутри проекта
from rpc.client import RPCClient  # Используем новый, унифицированный клиент
import db.db_manager as dbm # db_manager используется для фильтрации

logger = logging.getLogger("utils.signature_handler")

def fetch_signatures_for_token(
    token_mint_address: str,
    fetch_limit_per_call: int,
    total_tx_limit: Optional[int],
    direction: str = 'e',
    max_address_retries: int = 3
) -> List[Dict[str, Any]]:
    logger.info(f"Начало сбора сигнатур для токена (минта): {token_mint_address}, Направление: {direction}, Общий лимит: {total_tx_limit}")
    
    rpc_client = RPCClient()
    try:
        ata_accounts_raw = rpc_client.get_program_accounts(
            "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            token_mint_address
        )
        ata_addresses = [acc['pubkey'] for acc in ata_accounts_raw] if ata_accounts_raw else []
        logger.info(f"  Найдено {len(ata_addresses)} ATA для минта {token_mint_address}.")
    except Exception as e:
        logger.error(f"  Ошибка при поиске ATA: {e}", exc_info=True)
        ata_addresses = []

    addresses_to_scan = list(dict.fromkeys([token_mint_address] + ata_addresses))
    logger.info(f"  Всего уникальных адресов для сканирования: {len(addresses_to_scan)}")

    all_signatures_info: List[Dict[str, Any]] = []
    
    for current_address in addresses_to_scan:
        logger.info(f"  Начало получения сигнатур для адреса: {current_address}...")
        last_signature_fetched: Optional[str] = None
        address_retry_count = 0
        
        while True:
            current_signatures_batch = rpc_client.get_signatures_for_address(
                current_address,
                limit=fetch_limit_per_call,
                before=last_signature_fetched
            )
            
            if current_signatures_batch is None:
                # Вместо немедленного прерывания, пробуем retry
                address_retry_count += 1
                if address_retry_count <= max_address_retries:
                    retry_delay = min(2.0 * address_retry_count, 10.0)  # Прогрессивная задержка 2s, 4s, 6s...
                    logger.warning(
                        f"    ⚠️  Попытка {address_retry_count}/{max_address_retries} получения сигнатур для адреса {current_address} неудачна. "
                        f"Повтор через {retry_delay:.1f}с..."
                    )
                    time.sleep(retry_delay)
                    continue
                else:
                    logger.error(
                        f"    ❌ Исчерпаны все {max_address_retries} попыток получения сигнатур для адреса {current_address}. "
                        f"Переходим к следующему адресу."
                    )
                    break
            
            # Успешный ответ - сбрасываем счетчик retry
            if address_retry_count > 0:
                logger.info(f"    ✅ Успешно получены сигнатуры для адреса {current_address} после {address_retry_count} повторных попыток")
                address_retry_count = 0
            
            if not current_signatures_batch:
                logger.info(f"    Больше сигнатур не найдено для адреса {current_address}.")
                break
                
            all_signatures_info.extend(current_signatures_batch)
            if total_tx_limit is not None and len(all_signatures_info) >= total_tx_limit:
                break
            last_signature_fetched = current_signatures_batch[-1].get("signature")
            if not last_signature_fetched:
                break
            time.sleep(0.1)
            
        if total_tx_limit is not None and len(all_signatures_info) >= total_tx_limit:
            break
            
    logger.info(f"Сбор завершен. Собрано {len(all_signatures_info)} сигнатур.")
    unique_sigs_map: Dict[str, Dict[str, Any]] = {
        s_info["signature"]: s_info for s_info in all_signatures_info if s_info.get("signature") and s_info.get("blockTime")
    }
    processed_signatures_list = list(unique_sigs_map.values())
    if len(processed_signatures_list) < len(all_signatures_info):
        logger.info(f"  После дедупликации/фильтрации осталось {len(processed_signatures_list)} уникальных сигнатур с blockTime.")
    if direction == 'b':
        logger.info(f"Флаг направления: 'b' (begin). Сортировка по blockTime (старые сначала)...")
        processed_signatures_list.sort(key=lambda x: x['blockTime'])
    else:
        logger.info(f"Флаг направления: 'e' (end). Сортировка по blockTime (новые сначала)...")
        processed_signatures_list.sort(key=lambda x: x['blockTime'], reverse=True)
    if total_tx_limit is not None:
        if len(processed_signatures_list) > total_tx_limit:
            logger.info(f"  Применение общего лимита {total_tx_limit} к списку из {len(processed_signatures_list)} транзакций.")
            processed_signatures_list = processed_signatures_list[:total_tx_limit]
        else:
            logger.info(f"  Общий лимит {total_tx_limit} не уменьшил список из {len(processed_signatures_list)} транзакций.")
    logger.info(f"Итого возвращается {len(processed_signatures_list)} сигнатур.")
    return processed_signatures_list

def filter_new_signatures(signatures_info: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Проверяет сигнатуры в БД и возвращает только новые."""
    if not signatures_info:
        return []

    logger.info(f" Проверка {len(signatures_info)} собранных сигнатур в БД...")
    signatures_to_check = [s_info["signature"].strip() for s_info in signatures_info if s_info.get("signature")]
    
    existing_signatures_in_db: Set[str] = set()
    conn = None
    try:
        dbm.initialize_engine()
        conn = dbm.get_connection()
        if conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT signature FROM transactions WHERE signature = ANY(%s)", (signatures_to_check,))
                rows = cursor.fetchall()
                existing_signatures_in_db = {row[0] for row in rows}
            logger.info(f"  Найдено {len(existing_signatures_in_db)} существующих сигнатур в БД.")
        else:
            logger.error("  Не удалось получить соединение с БД для проверки.")
    except Exception as e:
        logger.error(f"  Ошибка при проверке существующих сигнатур: {e}", exc_info=True)
    finally:
        if conn: dbm.release_connection(conn)
    
    new_signatures_info = [s_info for s_info in signatures_info if s_info.get("signature") not in existing_signatures_in_db]
    logger.info(f"  Будет обработано {len(new_signatures_info)} НОВЫХ транзакций.")
    return new_signatures_info