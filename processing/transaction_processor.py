# Файл: processing/transaction_processor.py (ФИНАЛЬНАЯ ВЕРСИЯ ДЛЯ РЕФАКТОРИНГА)
import logging
import json
from typing import Optional, Dict, Any
import time

# --- Новые, правильные импорты ---
from rpc.client import RPCClient
from parser.universal_parser import UniversalParser
from db.db_writer import upsert_dex_pool_registry, get_connection

logger = logging.getLogger("processing.transaction")

def process_single_transaction(
    signature: str,
    token_address: str # Параметр остается для контекста, но может не использоваться напрямую
) -> Optional[Dict[str, Any]]:
    """
    Обрабатывает одну транзакцию: получает детали через RPCClient,
    парсит и обогащает через UniversalParser.
    Возвращает словарь с результатом, готовый для downstream-модулей.
    """
    signature = signature.strip()
    logger.info(f"--- [NEW PROCESSOR] Обработка Signature: {signature[:10]}... ---")

    # 1. Получение сырых деталей транзакции через новый RPCClient
    rpc_client = RPCClient()
    transaction_details_obj = rpc_client.get_transaction(signature)
    print(f"[DEBUG] type(transaction_details_obj) = {type(transaction_details_obj)}")
    if isinstance(transaction_details_obj, dict):
        print(f"[DEBUG] transaction_details_obj keys: {list(transaction_details_obj.keys())}")
    else:
        print(f"[DEBUG] transaction_details_obj: {transaction_details_obj}")

    # Универсальная обработка: поддерживаем dict и объект с raw_json
    if not transaction_details_obj:
        logger.error(f"[{signature[:10]}] FAIL (Fetch Details). Не удалось получить данные транзакции.")
        return None
    if isinstance(transaction_details_obj, dict):
        transaction_details = transaction_details_obj
        raw_for_db = transaction_details_obj
    elif hasattr(transaction_details_obj, 'raw_json') and transaction_details_obj.raw_json:
        transaction_details = transaction_details_obj.raw_json
        raw_for_db = transaction_details_obj.raw_json
    else:
        logger.error(f"[{signature[:10]}] FAIL (Fetch Details). Неизвестный тип результата от RPCClient.")
        return None

    # Добавляем сигнатуру в словарь, если ее там нет (важно для консистентности)
    if 'signature' not in transaction_details:
        transaction_details['signature'] = signature

    tx_error = transaction_details.get("meta", {}).get("err")
    is_failed_tx = tx_error is not None

    # 2. Парсинг и обогащение через UniversalParser
    parser = UniversalParser()
    try:
        # Создаем обертку JSON-RPC, которую ожидает нормализатор
        rpc_style_response = {
            "jsonrpc": "2.0",
            "result": transaction_details,
            "id": 1
        }
        
        # UniversalParser возвращает словарь с ключами "enriched_events" и "parser_version"
        enrich_result = parser.parse_raw_transaction(rpc_style_response)
        enriched_events = enrich_result.get("enriched_events", [])
        parser_version = enrich_result.get("parser_version", "unknown")
        logger.info(f"[{signature[:10]}] Parsed {len(enriched_events)} enriched events (parser_version={parser_version})")

        # --- ОТЛАДОЧНЫЙ БЛОК 1 ---
        print("\n" + "="*20 + " DEBUG: Результат из UniversalParser " + "="*20)
        print(json.dumps(enrich_result, indent=2, default=str))
        print("="*60 + "\n")
        # --- КОНЕЦ ОТЛАДОЧНОГО БЛОКА ---
    except ValueError as e:
        if "Unrecognized Solana transaction format" in str(e):
            logger.error(f"НОРМАЛИЗАЦИЯ ПРОВАЛЕНА для сигнатуры: {signature}")
            # Сохраняем "плохой" JSON для анализа
            try:
                with open(f"logs/failed_normalization_{signature}.json", "w", encoding='utf-8') as f:
                    json.dump(transaction_details, f, indent=2, ensure_ascii=False)
                logger.info(f"Сохранен проблемный JSON: logs/failed_normalization_{signature}.json")
            except Exception as save_error:
                logger.error(f"Ошибка сохранения проблемного JSON: {save_error}")
            return None # Прерываем обработку этой транзакции
        else:
            raise # Перебрасываем другие ошибки
    except Exception as parse_err:
        logger.critical(f"[{signature[:10]}] FAIL (CRITICAL UNIVERSAL PARSER ERROR): {parse_err}", exc_info=True)
        return None

    # 3. Формирование итогового словаря с результатом для downstream-потребителей (main.py, db_writer)
    result = {
        "signature": signature,
        "slot": transaction_details.get("slot"),
        "block_time": transaction_details.get("blockTime"),
        "is_failed_tx": is_failed_tx,
        "tx_error": tx_error,
        "enriched_events": enriched_events,
        "parser_version": parser_version,
        "transaction_details": raw_for_db # Сохраняем сырой ответ от Helius
    }

    # --- NEW: Автоматическая запись LiquidityPoolCreated в dex_pools_registry ---
    for event in enriched_events:
        details = event.get("details", {})
        # Проверяем, что это событие создания пула Raydium (Initialize2)
        if (
            details.get("instruction_name") == "Initialize2" and
            details.get("program_name", "").lower().startswith("raydium")
        ):
            # Собираем все нужные поля
            pool_address = details.get("pool_address")
            lp_mint = details.get("lp_mint")
            coin_mint = details.get("coin_mint")
            pc_mint = details.get("pc_mint")
            pool_coin_token_account = details.get("pool_coin_token_account")
            pool_pc_token_account = details.get("pool_pc_token_account")
            initial_liquidity_provider = details.get("initial_liquidity_provider")
            dex_name = details.get("program_name", "Raydium")
            now = int(time.time())
            # Записываем в базу
            conn = get_connection()
            upsert_dex_pool_registry(
                conn,
                pool_address=pool_address,
                dex_name=dex_name,
                token_a_mint=coin_mint,
                token_b_mint=pc_mint,
                token_a_vault=pool_coin_token_account,
                token_b_vault=pool_pc_token_account,
                lp_mint=lp_mint,
                initial_liquidity_provider=initial_liquidity_provider,
                last_updated=now
            )
            conn.close()
    # --- END NEW ---
    return result