import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
try:
    import time
    import logging
    from datetime import datetime, timezone
    from utils.signature_handler import fetch_signatures_for_token
    from rpc.client import RPCClient
    from services.onchain_price_engine import OnChainPriceEngine
    import json
except Exception as e:
    import traceback
    print('IMPORT ERROR:', e, file=sys.stderr)
    traceback.print_exc()
    sys.exit(1)

TOKEN_MINT = "AGmXzgFQw8bLtsJvr5dgnHtyP6rP6GLwmZwgVqjFL6Q8"
LOG_PATH = "damping.log"
FETCH_LIMIT_PER_CALL = 1000  # Helius max per call

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("damping_logger")

def detect_swap_and_dex(raw_tx):
    """
    Возвращает (swap_found: bool, dex: str|None)
    dex: 'RAYDIUM', 'JUPITER', или None
    """
    # Проверяем enriched_data, если есть
    enriched = raw_tx.get('enriched_data') or raw_tx.get('enriched')
    if isinstance(enriched, list):
        for event in enriched:
            if event.get('event_type', '').upper() == 'SWAP':
                # Определяем DEX
                for key in ['program_id', 'program', 'protocol', 'platform']:
                    val = (event.get(key) or '').lower()
                    if 'raydium' in val:
                        return True, 'RAYDIUM'
                    if 'jupiter' in val:
                        return True, 'JUPITER'
                # Если не нашли явно, но SWAP есть
                return True, None
    # Проверяем инструкции
    tx = raw_tx.get('transaction', {})
    message = tx.get('message', {})
    instructions = message.get('instructions', [])
    for instr in instructions:
        for key in ['programId', 'program', 'protocol', 'platform']:
            val = (instr.get(key) or '').lower()
            if 'raydium' in val:
                return True, 'RAYDIUM'
            if 'jupiter' in val:
                return True, 'JUPITER'
    return False, None

def main():
    # 1. Получаем все сигнатуры для токена (все связанные адреса)
    logger.info(f"Получаем все сигнатуры для токена {TOKEN_MINT}...")
    signatures_info = fetch_signatures_for_token(
        token_mint_address=TOKEN_MINT,
        fetch_limit_per_call=FETCH_LIMIT_PER_CALL,
        total_tx_limit=None,  # Без лимита
        direction='b'  # Старые сначала
    )
    logger.info(f"Всего найдено {len(signatures_info)} уникальных сигнатур.")

    rpc_client = RPCClient()
    price_engine = OnChainPriceEngine()

    with open(LOG_PATH, "w", encoding="utf-8") as logf:
        # Заголовок — сигнатура токена
        logf.write(f"{TOKEN_MINT}\n")
        count = 0
        for sig_info in signatures_info:
            signature = sig_info["signature"]
            block_time = sig_info.get("blockTime")
            # Время транзакции (UTC)
            tx_time_utc = (
                datetime.utcfromtimestamp(block_time).strftime("%Y-%m-%dT%H:%M:%SZ")
                if block_time else "NA"
            )
            # Время парсинга (локальное)
            parse_time_local = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            # Получаем сырую транзакцию
            try:
                raw_tx = rpc_client.get_transaction(signature)
            except Exception as e:
                logger.warning(f"Ошибка при получении транзакции {signature}: {e}")
                raw_tx = None
            # Пытаемся вычислить цену
            price = 0
            swap_prefix = "NO_____SWAP"
            if raw_tx:
                try:
                    swap_found, dex = detect_swap_and_dex(raw_tx)
                    if swap_found:
                        swap_prefix = f"SWAP_{dex}" if dex else "SWAP"
                    price = price_engine.calculate_price_from_swap({}, raw_tx)
                    if price is None:
                        price = 0
                except Exception as e:
                    logger.warning(f"Ошибка при анализе транзакции {signature}: {e}")
                    price = 0
            # Логируем строку
            enriched = None
            if raw_tx:
                enriched = raw_tx.get('enriched_data') or raw_tx.get('enriched')
            enriched_str = ''
            if enriched is not None:
                try:
                    enriched_str = json.dumps(enriched, ensure_ascii=False)
                    logf.write(f"ENRICHED_DATA for {signature}: {enriched_str}\n")
                except Exception as e:
                    enriched_str = f'ENRICHED_DUMP_ERROR: {e}'
                    logf.write(f"ENRICHED_DATA for {signature}: {enriched_str}\n")
            else:
                logf.write(f"NO_ENRICHED_DATA for {signature}\n")
            logf.write(f"{swap_prefix} {parse_time_local} {tx_time_utc} {signature} {price}\n")
            count += 1
            if count % 100 == 0:
                logger.info(f"Обработано {count} транзакций...")
        # Итоговое количество
        logf.write(f"TOTAL_TRANSACTIONS {count}\n")
    logger.info(f"Готово! Итоговое количество: {count}")

if __name__ == "__main__":
    main() 