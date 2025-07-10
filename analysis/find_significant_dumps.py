import sqlite3
import logging
from typing import List, Dict, Any, Optional

# Настройка логгера
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_PATH = "db/solana_db.sqlite"

def is_significant_dump(current_price_in_sol: float, previous_price_in_sol: float, volume_in_sol: float) -> bool:
    """
    Определяет, является ли изменение цены значимым дампом.
    (Копия логики из OnChainPriceEngine для использования в аналитических скриптах)
    """
    if previous_price_in_sol == 0:
        return False

    price_drop_percentage = (previous_price_in_sol - current_price_in_sol) / previous_price_in_sol

    # 1. Проверка минимального падения
    if price_drop_percentage < 0.30:
        return False

    # 2. Отсекаем транзакции с микро-объемами
    if volume_in_sol < 0.1:
        return False

    # 3. Итоговое решение на основе порогов
    if price_drop_percentage > 0.5 and volume_in_sol > 5:
        return True
    
    if 0.3 <= price_drop_percentage <= 0.5 and volume_in_sol > 20:
        return True
        
    return False

def find_dumps_for_token(token_address: str) -> List[Dict[str, Any]]:
    """
    Анализирует историю транзакций токена в БД и находит значимые дампы.

    Args:
        token_address: Mint-адрес токена для анализа.

    Returns:
        Список словарей, где каждый словарь представляет найденный дамп.
    """
    logging.info(f"Анализ на наличие дампов для токена: {token_address}")
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    # 1. Выбираем все транзакции для данного токена, где есть цена и объем
    # Мы ищем, где токен был либо на входе, либо на выходе.
    # Используем json_extract для доступа к данным в `enriched_data`.
    cursor.execute("""
        SELECT 
            signature, 
            block_time, 
            json_extract(enriched_data, '$.onchain_price_sol') as onchain_price_sol, 
            json_extract(enriched_data, '$.onchain_volume_sol') as onchain_volume_sol,
            json_extract(enriched_data, '$.swap_token_in') as swap_token_in,
            json_extract(enriched_data, '$.swap_token_out') as swap_token_out
        FROM 
            transactions
        WHERE 
            (
                json_extract(enriched_data, '$.swap_token_in') = ? OR 
                json_extract(enriched_data, '$.swap_token_out') = ?
            )
            AND json_extract(enriched_data, '$.onchain_price_sol') IS NOT NULL
            AND json_extract(enriched_data, '$.onchain_volume_sol') IS NOT NULL
        ORDER BY 
            block_time ASC
    """, (token_address, token_address))
    
    transactions = cursor.fetchall()
    conn.close()

    if len(transactions) < 2:
        logging.info("Недостаточно транзакций с ценовыми данными для анализа.")
        return []

    logging.info(f"Найдено {len(transactions)} транзакций с ценовыми данными для анализа.")

    dumps_found = []
    previous_tx = transactions[0]

    for i in range(1, len(transactions)):
        current_tx = transactions[i]
        
        previous_price = float(previous_tx['onchain_price_sol'])
        current_price = float(current_tx['onchain_price_sol'])
        volume = float(current_tx['onchain_volume_sol'])

        if is_significant_dump(current_price, previous_price, volume):
            price_drop_percentage = round(100 * (1 - current_price / previous_price), 2)
            dump_info = {
                "signature": current_tx['signature'],
                "block_time": current_tx['block_time'],
                "token_address": token_address,
                "price_drop_percent": price_drop_percentage,
                "previous_price_sol": previous_price,
                "dump_price_sol": current_price,
                "volume_sol": volume,
            }
            dumps_found.append(dump_info)
            logging.warning(f"""!!! НАЙДЕН ДАМП !!!
    - Сигнатура: {dump_info['signature']}
    - Время: {dump_info['block_time']}
    - Падение цены: {dump_info['price_drop_percent']}%
    - Цена до/после: {dump_info['previous_price_sol']:.6f} -> {dump_info['dump_price_sol']:.6f} SOL
    - Объем: {dump_info['volume_sol']:.2f} SOL""")

        previous_tx = current_tx
        
    logging.info(f"Анализ завершен. Найдено дампов: {len(dumps_found)}.")
    return dumps_found

if __name__ == '__main__':
    # Пример использования:
    # Замените на адрес токена, который вы хотите проанализировать
    # TOKEN_TO_ANALYZE = "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263" # BONK
    TOKEN_TO_ANALYZE = "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm" # WIF
    
    if not TOKEN_TO_ANALYZE:
        print("Пожалуйста, укажите адрес токена в переменной TOKEN_TO_ANALYZE")
    else:
        find_dumps_for_token(TOKEN_TO_ANALYZE) 