# reporting/summary.py

import logging
from typing import Optional, Dict, List, Tuple, Any

logger = logging.getLogger("reporting.summary")

def log_summary_stats(
    token_address: str,
    total_tx_limit: Optional[int],
    total_signatures_found_rpc: int,
    num_new_signatures_processed: int,
    processed_count: int,
    saved_count: int,
    db_errors_count: int,
    pools_found_parser_total: int,
    # Изменяем имя параметра для ясности, т.к. фильтрации нет
    pools_saved_to_discovered: int,
    pools_upserted_total: int, # Этот счетчик остается важным
    transaction_type_counts: Dict[str, int],
    discovered_pools_list: List[Tuple[str, Optional[str], Optional[str]]]
):
    """Логирует итоговую статистику выполнения Фазы 1."""
    logger.info("\n--- Итоговая статистика (Фаза 1) ---")
    logger.info(f" Токен:                          {token_address}")
    logger.info(f" Лимит транзакций:               {total_tx_limit if total_tx_limit is not None else 'НЕТ'}")
    logger.info(f" Всего собрано сигнатур (RPC):   {total_signatures_found_rpc}")
    logger.info(f" Новых сигнатур для обработки:   {num_new_signatures_processed}")
    logger.info(f" Успешно обработано (парсер):    {processed_count}")
    logger.info(f" Успешно сохранено в БД:         {saved_count}")
    logger.info(f" Ошибок сохранения в БД:         {db_errors_count}")
    logger.info(f" Всего найдено пулов (парсером): {pools_found_parser_total}")
    # Используем новое имя параметра
    logger.info(f" Передано на сохранение в disc.: {pools_saved_to_discovered}")
    logger.info(f" Успешно UPSERT'ов пулов в БД:   {pools_upserted_total}")
    logger.info(f" Распределение по типам транзакций:")
    if transaction_type_counts:
        sorted_types = sorted(transaction_type_counts.items(), key=lambda item: item[1], reverse=True)
        max_len_type = max(len(k) for k in transaction_type_counts.keys()) if transaction_type_counts else 10
        for tx_type_key, count in sorted_types:
            logger.info(f"  - {tx_type_key:<{max_len_type + 2}} : {count}")
    else:
        logger.info("  (нет данных)")
    
    # --- >>> Новый блок: Логирование списка найденных пулов <<< ---
    logger.info("\n--- Обнаруженные уникальные пулы (Фаза 1) ---")
    if discovered_pools_list:
        logger.info(f" Найдено уникальных адресов пулов: {len(discovered_pools_list)}")
        # Определяем ширину колонок для форматирования
        addr_width = 44  # Адреса Solana обычно до 44 символов
        dex_width = 10   # Ширина для DEX ID
        type_width = 15  # Ширина для типа пула
        header = f"  {'Pool Address':<{addr_width}} | {'DEX ID':<{dex_width}} | {'Pool Type':<{type_width}}"
        logger.info(header)
        logger.info("  " + "-" * (addr_width + dex_width + type_width + 6)) # Разделитель
        for pool_addr, dex_id, pool_type in discovered_pools_list:
            dex_str = str(dex_id) if dex_id else "N/A"
            type_str = str(pool_type) if pool_type else "N/A"
            logger.info(f"  {pool_addr:<{addr_width}} | {dex_str:<{dex_width}} | {type_str:<{type_width}}")
        logger.info("  (Примечание: адреса хранилищ токенов A/B будут определены в Фазе 2)")
    else:
        logger.info("  Уникальные пулы не были обнаружены в обработанных транзакциях.")
    logger.info("-" * 60) # Общий разделитель в конце

# --- Конец файла ---