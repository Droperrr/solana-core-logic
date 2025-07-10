# analysis/analyzer.py

import logging
import re
from analysis.analyzer_rules import (
    BuySellResult,
    _analyze_from_token_transfers,
    _analyze_from_inner_instructions,
    _analyze_from_balances
)

# Константы, перенесенные из main.py
SPL_TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
WSOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
# SOL_MINT = "11111111111111111111111111111111" # Нативный SOL обычно обрабатывается через изменения баланса SOL, а не как токен
JUP_MINT = "JUP4Dx7Rm94e6WGk3mF91J4FQFack2Vtrh3fW4DjZzfE"
# Используем set для быстрого поиска
QUOTE_TOKENS = {WSOL_MINT, USDC_MINT, USDT_MINT, JUP_MINT} # Убрали SOL_MINT

# Типы инструкций SPL Token
SPL_TRANSFER_TYPES = {'transfer', 'transferChecked'}

# Вспомогательная функция (если еще не используется глобально)
def _safe_get(data_dict, key_path, default=None):
    """
    Безопасно извлекает вложенное значение из словаря или списка по пути 'key1.key2.0.key3'.
    """
    # (можно скопировать реализацию из db_writer.py, если она еще не в общем модуле utils)
    keys = key_path.split('.')
    val = data_dict
    try:
        for key in keys:
            if isinstance(val, list):
                try:
                    key_idx = int(key)
                    if not 0 <= key_idx < len(val): return default
                    val = val[key_idx]
                except (ValueError, TypeError):
                     return default
            elif isinstance(val, dict):
                val = val.get(key)
            else: return default
            if val is None: return default
        return val
    except Exception:
        return default

def determine_buy_sell(transaction_details: dict, target_token_mint: str) -> BuySellResult | None:
    """
    Определяет покупку/продажу целевого токена, используя декомпозированные методы анализа.
    Возвращает BuySellResult или None.
    """
    # 1. Анализ по token_transfers
    result = _analyze_from_token_transfers(transaction_details)
    if result:
        return result
    # 2. Анализ по inner_instructions
    result = _analyze_from_inner_instructions(transaction_details)
    if result:
        return result
    # 3. Анализ по балансу
    result = _analyze_from_balances(transaction_details)
    if result:
        return result
    return None