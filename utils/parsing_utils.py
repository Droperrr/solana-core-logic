"""
Утилиты для парсинга транзакций Solana.
Содержит функции для работы с инструкциями, балансами токенов и переводами.
"""

from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass, asdict, is_dataclass
import logging

logger = logging.getLogger(__name__)

@dataclass
class TokenTransfer:
    """Представляет перевод токенов между аккаунтами."""
    source: Optional[Dict[str, Any]] = None
    destination: Optional[Dict[str, Any]] = None
    amount: Optional[int] = None

@dataclass
class Asset:
    """Представляет актив (токен)."""
    address: str
    symbol: str = "UNKNOWN"
    decimals: int = 0

@dataclass
class Amount:
    """Представляет количество токенов."""
    amount: int
    amount_human: float
    asset: Asset

def to_serializable_dict(obj):
    """
    Рекурсивно конвертирует дата-класс или объект в словарь.
    
    Args:
        obj: Объект для сериализации
        
    Returns:
        Словарь, совместимый с JSON сериализацией
    """
    if is_dataclass(obj):
        return asdict(obj)
    if isinstance(obj, list):
        return [to_serializable_dict(i) for i in obj]
    if isinstance(obj, dict):
        return {k: to_serializable_dict(v) for k, v in obj.items()}
    # Добавляем обработку других кастомных типов, если потребуется
    if hasattr(obj, '__dict__'):
        # Для объектов с атрибутами пытаемся извлечь их
        return {k: to_serializable_dict(v) for k, v in obj.__dict__.items() if not k.startswith('_')}
    return obj

def get_inner_instruction_transfers(instruction_index: int, inner_instructions: List[Dict]) -> List[TokenTransfer]:
    """
    Извлекает переводы из inner_instructions для конкретной инструкции.
    
    Args:
        instruction_index: Индекс родительской инструкции
        inner_instructions: Список inner_instructions из метаданных транзакции
        
    Returns:
        Список переводов токенов
    """
    transfers = []
    
    for inner_group in inner_instructions:
        if inner_group.get("index") == instruction_index:
            instructions = inner_group.get("instructions", [])
            for instr in instructions:
                # Простая логика извлечения переводов
                # В реальной реализации здесь будет более сложная логика
                if "parsed" in instr and instr["parsed"].get("type") == "transfer":
                    parsed = instr["parsed"]["info"]
                    transfer = TokenTransfer(
                        source={"address": parsed.get("source"), "owner": parsed.get("source")},
                        destination={"address": parsed.get("destination"), "owner": parsed.get("destination")},
                        amount=int(parsed.get("amount", 0))
                    )
                    transfers.append(transfer)
    
    return transfers

def find_token_balance_change(
    account_address: str, 
    pre_balances: List[Dict], 
    post_balances: List[Dict], 
    change_type: str
) -> Optional[Amount]:
    """
    Находит изменение баланса токена для конкретного аккаунта.
    
    Args:
        account_address: Адрес аккаунта
        pre_balances: Балансы до транзакции
        post_balances: Балансы после транзакции
        change_type: Тип изменения ("increase" или "decrease")
        
    Returns:
        Объект Amount с информацией об изменении
    """
    pre_amount = 0
    post_amount = 0
    mint = None
    decimals = 0
    
    # Находим баланс до транзакции
    for balance in pre_balances:
        if balance.get("accountIndex") == account_address or balance.get("mint") == account_address:
            pre_amount = int(balance.get("uiTokenAmount", {}).get("amount", "0"))
            mint = balance.get("mint")
            decimals = balance.get("uiTokenAmount", {}).get("decimals", 0)
            break
    
    # Находим баланс после транзакции
    for balance in post_balances:
        if balance.get("accountIndex") == account_address or balance.get("mint") == account_address:
            post_amount = int(balance.get("uiTokenAmount", {}).get("amount", "0"))
            if not mint:
                mint = balance.get("mint")
            if not decimals:
                decimals = balance.get("uiTokenAmount", {}).get("decimals", 0)
            break
    
    # Рассчитываем изменение
    change = post_amount - pre_amount
    
    # Проверяем тип изменения
    if change_type == "increase" and change <= 0:
        return None
    if change_type == "decrease" and change >= 0:
        return None
    
    # Создаем объекты
    asset = Asset(address=mint or account_address, decimals=decimals)
    amount_human = change / (10 ** decimals) if decimals > 0 else change
    
    return Amount(
        amount=abs(change),
        amount_human=amount_human,
        asset=asset
    )

def get_asset_from_token_account(account_address: str, token_balances: List[Dict]) -> Optional[Asset]:
    """
    Получает информацию об активе из баланса токенов.
    
    Args:
        account_address: Адрес аккаунта токена
        token_balances: Список балансов токенов
        
    Returns:
        Объект Asset или None
    """
    for balance in token_balances:
        if balance.get("accountIndex") == account_address or balance.get("mint") == account_address:
            mint = balance.get("mint")
            decimals = balance.get("uiTokenAmount", {}).get("decimals", 0)
            return Asset(address=mint, decimals=decimals)
    return None

def get_token_address_from_account(account_address: str, token_balances: List[Dict]) -> Optional[str]:
    """
    Получает адрес токена из аккаунта.
    
    Args:
        account_address: Адрес аккаунта
        token_balances: Список балансов токенов
        
    Returns:
        Адрес токена или None
    """
    for balance in token_balances:
        if balance.get("accountIndex") == account_address:
            return balance.get("mint")
    return None 