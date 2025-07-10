"""
Централизованный реестр экзотических токенов для системы анализа.
Создан по рекомендации архитектора для консолидации информации о токенах с особым поведением.
"""
from typing import Dict, Any

# Основной реестр экзотических токенов
EXOTIC_TOKEN_REGISTRY: Dict[str, Dict[str, Any]] = {
    # Fee-on-transfer токены
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {  # USDC (некоторые версии)
        "behavior": "fee_on_transfer",
        "fee_percentage": 0.0,  # Обычно 0, но может изменяться
        "description": "USDC with potential fee-on-transfer behavior",
        "decimals": 6
    },
    
    # Rebase токены
    "mSoLzYCxHdYgdzU16g5QSh3i5K3z3KZK7ytfqcJm7So": {  # mSOL (Marinade)
        "behavior": "rebase",
        "description": "Marinade staked SOL - rebase token",
        "decimals": 9,
        "rebase_mechanism": "stake_rewards"
    },
    
    "7dHbWXmci3dT8UFYWYZweBLXgycu7Y3iL6trKn1Y7ARj": {  # stSOL (Lido)
        "behavior": "rebase", 
        "description": "Lido staked SOL - rebase token",
        "decimals": 9,
        "rebase_mechanism": "stake_rewards"
    },
    
    # Deflationary токены
    "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R": {  # RAY
        "behavior": "deflationary",
        "description": "Raydium token with burn mechanics",
        "decimals": 6,
        "burn_rate": "variable"
    },
    
    # Примеры других экзотических токенов
    "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {  # USDT
        "behavior": "standard",  # Но может иметь особенности
        "description": "Tether USD",
        "decimals": 6,
        "notes": "May have blacklist functionality"
    }
}

# Категории поведения токенов
TOKEN_BEHAVIOR_CATEGORIES = {
    "fee_on_transfer": {
        "description": "Токены, которые взимают комиссию при переводе",
        "calculation_method": "balance_based",  # Расчет только по балансам
        "special_handling": True
    },
    
    "rebase": {
        "description": "Токены с автоматическим изменением баланса",
        "calculation_method": "balance_based",
        "special_handling": True,
        "balance_changes": "automatic"
    },
    
    "deflationary": {
        "description": "Токены с механизмом сжигания",
        "calculation_method": "balance_based",
        "special_handling": True,
        "supply_changes": "decreasing"
    },
    
    "standard": {
        "description": "Стандартные SPL токены",
        "calculation_method": "standard",
        "special_handling": False
    }
}

# Функции для работы с реестром
def is_exotic_token(mint: str) -> bool:
    """
    Проверяет, является ли токен экзотическим.
    
    Args:
        mint: Адрес mint токена
        
    Returns:
        True, если токен экзотический
    """
    return mint in EXOTIC_TOKEN_REGISTRY

def get_token_behavior(mint: str) -> str:
    """
    Возвращает тип поведения токена.
    
    Args:
        mint: Адрес mint токена
        
    Returns:
        Тип поведения токена
    """
    token_info = EXOTIC_TOKEN_REGISTRY.get(mint, {})
    return token_info.get("behavior", "standard")

def get_token_info(mint: str) -> Dict[str, Any]:
    """
    Возвращает полную информацию о токене.
    
    Args:
        mint: Адрес mint токена
        
    Returns:
        Словарь с информацией о токене
    """
    return EXOTIC_TOKEN_REGISTRY.get(mint, {
        "behavior": "standard",
        "description": "Unknown token",
        "special_handling": False
    })

def requires_special_handling(mint: str) -> bool:
    """
    Проверяет, требует ли токен специальной обработки.
    
    Args:
        mint: Адрес mint токена
        
    Returns:
        True, если требуется специальная обработка
    """
    behavior = get_token_behavior(mint)
    category_info = TOKEN_BEHAVIOR_CATEGORIES.get(behavior, {})
    return category_info.get("special_handling", False)

def get_calculation_method(mint: str) -> str:
    """
    Возвращает рекомендуемый метод расчета для токена.
    
    Args:
        mint: Адрес mint токена
        
    Returns:
        Метод расчета ("balance_based" или "standard")
    """
    behavior = get_token_behavior(mint)
    category_info = TOKEN_BEHAVIOR_CATEGORIES.get(behavior, {})
    return category_info.get("calculation_method", "standard")

# Константы для быстрого доступа
EXOTIC_TOKEN_MINTS = set(EXOTIC_TOKEN_REGISTRY.keys())
FEE_ON_TRANSFER_TOKENS = {
    mint for mint, info in EXOTIC_TOKEN_REGISTRY.items() 
    if info.get("behavior") == "fee_on_transfer"
}
REBASE_TOKENS = {
    mint for mint, info in EXOTIC_TOKEN_REGISTRY.items() 
    if info.get("behavior") == "rebase"
}
DEFLATIONARY_TOKENS = {
    mint for mint, info in EXOTIC_TOKEN_REGISTRY.items() 
    if info.get("behavior") == "deflationary"
} 