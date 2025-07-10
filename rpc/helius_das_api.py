# rpc/helius_das_api.py

import requests
import logging
from typing import Optional

# Используем наш централизованный клиент для управления ключами и ретраями
from .client import RPCClient

logger = logging.getLogger(__name__)

def get_on_chain_tx_count(token_address: str) -> Optional[int]:
    """
    Получает количество транзакций для SPL токена.

    ВАЖНО: Текущая реализация подсчитывает только транзакции связанные 
    с самим минтом токена (создание/уничтожение токенов), но НЕ включает 
    транзакции между Associated Token Accounts (ATA), которые составляют 
    основную массу активности SPL токенов.
    
    Для полного анализа активности SPL токена необходимо также учитывать:
    - Переводы между ATA пользователей  
    - Операции swap в DEX
    - Другие DeFi операции через программы
    
    Args:
        token_address: Адрес минта токена.

    Returns:
        Количество транзакций минта или None в случае ошибки.
    """
    logger.warning(f"ВНИМАНИЕ: Подсчет транзакций для {token_address} НЕ включает ATA переводы!")
    logger.info(f"Запрос кол-ва транзакций минта для токена {token_address} через Helius API...")
    
    rpc_client = RPCClient()
    
    # Используем getSignaturesForAddress с минимальным лимитом для быстрого получения общего количества
    payload = {
        "jsonrpc": "2.0",
        "id": "bant-get-signatures-count",
        "method": "getSignaturesForAddress",
        "params": [
            token_address,
            {
                "limit": 1,  # Минимальный лимит для быстрого ответа
                "commitment": "confirmed"  # Используем confirmed для баланса скорости и точности
            }
        ]
    }
    
    try:
        # Используем _make_request из нашего клиента, который уже умеет делать ретраи
        response_data = rpc_client._make_request(payload)
        
        if response_data and isinstance(response_data, list):
            # Если токен имеет транзакции, нам нужно получить полный список
            # для подсчета. Но делаем это эффективно через пагинацию
            count = 0
            last_signature = None
            
            while True:
                payload_paginated = {
                    "jsonrpc": "2.0",
                    "id": f"bant-get-signatures-page-{count // 1000}",
                    "method": "getSignaturesForAddress",
                    "params": [
                        token_address,
                        {
                            "limit": 1000,  # Максимальный размер страницы
                            "commitment": "confirmed",
                            **({"before": last_signature} if last_signature else {})
                        }
                    ]
                }
                
                page_data = rpc_client._make_request(payload_paginated)
                
                if not page_data or not isinstance(page_data, list):
                    break
                    
                page_count = len(page_data)
                count += page_count
                
                # Если получили меньше максимального размера страницы, значит это последняя страница
                if page_count < 1000:
                    break
                    
                # Обновляем last_signature для следующей итерации
                last_signature = page_data[-1]["signature"]
                
                # Добавляем небольшую задержку между запросами пагинации
                import time
                time.sleep(0.1)
            
            logger.info(f"Подсчитано транзакций минта: {count} для токена {token_address}.")
            return count
            
        elif response_data == []:
            # Пустой список означает, что транзакций нет
            logger.info(f"Токен {token_address} не имеет транзакций.")
            return 0
        else:
            logger.warning(f"Получен неожиданный ответ от Helius API для {token_address}: {type(response_data)}")

    except Exception as e:
        logger.error(f"Критическая ошибка при запросе к Helius API для {token_address}: {e}", exc_info=True)

    return None


def get_comprehensive_spl_token_activity(token_address: str) -> Optional[dict]:
    """
    Получает более полную картину активности SPL токена, включая ATA.
    
    ВНИМАНИЕ: Эта функция экспериментальная и может быть ресурсоемкой 
    для популярных токенов!
    
    Для полного анализа SPL токена нужно:
    1. Найти все Associated Token Accounts (ATA) связанные с этим минтом  
    2. Подсчитать транзакции каждого ATA
    3. Агрегировать всю статистику
    
    Args:
        token_address: Адрес минта токена.

    Returns:
        Словарь с детальной статистикой или None в случае ошибки.
    """
    logger.warning(f"ЭКСПЕРИМЕНТАЛЬНО: Анализ полной активности токена {token_address}")
    
    rpc_client = RPCClient()
    
    try:
        # Шаг 1: Получаем все ATA для данного минта
        logger.info("Поиск всех Associated Token Accounts...")
        
        # Используем getProgramAccounts для поиска всех ATA этого токена
        payload = {
            "jsonrpc": "2.0",
            "id": "bant-get-token-accounts",
            "method": "getProgramAccounts",
            "params": [
                "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",  # SPL Token Program ID
                {
                    "encoding": "jsonParsed",
                    "filters": [
                        {
                            "dataSize": 165  # Размер Token Account
                        },
                        {
                            "memcmp": {
                                "offset": 0,  # Mint находится в начале данных
                                "bytes": token_address
                            }
                        }
                    ]
                }
            ]
        }
        
        response_data = rpc_client._make_request(payload)
        
        if not response_data or not isinstance(response_data, list):
            logger.error("Не удалось получить список ATA")
            return None
        
        total_ata_count = len(response_data)
        logger.info(f"Найдено {total_ata_count} Associated Token Accounts")
        
        # Шаг 2: Для первых нескольких ATA получаем примерную статистику
        sample_size = min(10, total_ata_count)  # Ограничиваем для производительности
        sample_tx_count = 0
        
        for i, account_info in enumerate(response_data[:sample_size]):
            ata_address = account_info["pubkey"]
            logger.info(f"Анализ ATA {i+1}/{sample_size}: {ata_address}")
            
            # Получаем количество транзакций для этого ATA
            ata_payload = {
                "jsonrpc": "2.0",
                "id": f"bant-ata-sigs-{i}",
                "method": "getSignaturesForAddress",
                "params": [
                    ata_address,
                    {"limit": 1}  # Только для проверки наличия транзакций
                ]
            }
            
            ata_response = rpc_client._make_request(ata_payload)
            if ata_response and isinstance(ata_response, list):
                # Упрощенный подсчет для демонстрации
                sample_tx_count += len(ata_response)
            
        # Шаг 3: Получаем статистику минта
        mint_tx_count = get_on_chain_tx_count(token_address) or 0
        
        # Шаг 4: Экстраполируем результаты (очень приблизительно!)
        estimated_total_ata_transactions = (sample_tx_count / sample_size) * total_ata_count if sample_size > 0 else 0
        
        result = {
            "mint_address": token_address,
            "mint_transactions": mint_tx_count,
            "total_ata_accounts": total_ata_count,
            "sampled_ata_accounts": sample_size,
            "sampled_ata_transactions": sample_tx_count,
            "estimated_total_ata_transactions": int(estimated_total_ata_transactions),
            "estimated_total_activity": mint_tx_count + int(estimated_total_ata_transactions),
            "warning": "Это ПРИБЛИЗИТЕЛЬНАЯ оценка! Для точного анализа нужны дополнительные методы."
        }
        
        logger.info(f"Анализ завершен: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Ошибка при анализе полной активности токена {token_address}: {e}", exc_info=True)
        return None 