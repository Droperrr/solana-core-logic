import threading
import time
import logging
import requests
from typing import Optional, Dict, Any, List
import random
from pydantic import BaseModel
import config.config as app_config

# --- Pydantic-модель для нормализации ответа ---
class CanonicalRawTransaction(BaseModel):
    # TODO: определить поля согласно стандарту Solana JSON-RPC
    raw_json: dict
    rpc_source: str

# --- RateLimiter ---
class RateLimiter:
    """
    Простой и надежный ограничитель скорости, основанный на минимальном интервале между запросами.
    """
    def __init__(self, rate_per_sec: int):
        self.rate_per_sec = max(1, rate_per_sec)
        self.period = 1.0 / self.rate_per_sec
        self.lock = threading.Lock()
        self.last_request_time = 0
        self.logger = logging.getLogger("rpc.ratelimiter")

    def acquire(self):
        with self.lock:
            now = time.monotonic()
            time_since_last = now - self.last_request_time
            if time_since_last < self.period:
                sleep_time = self.period - time_since_last
                self.logger.info(f"Rate limit: sleeping for {sleep_time:.3f} seconds to maintain {self.rate_per_sec} req/s.")
                time.sleep(sleep_time)
            self.last_request_time = time.monotonic()

# --- RPCClient with Enterprise Retry Mechanism ---
class RPCClient:
    # Теперь у нас только один провайдер
    PROVIDER_NAME = 'helius'
    BASE_URL = 'https://mainnet.helius-rpc.com'
    
    # Retry configuration
    MAX_RETRIES = 5
    BASE_DELAY = 1.0  # секунд
    MAX_DELAY = 30.0  # секунд
    EXPONENTIAL_BASE = 2

    def __init__(self):
        self.api_keys = [k for k in app_config.HELIUS_API_KEYS if k]
        self.key_index = 0
        self.rate_limiter = RateLimiter(rate_per_sec=9)  # Установлен лимит 9 req/s
        self.logger = logging.getLogger("rpc.client.helius")
        if not self.api_keys:
            self.logger.critical("Ключи Helius API не найдены в конфигурации! Пайплайн не сможет работать.")

    def _get_next_key(self):
        self.key_index = (self.key_index + 1) % len(self.api_keys)
        return self.api_keys[self.key_index]

    def _exponential_backoff_delay(self, attempt: int) -> float:
        """Вычисляет задержку с exponential backoff и jitter."""
        delay = min(self.BASE_DELAY * (self.EXPONENTIAL_BASE ** attempt), self.MAX_DELAY)
        # Добавляем jitter (±20%) для избежания thundering herd
        jitter = delay * 0.2 * (random.random() - 0.5)
        return delay + jitter

    def _is_retryable_error(self, error: Exception) -> bool:
        """Определяет, стоит ли повторить запрос при данной ошибке."""
        if isinstance(error, requests.exceptions.ConnectionError):
            return True
        if isinstance(error, requests.exceptions.Timeout):
            return True
        if isinstance(error, requests.exceptions.ChunkedEncodingError):
            return True
        if isinstance(error, requests.exceptions.RequestException):
            # Проверяем коды ошибок
            if hasattr(error, 'response') and error.response is not None:
                status_code = error.response.status_code
                # Retry на временные ошибки сервера
                if status_code in [502, 503, 504, 520, 521, 522, 524]:
                    return True
            return True  # По умолчанию retry для других RequestException
        return False

    def _make_request(self, payload: dict) -> Optional[Any]:
        if not self.api_keys:
            self.logger.error("Нет доступных API ключей для выполнения запроса")
            return None
        
        method_name = payload.get('method', 'unknown')
        original_key_index = self.key_index
        
        # Попробуем все ключи с retry logic
        for key_attempt in range(len(self.api_keys)):
            api_key = self.api_keys[self.key_index]
            
            # Retry logic для текущего ключа
            for retry_attempt in range(self.MAX_RETRIES):
                self.rate_limiter.acquire()  # Ограничиваем частоту запросов
                url = f"{self.BASE_URL}?api-key={api_key}"
                
                try:
                    resp = requests.post(url, json=payload, timeout=30)
                    
                    # Обрабатываем rate limit (429) - переключаемся на следующий ключ
                    if resp.status_code == 429:
                        self.logger.warning(f"429 Rate limit для ключа {self.key_index+1}/{len(self.api_keys)} при выполнении {method_name}, переключаемся на следующий ключ...")
                        break  # Выходим из retry loop для этого ключа
                    
                    resp.raise_for_status()
                    json_resp = resp.json()
                    
                    if "error" in json_resp:
                        error_code = json_resp.get('error', {}).get('code', 'unknown')
                        error_message = json_resp.get('error', {}).get('message', 'unknown')
                        self.logger.error(f"RPC Error от Helius при выполнении {method_name}: код {error_code}, сообщение: {error_message}")
                        return None
                    
                    # Успешный запрос
                    if retry_attempt > 0:
                        self.logger.info(f"✅ Запрос {method_name} успешен после {retry_attempt} повторных попыток")
                    return json_resp.get('result')
                    
                except requests.RequestException as e:
                    is_retryable = self._is_retryable_error(e)
                    
                    if retry_attempt < self.MAX_RETRIES - 1 and is_retryable:
                        delay = self._exponential_backoff_delay(retry_attempt)
                        self.logger.warning(
                            f"🔄 Попытка {retry_attempt + 1}/{self.MAX_RETRIES} для {method_name} неудачна: {e}. "
                            f"Повтор через {delay:.1f}с..."
                        )
                        time.sleep(delay)
                        continue
                    else:
                        # Исчерпаны попытки retry для этого ключа или ошибка не retryable
                        if is_retryable:
                            self.logger.error(
                                f"❌ Все {self.MAX_RETRIES} попыток для {method_name} с ключом {self.key_index+1}/{len(self.api_keys)} исчерпаны. "
                                f"Последняя ошибка: {e}"
                            )
                        else:
                            self.logger.error(f"❌ Неповторяемая ошибка для {method_name}: {e}")
                        break  # Выходим из retry loop для этого ключа
                
                except Exception as e:
                    self.logger.error(f"❌ Неожиданная ошибка при выполнении {method_name}: {e}")
                    break  # Выходим из retry loop для этого ключа
            
            # Переключаемся на следующий ключ
            self._get_next_key()
            
            # Если мы прошли полный круг по всем ключам, выходим
            if self.key_index == original_key_index:
                break
        
        self.logger.error(f"🚨 КРИТИЧЕСКАЯ ОШИБКА: Не удалось выполнить {method_name} после попыток со всеми {len(self.api_keys)} ключами")
        return None

    def get_transaction(self, signature: str) -> Optional[dict]:
        payload = {
            "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
            "params": [signature, {"encoding": "json", "maxSupportedTransactionVersion": 0}]
        }
        result = self._make_request(payload)
        if result:
            result['rpc_source'] = self.PROVIDER_NAME
        return result

    def get_signatures_for_address(self, address: str, limit: int = 1000, before: Optional[str] = None) -> Optional[List[Dict]]:
        params = {"limit": limit}
        if before: params["before"] = before
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getSignaturesForAddress", "params": [address, params]}
        return self._make_request(payload)
    
    def get_program_accounts(self, program_address: str, mint_address: str = None, filters: List[Dict[str, Any]] = None) -> Optional[List[Dict]]:
        """
        Fetch program accounts with optional custom filters. If filters is None, use the legacy behavior (by mint_address at offset 0, dataSize 165).
        """
        if filters is not None:
            config = {
                "encoding": "base64",
                "filters": filters
            }
        else:
            config = {
                "encoding": "jsonParsed",
                "filters": [
                    {"memcmp": {"offset": 0, "bytes": mint_address}},  # Исправлено: offset 0, не 32
                    {"dataSize": 165}
                ]
            }
        payload = {"jsonrpc": "2.0", "id": 1, "method": "getProgramAccounts", "params": [program_address, config]}
        return self._make_request(payload) 