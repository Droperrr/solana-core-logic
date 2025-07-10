#!/usr/bin/env python3
"""
RPC Rate Limiter для предотвращения ошибок Helius
"""

import time
import logging
from typing import Callable, Any

logger = logging.getLogger("rpc_rate_limiter")

class RPCRateLimiter:
    def __init__(self, requests_per_second: float = 2.0, retry_attempts: int = 3):
        self.min_interval = 1.0 / requests_per_second
        self.last_request_time = 0
        self.retry_attempts = retry_attempts
    
    def execute_with_rate_limit(self, func: Callable, *args, **kwargs) -> Any:
        """
        Выполняет функцию с ограничением скорости и retry логикой
        """
        for attempt in range(self.retry_attempts):
            try:
                # Ждем нужный интервал между запросами
                current_time = time.time()
                time_since_last = current_time - self.last_request_time
                
                if time_since_last < self.min_interval:
                    sleep_time = self.min_interval - time_since_last
                    logger.info(f"Rate limiting: sleeping for {sleep_time:.2f}s")
                    time.sleep(sleep_time)
                
                # Выполняем запрос
                self.last_request_time = time.time()
                result = func(*args, **kwargs)
                return result
                
            except Exception as e:
                if "32600" in str(e) or "deprioritized" in str(e):
                    if attempt < self.retry_attempts - 1:
                        wait_time = (attempt + 1) * 2  # Экспоненциальный backoff
                        logger.warning(f"RPC error {e}, retrying in {wait_time}s (attempt {attempt + 1})")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"RPC failed after {self.retry_attempts} attempts: {e}")
                        raise
                else:
                    # Другие ошибки пробрасываем сразу
                    raise
        
        return None

# Глобальный экземпляр
rpc_limiter = RPCRateLimiter(requests_per_second=1.5)  # Консервативный лимит 