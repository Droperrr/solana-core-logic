# rpc

Модуль работы с RPC-интерфейсами Solana и Helius для получения транзакций, сигнатур, токен-аккаунтов и управления API-ключами.

## Назначение
- Запросы к Solana RPC и Helius: получение транзакций, сигнатур, токен-аккаунтов
- Централизованное управление API-ключами, поддержка ротации и fallback
- Логирование сырых данных и проблемных сигнатур для аудита

## Архитектура и ключевые файлы
- `helius_rpc.py` — Основная логика RPC-запросов (getTransaction, getSignaturesForAddress, getProgramAccounts), обработка ошибок, ротация ключей, логирование
- `api_keys.py` — Список API-ключей, функции получения и ротации ключа (`get_current_api_key`, `rotate_api_key`)

## Пример использования
```python
from rpc.helius_rpc import get_transaction, get_signatures_for_address, get_program_accounts_for_mint

# Получить детали транзакции
raw_tx = get_transaction("5N...abc")

# Получить сигнатуры для адреса
sigs = get_signatures_for_address("So111...111", limit=10)

# Получить все ATA для минта
atas = get_program_accounts_for_mint("So111...111")
```

## Best practices
- Все ключи централизованы в `api_keys.py`, поддерживается ротация при rate limit/ошибках
- Для аудита используйте логирование сырых данных (`log_helius_raw_file`) и проблемных сигнатур
- Не храните реальные ключи в публичном репозитории, используйте переменные окружения или .env для production

## Ограничения
- Модуль не содержит CLI, только Python API
- Для работы требуется валидный API-ключ Helius (или Solana RPC)
- Не реализует автоматическую обработку всех edge-cases RPC (например, нестабильные ответы, частые rate limits)
- Для production рекомендуется расширить обработку ошибок и добавить мониторинг использования ключей 