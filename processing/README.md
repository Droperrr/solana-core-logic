# processing

Модуль основной обработки транзакций, enrichment, QC и записи в БД. Является центральным компонентом пайплайна обработки Solana Data Pipeline.

## Назначение
- Оркестрация полного цикла обработки транзакций: парсинг, enrichment, QC, запись в БД
- Поддержка batch- и single-режимов обработки
- Интеграция с парсерами, enrichment-модулями, QC и базой данных
- Логирование, обработка ошибок, поддержка кастомных сценариев

## Архитектура и ключевые файлы
- `transaction_processor.py` — Главный процессор: функции batch- и single-обработки, enrichment, QC, запись в БД, интеграция с парсерами и QC
- `errors.py` — Кастомные исключения для этапов обработки (TransactionError, SerializationError, DatabaseError, EnrichmentError)
- `test_transaction_processor.py` — Юнит-тесты для процессора (unittest + mock)
- `__init__.py` — Импортирует основные функции/классы для использования как пакет

## Примеры запуска
- Основные функции вызываются из main.py или скриптов верхнего уровня
- Для тестирования: `pytest processing/test_transaction_processor.py` или `python -m unittest processing/test_transaction_processor.py`

## Тесты
- `test_transaction_processor.py` — покрывает ключевые поля enrichment, QC и обработку транзакций (использует mock)
- Для запуска всех тестов: `pytest processing/` или `python -m unittest processing/`

## Best practices
- Весь процесс покрыт логированием и QC, поддержка batch и single mode
- Для новых enrichment- и QC-функций — расширять transaction_processor.py и покрывать тестами
- Все ошибки обрабатываются через кастомные исключения (errors.py)
- Для интеграции с новыми DEX/типами транзакций — использовать соответствующие enrichment-функции из enrich_parser

## Ограничения и TODO
- Нет отдельных интеграционных тестов для batch-режима (только unit для single)
- Для production-использования рекомендуется централизовать конфиги и переменные окружения
- Для сложных сценариев QC рекомендуется расширять QC-логику и тесты

## Ссылки
- [transaction_processor.py](transaction_processor.py) — основной процессор
- [errors.py](errors.py) — кастомные исключения
- [test_transaction_processor.py](test_transaction_processor.py) — тесты 