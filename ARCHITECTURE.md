# Архитектурные принципы enrichment и Golden Dataset

## Приоритеты паттернов для Golden Dataset и разработки обогатителей
1. **Single-Hop Swaps** (Raydium, Orca)
2. **SPL Token Transfers** (transfer, transferChecked)
3. **Account Management** (createAssociatedTokenAccount, closeAccount)
4. **Aggregator Swaps** (Jupiter)
5. **Liquidity Provisioning** (deposit/withdraw Raydium, open_position Orca)
6. **Metaplex Operations** (createMetadataAccountV3)
7. **Bridging (мосты)**

## Эталонная структура enrich-модели (Pydantic EnrichmentContext)
- `transaction_metadata`: сигнатура, время, статус
- `token_flows`: универсальный список всех движений токенов (IN/OUT)
- `protocol_details`: список взаимодействий с протоколами (расширяемый)
- `parser_version`: версия обогатителя
- `qc_context`: QC-информация

## Организация кода
- Каждый протокол — отдельный enricher-модуль (`enrich_parser/enrich/<protocol>.py`)
- UniversalParser — диспетчер, вызывает enricher по program_id

## Fallback/Unknown
- Логирование и enrich с пометкой "Unknown"
- Запись в protocol_details: `{ "protocol_name": "Unknown", ... }`
- Алертинг по количеству unknown-паттернов

## Best practices по тестированию и поддержанию Golden Dataset
- Golden Dataset — единственный источник истины для регрессионных тестов
- Каждый тест — атомарный, покрывает один аспект
- Edge-кейсы и ошибки из продакшена — сразу в Golden Dataset
- Только автоматизированное управление фикстурами (manage_fixtures.py)
- Каждая сигнатура снабжена комментарием о покрываемом кейсе 

## Стандарты именования констант Program ID

Все константы, содержащие Program ID, должны именоваться по шаблону `PROTOCOL_NAME_PROGRAM_ID`. Избыточные суффиксы, такие как `_STR` или `_ID_STR`, не используются.

**Пример:**
- RAYDIUM_V4_PROGRAM_ID
- JUPITER_V6_PROGRAM_ID 