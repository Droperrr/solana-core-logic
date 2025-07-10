# tests

**Event-based enrichment:** Все тесты теперь валидируют event-based enriched_events (`EnrichedEvent`), flat-структуры, QC, ML-ready. Подробнее — см. [PROJECT_DOC.md](../PROJECT_DOC.md).

Модуль тестов для покрытия всех ключевых компонентов пайплайна.

## Основные папки и файлы:
- `analysis/` — Тесты для аналитики и buy/sell-логики.
- `enrich/` — Тесты для enrichment, edge cases, сравнение с Solscan, event-based enriched events.
- `test_enrich_vs_solscan.py` — Сравнение enrichment с Solscan.

## Event-based enrichment
- Все тесты enrichment теперь проверяют корректность структуры event-based enriched events (`EnrichedEvent`).
- Universal Parser и enrich-функции тестируются на соответствие новой модели событий (event_id, event_type, protocol, token_flows, qc_tags и др.).
- Для новых enrichers обязательно писать unit-тесты для `enrich(instr, raw_tx)` и интеграционные тесты для Universal Parser.

## Best practice
- Все новые функции покрываются тестами, тесты запускаются автоматически. 