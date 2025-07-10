# tests/enrich

Тесты для проверки enrichment-логики, QC edge-cases и сравнения с эталонными результатами.

## Назначение
- Проверка корректности enrichment для Raydium, Metaplex, edge-case транзакций
- Сравнение enrichment с внешними источниками (Solscan, Metaplex)
- Тестирование QC edge-cases: ошибки, нестыковки, сложные сценарии

## Структура
- `test_enrich_metaplex_vs_solscan.py` — сравнение enrichment Metaplex с Solscan reference
- `test_raydium.py` — ручной тест enrichment Raydium swap (логирование результатов)
- `test_raydium_price_impact.py` — pytest-тесты для Raydium price impact, QC edge-cases (mint mismatch, vault mismatch, CLMM, ошибки баланса)
- `zzz_test.py` — тестирование save_parsed_transaction с GMGN fee (ручной dummy-тест)
- `test_dataclasses_import.py` — sanity-check на импорт dataclasses
- `problematic_enrich_dumps/` — директория для хранения edge-case dumps (пустая или пополняется вручную)

## Пример запуска pytest
```bash
pytest tests/enrich/test_raydium_price_impact.py
```

## Best practices
- Для каждого нового edge-case добавляйте отдельный тест
- Используйте реальные raw/enrich фикстуры из fixtures/transactions
- Для сложных сценариев используйте логирование и ручную проверку

## Ограничения
- Не все тесты автоматизированы: часть требует ручного запуска и анализа логов
- Для некоторых тестов нужны внешние файлы (фикстуры, dumps)
- Коллекция edge-case dumps пополняется вручную 