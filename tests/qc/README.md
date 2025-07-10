# tests/qc

Тесты для проверки QC-логики, генерации эталона и diff-отчётов enrichment vs etalon.

## Назначение
- Проверка генерации эталонной модели из raw транзакции
- Сравнение enrichment с эталоном, анализ diff-отчётов и QC-результатов

## Структура
- `test_qc_etalon_and_diff.py` — pytest-тесты для qc.etalon_generator и qc.diff_engine (генерация эталона, сравнение с enrichment)

## Пример запуска pytest
```bash
pytest tests/qc/test_qc_etalon_and_diff.py
```

## Best practices
- Для каждого нового edge-case добавляйте отдельный raw/enrich fixture в fixtures/transactions
- Проверяйте, что enrichment не даёт CRITICAL-расхождений с эталоном

## Ограничения
- Тесты используют только заранее подготовленные фикстуры raw/enrich
- Для сложных edge-cases требуется ручное добавление новых файлов 