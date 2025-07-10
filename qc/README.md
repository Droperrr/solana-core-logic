# qc

Модуль автоматического контроля качества (QC) enrichment-данных и инфраструктуры Solana Data Pipeline.

## Назначение
- Автоматизация QC enrichment: поиск ошибок, дубликатов, orphan-объектов, enrichment-ошибок
- Генерация diff-отчётов и эталонов для ML/аналитики
- CLI-инструменты для batch-валидации, экспорта, аудита и визуализации QC
- Каталог QC-тегов и best practices для edge-case анализа

## Архитектура и ключевые файлы/папки
- `checks.py` — QC-проверки: структура транзакций, пропуски, дубликаты, orphan-объекты, enrichment-ошибки
- `reporter.py` — Агрегация и запуск QC-проверок (run_all_checks)
- `models.py` — Pydantic-модели эталона, swap_summary, token_flows, confidence
- `diff_engine.py` — Сравнение эталона и enrichment, генерация diff-отчётов с severity
- `etalon_generator.py` — Генерация эталонных моделей из raw-транзакций
- `export_fixture.py` — CLI: экспорт enrichment-фрагментов из БД для тестов/валидации
- `run_batch_validation.py` — CLI: batch-валидация enrichment против эталона, генерация diff-отчётов
- `validate_transaction.py` — CLI: сравнение одного enrichment с эталоном, подробный diff-отчёт
- `audit_codebase.py` — CLI: meta-QC — аудит наличия QC-тегов в enrichment-коде
- `report_dashboard.py`, `dashboard.py` — HTML/Streamlit-дашборды QC-метрик и отчётов
- `qc_tags_catalog.md` — Каталог QC-тегов, severity, описание edge-cases
- `run_audit.py` — CLI: аудит QC и wallet_links, вывод статистики
- `cli_template.py` — шаблон для новых CLI-инструментов

## Примеры запуска CLI
- Валидация одного enrichment:
  ```bash
  python -m qc.validate_transaction --raw path/to/tx.raw.json --enrich path/to/tx.enrich.json
  ```
- Batch-валидация:
  ```bash
  python -m qc.run_batch_validation --file sigs.txt --raw-dir raw/ --enrich-dir enrich/ --out-dir diffs/
  ```
- Экспорт enrichment-фрагментов:
  ```bash
  python -m qc.export_fixture --signature <sig> --platform raydium_enrich --output-dir out/
  ```
- Генерация HTML-дашборда:
  ```bash
  python -m qc.report_dashboard
  ```
- Аудит наличия QC-тегов в except-блоках:
  ```bash
  python -m qc.audit_codebase
  ```

## Тесты
- Тесты QC-логики и enrichment сравнения — в `tests/enrich/` (см. test_raydium.py, test_enrich_metaplex_vs_solscan.py и др.)
- Для ручной проверки используйте CLI-инструменты выше

## Best practices
- Все enrichment-ошибки должны сопровождаться QC-тегами (см. audit_codebase.py)
- Каталог QC-тегов (`qc_tags_catalog.md`) регулярно пополняется и проходит code review
- Для новых edge-cases добавляйте тесты и описания в каталог

## Ограничения
- QC-проверки ориентированы на структуру и enrichment-теги, не покрывают все бизнес-правила
- Для корректной работы CLI требуется настроенный доступ к БД и фикстурам
- Дашборды требуют наличия логов/отчётов и корректной структуры каталогов 