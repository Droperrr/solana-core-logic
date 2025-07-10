# reporting

Модуль генерации сводных отчётов и логирования итоговой статистики по обработанным данным Solana Data Pipeline.

## Назначение
- Формирование и логирование сводной статистики по batch-обработке транзакций, пулам, ошибкам, распределению типов транзакций
- Используется для аудита, мониторинга прогресса и отладки пайплайна

## Архитектура и ключевые файлы
- `summary.py` — Главный модуль: функция `log_summary_stats` для логирования итоговой статистики по токену, транзакциям, пулам, ошибкам, распределению типов

## Пример использования
```python
from reporting.summary import log_summary_stats
# ... после завершения batch-обработки:
log_summary_stats(
    token_address=token,
    total_tx_limit=1000,
    total_signatures_found_rpc=1200,
    num_new_signatures_processed=950,
    processed_count=900,
    saved_count=890,
    db_errors_count=10,
    pools_found_parser_total=15,
    pools_saved_to_discovered=12,
    pools_upserted_total=10,
    transaction_type_counts={"swap": 800, "transfer": 100},
    discovered_pools_list=[("pool1...", "Raydium", "stable"), ...]
)
```

## Best practices
- Используйте `log_summary_stats` в конце каждой batch-обработки для прозрачного аудита
- Логируйте не только успехи, но и ошибки/edge-cases для последующего анализа
- Для расширения отчётов добавляйте новые поля только после согласования с командой

## Ограничения
- Модуль не содержит CLI и не формирует отдельные файлы-отчёты (только логирование)
- Не предназначен для генерации визуальных дашбордов или сложной аналитики
- Для полноценного аудита используйте совместно с модулями `qc` и `analysis` 