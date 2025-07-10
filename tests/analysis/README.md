# tests/analysis

Тесты для проверки buy/sell-аналитики и вспомогательных функций анализа транзакций.

## Назначение
- Проверка корректности buy/sell-детекции и вспомогательных функций анализа (token_transfers, inner_instructions, balances)
- Использование мок-данных для unit-тестирования логики

## Структура
- `test_analyzer_rules.py` — pytest-тесты для функций анализа из analysis.analyzer_rules (BuySellResult, _analyze_from_token_transfers и др.)

## Пример запуска pytest
```bash
pytest tests/analysis/test_analyzer_rules.py
```

## Best practices
- Для каждого нового паттерна или edge-case добавляйте отдельный тест
- Используйте мок-данные, покрывающие buy, sell, unknown сценарии

## Ограничения
- Тесты используют только мок-данные, не интегрируются с реальными транзакциями
- Для сложных сценариев требуется ручное добавление новых кейсов 