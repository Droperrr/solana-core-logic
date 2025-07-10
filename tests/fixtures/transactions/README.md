# Тестовые транзакции (fixtures)

В этой директории хранятся JSON-дампы реальных Solana-транзакций для тестирования QC, enrichment и парсинга.

## Назначение
- Используются для ручного и автоматического тестирования enrichment, QC, сравнения с эталоном
- Покрывают edge-cases, неуспешные транзакции, сложные swap-ы и стандартные сценарии

## Формат файлов
- Каждый файл — отдельная транзакция
- Формат имени: `<dex>_<shortsig>.<type>.json`
  - `<dex>` — raydium, jupiter, orca, fail, edge
  - `<shortsig>` — сокращённая сигнатура транзакции
  - `<type>` — raw (оригинальный raw_json), enrich (результат enrichment)
- Пример: `raydium_2WjgSSiE.raw.json`, `raydium_2WjgSSiE.enrich.json`

## Категории
- `raydium_*.json` — простые и edge-case Raydium swaps
- `jupiter_*.json` — Jupiter multi-hop, aggregator, ошибки
- `orca_*.json` — Orca single/multi-hop, ошибки
- `fail_*.json` — неуспешные транзакции
- `edge_*.json` — сложные случаи (несколько swap-ов, необычные токен-флоу)

## Пример шаблона raw_json
```json
{
  "slot": 123456789,
  "blockTime": 1700000000,
  "meta": { ... },
  "transaction": { ... }
}
```

> Для добавления новой фикстуры: скопируйте raw_json из Solana RPC/Helius и сохраните по шаблону. Для enrich-версии используйте результат enrichment pipeline.

## Best practices
- Для каждого edge-case добавляйте как raw, так и enrich версию транзакции
- Используйте короткие, но уникальные имена файлов для удобства поиска
- Описывайте найденные edge-cases и ошибки в QC-отчётах

## Ограничения
- Не все edge-cases покрыты — пополняйте коллекцию по мере обнаружения новых сценариев
- Структура raw_json должна строго соответствовать формату Solana RPC/Helius 