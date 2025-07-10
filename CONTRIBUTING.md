# CONTRIBUTING.md

## Кодировка файлов

**Важно:** Все файлы в проекте должны быть в кодировке UTF-8 без BOM.

- Для проверки кодировки файлов используйте скрипт:
  ```bash
  python scripts/check_file_encodings.py
  ```
- Для исправления кодировки файлов используйте скрипт:
  ```bash
  python scripts/fix_file_encodings.py
  ```

При создании новых файлов убедитесь, что ваш редактор настроен на использование UTF-8 без BOM. Для Visual Studio Code добавьте в settings.json:
```json
{
    "files.encoding": "utf8",
    "files.autoGuessEncoding": false
}
```

В CI/CD пайплайне настроена автоматическая проверка кодировки файлов. Pull Request с файлами в некорректной кодировке будет отклонен.

## Developer Workflow

1. **Работа пайплайна:** Запускайте основной пайплайн (`main.py`) для сбора и обогащения данных.
2. **Обнаружение сигнала:** Следите за логом `logs/enrich_qc.log` — если появляется запись с QC-тегом (`CRITICAL`, `WARNING`), переходите к анализу.
3. **Валидация транзакции:**
   - Используйте CLI-валидатор:
     ```bash
     python -m qc.validate_transaction --raw <raw.json> --enrich <enrich.json>
     ```
   - Для создания фикстуры enrichment используйте:
     ```bash
     python -m qc.export_fixture --signature <SIG> --platform raydium_enrich
     ```
4. **Анализ отчёта:** Изучите цветной diff-отчёт, исправьте логику enrichment при необходимости.
5. **Регрессионное тестирование:** Повторно запустите валидатор на фикстуре, чтобы убедиться в отсутствии CRITICAL-расхождений.
6. **Коммит и Pull Request:**
   - Перед коммитом убедитесь, что проходят все pre-commit хуки (`black`, `ruff`).
   - Оформите PR с описанием сути изменений и ссылкой на тикет/issue.

---

## Git Flow

- Ветки именуйте по шаблону: `feature/<desc>`, `bugfix/<desc>`, `test/<desc>`
- Все изменения проходят через Pull Request и code review.
- Не коммитьте напрямую в `main`/`master`.

---

## Code Review Checklist

- [ ] Код отформатирован с помощью `black` и проходит `ruff` без ошибок.
- [ ] Все новые функции снабжены docstring в Google Python Style Guide.
- [ ] Нет закомментированного или неиспользуемого кода.
- [ ] Все QC-теги, используемые в enrichment, описаны в `qc/qc_tags_catalog.md`.
- [ ] Для новых enrichment-функций есть unit-тесты и фикстуры.
- [ ] Все изменения протестированы локально (включая QC-валидацию).
- [ ] Если в PR изменяется логика обработки ошибок или добавляются новые try-except блоки в модулях enrichment, обязательно проверьте, что в except добавлен соответствующий QC-тег (например, через qc_tags.append(...)).
- [ ] Все файлы в PR имеют кодировку UTF-8 без BOM.

---

## Документирование

- Все новые функции и классы должны иметь docstring в стиле Google Python Style Guide.
- Для новых QC-тегов обязательно обновляйте `qc/qc_tags_catalog.md`.
- Для новых CLI-утилит добавляйте usage-инструкцию в README.md или отдельный раздел.

## Golden Dataset: Automated Fixture Management

Для управления эталонными тестовыми данными (Golden Dataset) используйте скрипт `scripts/manage_fixtures.py`.

### Основные команды:

- `--add <signature>`: скачивает raw.json по сигнатуре, сохраняет в `tests/regression/fixtures/<signature>/raw.json`, затем пропускает через UniversalParser и сохраняет результат как `enrich.etalon.json`.
- `--update-from-list <file_path>`: читает список сигнатур из файла и выполняет операцию `--add` для каждой, пропуская уже существующие raw.json.
- `--re-enrich <signature|--all>`: не скачивая raw.json заново, перегенерирует enrich.etalon.json для указанной сигнатуры (или для всех), используя последнюю версию UniversalParser.

### Процесс добавления новой транзакции:
1. Добавьте сигнатуру в `tests/regression/golden_signatures.txt`.
2. Запустите: `python scripts/manage_fixtures.py --update-from-list tests/regression/golden_signatures.txt`
3. Проверьте, что новые файлы появились в соответствующих папках.

### Важно:
- Все фикстуры должны быть в репозитории. Тесты не должны обращаться к RPC.
- Если raw.json не может быть получен, скрипт завершится с ошибкой.

## Архитектурные принципы enrichment и Golden Dataset

### Приоритеты паттернов для Golden Dataset и разработки обогатителей
1. **Single-Hop Swaps** (Raydium, Orca)
2. **SPL Token Transfers** (transfer, transferChecked)
3. **Account Management** (createAssociatedTokenAccount, closeAccount)
4. **Aggregator Swaps** (Jupiter)
5. **Liquidity Provisioning** (deposit/withdraw Raydium, open_position Orca)
6. **Metaplex Operations** (createMetadataAccountV3)
7. **Bridging (мосты)**

### Эталонная структура enrich-модели (Pydantic EnrichmentContext)
- `transaction_metadata`: сигнатура, время, статус
- `token_flows`: универсальный список всех движений токенов (IN/OUT)
- `protocol_details`: список взаимодействий с протоколами (расширяемый)
- `parser_version`: версия обогатителя
- `qc_context`: QC-информация

### Организация кода
- Каждый протокол — отдельный enricher-модуль (`enrich_parser/enrich/<protocol>.py`)
- UniversalParser — диспетчер, вызывает enricher по program_id

### Fallback/Unknown
- Логирование и enrich с пометкой "Unknown"
- Запись в protocol_details: `{ "protocol_name": "Unknown", ... }`
- Алертинг по количеству unknown-паттернов

### Best practices по тестированию и поддержанию Golden Dataset
- Golden Dataset — единственный источник истины для регрессионных тестов
- Каждый тест — атомарный, покрывает один аспект
- Edge-кейсы и ошибки из продакшена — сразу в Golden Dataset
- Только автоматизированное управление фикстурами (manage_fixtures.py)
- Каждая сигнатура снабжена комментарием о покрываемом кейсе 

## Архитектурный паттерн: Унифицированный анализ потоков ценности (Value Flows)

### Проблема
В Solana транзакциях движение нативного SOL часто не отражается в `preTokenBalances`/`postTokenBalances`, а только в `preBalances`/`postBalances`. Это создавало "слепую зону" для анализа потоков ценности, особенно при оборачивании/разворачивании SOL -> WSOL, что критично для финансового анализа и ML.

### Решение
В проекте принят единый паттерн анализа value flows: для каждого обогатителя (enricher) используется функция `get_all_value_flows(pre_bal, post_bal, owner)` из `enrich_parser/core/value_flows.py`. Она формирует список `TokenFlow` (IN/OUT) по всем токенам, где есть изменение баланса для fee payer/owner. Это обеспечивает:
- Корректный учёт SOL/WSOL и SPL-токенов,
- Масштабируемость и консистентность для всех DEX/протоколов,
- ML-ready структуру данных для downstream-аналитики.

### Пример использования
```python
from enrich_parser.core.value_flows import get_all_value_flows
# ...
token_flows = get_all_value_flows(pre_bal, post_bal, initiator)
for flow in token_flows:
    if flow.direction == "OUT":
        input_token, input_amt = flow.mint, flow.amount
    elif flow.direction == "IN":
        output_token, output_amt = flow.mint, flow.amount
```

### Best practices
- Всегда используйте этот паттерн для анализа input/output во всех enrichers.
- Не реализуйте логику анализа балансов inline — только через общий метод.
- Добавляйте edge-case тесты с SOL/WSOL и транзакциями, где движение SOL видно только через `pre/postBalances`.
- При добавлении новых протоколов — интегрируйте этот паттерн с самого начала.

### Edge-cases для тестирования
- Своп с неявным оборачиванием SOL -> WSOL.
- Минт NFT с оплатой нативным SOL.
- Любые транзакции, где движение SOL не отражено в `tokenBalances`.

