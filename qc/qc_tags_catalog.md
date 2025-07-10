# QC Tags Catalog

| Tag                        | Severity   | Description                                      |
|----------------------------|------------|--------------------------------------------------|
| BALANCE_CHANGE_MISMATCH    | CRITICAL   | Несовпадение движения токенов/балансов           |
| AMOUNT_OUT_MISMATCH        | CRITICAL   | Некорректная сумма на выходе                     |
| POOL_HEURISTIC_MISMATCH    | WARNING    | Не совпадает определённый пул (эвристика)        |
| ROUTE_MISMATCH             | WARNING    | Не совпадает маршрут (эвристика)                 |
| EXTERNAL_TAG_DIFFERENCE    | INFO       | Внешний источник содержит дополнительные теги    |
| FIELD_MISSING              | WARNING    | В enrichment отсутствует ожидаемое поле           |
| AMBIGUOUS_FLOW             | WARNING    | Неоднозначное определение токен-флоу             |
| ...                        | ...        | ...                                              |

> Каталог пополняется по мере появления новых edge-cases. Все изменения проходят code review. 