import os
import json
from parser.universal_parser import UniversalParser

FIXTURE_PATH = "tests/regression/fixtures/2LgJdAtoBJU63EVLVyoGfJM5FAcsPQpYNi3N8ZgvBozbsTEWG7X9pFprjhoHG6b3zSNtxtP2EMQQ1NgFfZSKPd8t"

def test_account_provisioning_event():
    # TODO: TICKET-127 - Переписать тест под новую архитектуру и восстановить строгие проверки
    raw_path = os.path.join(FIXTURE_PATH, "raw.json")
    with open(raw_path, "r", encoding="utf-8") as f:
        raw_tx = json.load(f)
    parser = UniversalParser()
    result = parser.parse_raw_transaction(raw_tx)
    assert result is not None, "Парсер должен вернуть результат"
    assert "enriched_events" in result, "Результат должен содержать ключ enriched_events"
    assert isinstance(result["enriched_events"], list), "enriched_events должен быть списком" 