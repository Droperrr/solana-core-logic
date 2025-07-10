import json
from parser.universal_parser import UniversalParser
from pathlib import Path
import pytest
from decoder.enrichers.dump_detector_enricher import DumpDetectorEnricher

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures/enrichment/raydium"

@pytest.fixture
def happy_path_tx():
    with open(FIXTURES_DIR / "happy_path_swap.json", encoding='utf-8') as f:
        return json.load(f)

def test_enriched_event_fields(happy_path_tx):
    # TODO: TICKET-124 - Заменить фикстуру на реальный SWAP для проверки priority_fee и других полей.
    parser = UniversalParser()
    result = parser.parse_raw_transaction(happy_path_tx)
    assert result is not None, "Парсер должен вернуть результат"
    assert "enriched_events" in result, "Результат должен содержать ключ enriched_events"
    assert isinstance(result["enriched_events"], list), "enriched_events должен быть списком"
    # Smoke: не проверяем наличие SWAP, только структуру enriched_events 

def test_dump_detector_enricher_simple():
    enricher = DumpDetectorEnricher()
    token = "So11111111111111111111111111111111111111112"
    # Первое событие — цена 10, дампа быть не должно
    event1 = {
        "enrichment_data": {"price_impact": {"execution_price": 10.0, "mint_in": token}},
        "details": {},
    }
    res1 = enricher.enrich(event1, {})
    assert res1 is None
    # Второе событие — цена 4 (падение на 60%)
    event2 = {
        "enrichment_data": {"price_impact": {"execution_price": 4.0, "mint_in": token}},
        "details": {},
    }
    res2 = enricher.enrich(event2, {})
    assert res2 is not None
    assert "dump_detected" in res2
    dump = res2["dump_detected"]
    assert dump["old_price"] == 10.0
    assert dump["new_price"] == 4.0
    assert abs(dump["drop_pct"] - 0.6) < 1e-6
    # Третье событие — цена 3 (ещё падение, теперь от 4)
    event3 = {
        "enrichment_data": {"price_impact": {"execution_price": 3.0, "mint_in": token}},
        "details": {},
    }
    res3 = enricher.enrich(event3, {})
    assert res3 is None  # Падение < 50% от предыдущей (4 -> 3) 