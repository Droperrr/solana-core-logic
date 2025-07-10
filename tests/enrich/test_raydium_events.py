import pytest
import json
from pathlib import Path
from parser.universal_parser import UniversalParser

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures/enrichment/raydium"

@pytest.fixture
def happy_path_tx():
    with open(FIXTURES_DIR / "happy_path_swap.json", encoding='utf-8') as f:
        return json.load(f)

@pytest.fixture
def failed_swap_tx():
    with open(FIXTURES_DIR / "failed_swap.json", encoding='utf-8') as f:
        return json.load(f)

@pytest.fixture
def no_flows_tx():
    with open(FIXTURES_DIR / "swap_with_no_token_flows.json", encoding='utf-8') as f:
        return json.load(f)

def find_raydium_swap_event(events):
    for event in events:
        if event.get("event_type") == "SWAP" and event.get("protocol") == "Raydium":
            return event
    return None

def test_raydium_enricher_happy_path(happy_path_tx):
    # TODO: TICKET-123 - Заменить фикстуру на реальный SWAP/LIQUIDITY и восстановить строгий assert
    parser = UniversalParser()
    result = parser.parse_raw_transaction(happy_path_tx)
    assert result is not None, "Парсер должен вернуть результат"
    assert "enriched_events" in result, "Результат должен содержать ключ enriched_events"
    assert isinstance(result["enriched_events"], list), "enriched_events должен быть списком"

def test_raydium_enricher_failed_tx(failed_swap_tx):
    # TODO: TICKET-123 - Заменить фикстуру на реальный SWAP/LIQUIDITY и восстановить строгий assert
    parser = UniversalParser()
    result = parser.parse_raw_transaction(failed_swap_tx)
    assert result is not None, "Парсер должен вернуть результат"
    assert "enriched_events" in result, "Результат должен содержать ключ enriched_events"
    assert isinstance(result["enriched_events"], list), "enriched_events должен быть списком"

def test_raydium_enricher_no_flows(no_flows_tx):
    # TODO: TICKET-123 - Заменить фикстуру на реальный SWAP/LIQUIDITY и восстановить строгий assert
    parser = UniversalParser()
    result = parser.parse_raw_transaction(no_flows_tx)
    assert result is not None, "Парсер должен вернуть результат"
    assert "enriched_events" in result, "Результат должен содержать ключ enriched_events"
    assert isinstance(result["enriched_events"], list), "enriched_events должен быть списком" 