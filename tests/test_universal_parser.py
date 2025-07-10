import pytest
from parser.universal_parser import UniversalParser
from enrich_parser.models import EnrichedEvent
import os
import json

from enrich_parser.enrich import raydium, jupiter, orca

class DummyEnricher:
    PROGRAM_ID = "DummyProgram11111111111111111111111111111111"
    __version__ = "1.2.3"
    @staticmethod
    def enrich(instr, raw_tx):
        # Минимальный EnrichedEvent для теста
        return [EnrichedEvent(
            event_id="sig:0:-1:0",
            tx_signature="sig",
            event_type="UNKNOWN",
            protocol="Dummy",
            instruction_type="dummy",
            slot=0,
            block_time=0,
            parser_version=DummyEnricher.__version__,
            initiator="dummy",
            involved_accounts=[],
            token_a_mint=None,
            token_a_amount_change=None,
            token_b_mint=None,
            token_b_amount_change=None,
            liquidity_change_type=None,
            liquidity_provider=None,
            transfer_source=None,
            transfer_destination=None,
            transfer_mint=None,
            transfer_amount=None,
            staking_validator=None,
            staking_amount=None,
            token_flows=[],
            qc_tags=[],
            raw_instruction=instr
        )]

def test_parse_raw_transaction_single_enricher(monkeypatch):
    # TODO: TICKET-125 - Переписать тест под новую архитектуру и восстановить строгие проверки
    from parser import universal_parser
    monkeypatch.setitem(universal_parser.ENRICHERS, DummyEnricher.PROGRAM_ID, DummyEnricher)
    raw_tx = {
        "transaction": {
            "message": {
                "instructions": [
                    {"programId": DummyEnricher.PROGRAM_ID, "data": "abc"}
                ]
            },
            "signatures": ["sig"]
        },
        "signature": "sig"
    }
    parser = UniversalParser()
    result = parser.parse_raw_transaction(raw_tx)
    assert result is not None, "Парсер должен вернуть результат"
    assert "enriched_events" in result, "Результат должен содержать ключ enriched_events"
    assert isinstance(result["enriched_events"], list), "enriched_events должен быть списком"

@pytest.mark.parametrize("enricher, program_id, key", [
    (raydium, raydium.PROGRAM_ID, "raydium"),
    (jupiter, jupiter.PROGRAM_ID, "jupiter"),
    (orca, orca.PROGRAM_ID, "orca"),
])
def test_parse_raw_transaction_enrichers(enricher, program_id, key):
    # TODO: TICKET-125 - Переписать тест под новую архитектуру и восстановить строгие проверки
    raw_tx = {
        "transaction": {
            "message": {
                "instructions": [
                    {"programId": program_id, "data": "abc"}
                ]
            },
            "signatures": ["sig123"]
        },
        "signature": "sig123"
    }
    parser = UniversalParser()
    result = parser.parse_raw_transaction(raw_tx)
    assert result is not None, "Парсер должен вернуть результат"
    assert "enriched_events" in result, "Результат должен содержать ключ enriched_events"
    assert isinstance(result["enriched_events"], list), "enriched_events должен быть списком"

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