import json
from pathlib import Path
from qc.etalon_generator import generate_from_reparse
from qc.diff_engine import diff_etalon_vs_enrich

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures" / "transactions"
RAW_PATH = FIXTURES_DIR / "raydium_2WjgSSiE.raw.json"
ENRICH_PATH = FIXTURES_DIR / "raydium_2WjgSSiE.enrich.json"

def test_generate_etalon_from_raw():
    with open(RAW_PATH, "r", encoding="utf-8") as f:
        raw_json = json.load(f)
    etalon = generate_from_reparse(raw_json)
    assert isinstance(etalon.token_flows, list)
    # Можно добавить больше проверок по содержимому token_flows и swap_summary

def test_diff_etalon_vs_enrich():
    # TODO: TICKET-126 - Переписать тест под новую архитектуру и восстановить строгие проверки
    with open(RAW_PATH, "r", encoding="utf-8") as f:
        raw_json = json.load(f)
    with open(ENRICH_PATH, "r", encoding="utf-8") as f:
        enrich_json = json.load(f)
    etalon = generate_from_reparse(raw_json)
    diff = diff_etalon_vs_enrich(etalon, enrich_json)
    assert "diffs" in diff
    assert "summary" in diff 