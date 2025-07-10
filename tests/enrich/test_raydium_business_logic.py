import json
import os
import pytest
from parser.universal_parser import UniversalParser

FIXTURE_DIR = "tests/regression/fixtures/3FbMpzJHkHjw56w28mYQJ6GdisPUGcDK2sqyNmDDFzP9qtaSpR9RSTiJ77Yn52iNGC4zTnts3bSqafg5RSNgswWz"
RAW_PATH = os.path.join(FIXTURE_DIR, "raw.json")

@pytest.mark.parametrize("raw_path", [RAW_PATH])
def test_raydium_add_liquidity(raw_path):
    with open(raw_path, "r", encoding="utf-8") as f:
        raw_tx = json.load(f)

    parser = UniversalParser()
    result = parser.parse_raw_transaction(raw_tx)
    events = result["enriched_events"]
    # Ожидаем только одно событие LIQUIDITY
    assert len(events) == 1
    event = events[0]

    # Проверяем основные поля
    assert event["event_type"] == "LIQUIDITY"
    assert event["protocol"] == "Raydium"
    assert event["instruction_type"] == "deposit"
    assert event["initiator"] == "GneZyKGyMesLHQRzm5ot7NvyfKTa5t2Mfo9BPjYCABAL"
    assert event["liquidity_pool_address"] == "36TQrxTTH16RZovowLuJNfAurUS1ZjS3pd53ZdzXG8q3"
    assert event["liquidity_change_type"] == "ADD"
    assert event["priority_fee"] == 7500000
    assert event["compute_units_consumed"] == 74881

    # Проверяем token_flows
    flows = event["token_flows"]
    assert isinstance(flows, list)
    assert len(flows) == 3

    # Проверяем, что есть OUT WSOL, OUT custom token, IN LP token
    directions = {f["direction"] for f in flows}
    assert directions == {"OUT", "IN"}

    # WSOL OUT
    wsol = next(f for f in flows if f["mint"] == "So11111111111111111111111111111111111111112")
    assert wsol["direction"] == "OUT"
    assert wsol["owner"] == event["initiator"]
    assert wsol["amount"] == 5000000

    # Custom token OUT
    custom = next(f for f in flows if f["mint"] == "AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC")
    assert custom["direction"] == "OUT"
    assert custom["owner"] == event["initiator"]
    assert custom["amount"] == 15584167321017243

    # LP token IN
    lp = next(f for f in flows if f["mint"] == "YPa9WuUBgzwkwrCMPmeanWVgv8Q2QDiKF5YHkoRQBkz")
    assert lp["direction"] == "IN"
    assert lp["owner"] == event["initiator"]
    assert lp["amount"] == 166354744 