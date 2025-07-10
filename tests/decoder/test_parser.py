import json
import pytest
from decoder.normalizer import normalize
from decoder.router import parse_instruction
from decoder.parsers.pumpfun import Swap

FIXTURE_PATH = 'soltxs-main/tests/.data'

def load_fixture(name):
    path = f'{FIXTURE_PATH}/{name}'
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_router_pumpfun_swap():
    data = load_fixture('pumpfun_buy_rpc.json')
    tx = normalize(data)
    # Ищем первую инструкцию с нужным programId
    for idx, instr in enumerate(tx.message.instructions):
        program_id = tx.all_accounts[instr.programIdIndex]
        if program_id == '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P':
            parsed = parse_instruction(tx, idx)
            assert isinstance(parsed, Swap)
            assert parsed.instruction_name == 'Swap'
            assert parsed.mint is not None
            assert parsed.user is not None
            break
    else:
        pytest.fail('PumpFun instruction not found in fixture') 