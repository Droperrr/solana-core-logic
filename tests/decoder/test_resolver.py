import json
import pytest
from decoder.normalizer import normalize
from decoder.router import parse_instruction
from decoder.resolver import resolve
from decoder.resolver.resolvers.pumpfun import PumpFun
from decoder.resolver.resolvers.raydium import Raydium
from decoder.resolver.resolvers.unknown import Unknown

FIXTURE_PATH = 'soltxs-main/tests/.data'

def load_fixture(name):
    path = f'{FIXTURE_PATH}/{name}'
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_resolver_pumpfun():
    """
    Тест для проверки резолвера PumpFun
    """
    # Загружаем транзакцию PumpFun
    data = load_fixture('pumpfun_buy_rpc.json')
    tx = normalize(data)
    
    # Парсим все инструкции
    parsed_instructions = []
    for idx, instr in enumerate(tx.message.instructions):
        parsed = parse_instruction(tx, idx)
        parsed_instructions.append(parsed)
    
    # Резолвим транзакцию
    resolved = resolve(parsed_instructions)
    
    # Проверяем, что резолвер вернул объект PumpFun
    assert isinstance(resolved, PumpFun)
    assert resolved.type in ["buy", "sell"]
    assert resolved.who is not None
    assert resolved.from_token is not None
    assert resolved.to_token is not None
    assert resolved.from_amount > 0
    assert resolved.to_amount > 0

def test_resolver_unknown():
    """
    Тест для проверки резолвера Unknown
    """
    # Создаем набор инструкций, которые не должны быть распознаны
    class DummyInstruction:
        def __init__(self):
            self.program_id = "dummy"
            self.instruction_name = "dummy"
    
    dummy_instructions = [DummyInstruction()]
    
    # Резолвим транзакцию
    resolved = resolve(dummy_instructions)
    
    # Проверяем, что резолвер вернул объект Unknown
    assert isinstance(resolved, Unknown)
    assert resolved.type == "unknown"
    assert len(resolved.instructions) == 1 