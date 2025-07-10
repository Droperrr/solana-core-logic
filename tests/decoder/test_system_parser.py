import json
import pytest
import base58
from decoder.normalizer import normalize
from decoder.router import parse_instruction
from decoder.parsers.systemProgram import CreateAccount, Transfer

FIXTURE_PATH = 'soltxs-main/tests/.data'

def load_fixture(name):
    path = f'{FIXTURE_PATH}/{name}'
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_router_system_instructions():
    """
    Тестирование парсинга инструкций System Program.
    Проверяет, что инструкции корректно парсятся и маршрутизируются.
    """
    data = load_fixture('pumpfun_create_rpc.json')
    tx = normalize(data)
    
    # Проверка Transfer инструкции (дискриминатор 2)
    found_transfer = False
    for idx, instr in enumerate(tx.message.instructions):
        program_id = tx.all_accounts[instr.programIdIndex]
        if program_id == '11111111111111111111111111111111':
            decoded_data = base58.b58decode(instr.data)
            discriminator = int.from_bytes(decoded_data[:4], 'little')
            if discriminator == 2:  # Transfer
                parsed = parse_instruction(tx, idx)
                assert isinstance(parsed, Transfer)
                assert parsed.instruction_name == 'Transfer'
                assert parsed.from_account is not None
                assert parsed.to_account is not None
                assert parsed.lamports > 0
                found_transfer = True
                break
    
    assert found_transfer, 'SystemProgram Transfer instruction not found in fixture'
    
    # Тест для CreateAccount будет добавлен, когда появится подходящая транзакция
    # Пока проверяем только структуру класса
    create_account = CreateAccount(
        funding_account="test_funding",
        new_account="test_new_account",
        lamports=1000000,
        space=100,
        owner="test_owner",
        program_id="11111111111111111111111111111111",
        program_name="SystemProgram",
        instruction_name="CreateAccount"
    )
    assert create_account.instruction_name == "CreateAccount"
    assert create_account.lamports == 1000000
    assert create_account.space == 100 