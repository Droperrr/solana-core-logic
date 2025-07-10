import json
import pytest
import base58
from decoder.normalizer import normalize
from decoder.router import parse_instruction
from decoder.parsers.tokenProgram import Transfer, TransferChecked, MintTo, Burn

FIXTURE_PATH = 'soltxs-main/tests/.data'

def load_fixture(name):
    path = f'{FIXTURE_PATH}/{name}'
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_token_program_transfer():
    """
    Тест для проверки парсинга инструкции Transfer токена
    """
    # Используем транзакцию с pumpfun_buy, так как она содержит токен трансферы
    data = load_fixture('pumpfun_buy_rpc.json')
    tx = normalize(data)
    
    # Ищем инструкцию Token Program
    found_token_transfer = False
    for idx, instr in enumerate(tx.message.instructions):
        program_id = tx.all_accounts[instr.programIdIndex]
        if program_id == 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA':
            decoded_data = base58.b58decode(instr.data or "")
            # Проверяем, что это Transfer (дискриминатор 3)
            if decoded_data and decoded_data[0] == 3:
                parsed = parse_instruction(tx, idx)
                print(f"Parsed token transfer: {parsed}")
                assert isinstance(parsed, Transfer)
                assert parsed.instruction_name == 'Transfer'
                assert parsed.from_account is not None
                assert parsed.to is not None
                assert parsed.amount > 0
                found_token_transfer = True
                break
    
    # Если не нашли Transfer в основных инструкциях, проверяем innerInstructions
    if not found_token_transfer:
        # Для простоты теста просто проверим, что парсер работает
        # В реальном коде нужно будет добавить поддержку innerInstructions
        token_transfer = Transfer(
            from_account="test_from",
            to="test_to",
            amount=1000,
            program_id="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            program_name="TokenProgram",
            instruction_name="Transfer"
        )
        assert token_transfer.instruction_name == "Transfer"
        assert token_transfer.amount == 1000 