import json
import base58
from decoder.schemas import CreateAccountData
from decoder.parsers.systemProgram import SystemProgramParser

FIXTURE_PATH = "tests/fixtures/decoder/minimal_create_account.json"


def test_parse_create_account_instruction():
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        tx = json.load(f)
    # Извлекаем instruction data (base58)
    instr = tx["result"]["transaction"]["message"]["instructions"][0]
    data_b58 = instr["data"]
    data_bytes = base58.b58decode(data_b58)
    # Парсим через схему
    parsed = CreateAccountData.parse(data_bytes)
    # Проверяем discriminator (первые 4 байта должны быть нулями для CreateAccount)
    assert parsed.discriminator == b"\x00\x00\x00\x00"
    assert parsed.lamports > 0
    assert parsed.space > 0
    assert len(parsed.owner) == 32
    # Проверяем, что owner корректно сериализуется в base58
    owner_b58 = base58.b58encode(parsed.owner).decode()
    assert 43 <= len(owner_b58) <= 44
    print("✓ SystemProgram:CreateAccount instruction parsed successfully")


def test_system_program_parser_create_account():
    from decoder.normalizer.models import Transaction, Message, Instruction as Instr, Meta, LoadedAddresses
    with open(FIXTURE_PATH, "r", encoding="utf-8") as f:
        tx_json = json.load(f)
    
    result_data = tx_json["result"]
    msg_data = result_data["transaction"]["message"]
    instr = msg_data["instructions"][0]
    meta_data = result_data["meta"]
    
    # Создаем объекты для парсера
    message = Message(
        accountKeys=msg_data["accountKeys"],
        recentBlockhash=msg_data["recentBlockhash"],
        instructions=[
            Instr(
                accounts=instr["accounts"], 
                data=instr["data"], 
                programIdIndex=instr["programIdIndex"], 
                stackHeight=0
            )
        ],
        addressTableLookups=[]
    )
    
    meta = Meta(
        fee=meta_data["fee"],
        preBalances=meta_data["preBalances"],
        postBalances=meta_data["postBalances"],
        preTokenBalances=[],
        postTokenBalances=[],
        innerInstructions=[],
        logMessages=meta_data["logMessages"],
        err=meta_data["err"],
        status=meta_data["status"],
        computeUnitsConsumed=0
    )
    
    tx = Transaction(
        slot=result_data["slot"],
        blockTime=result_data["blockTime"],
        signatures=result_data["transaction"]["signatures"],
        message=message,
        meta=meta,
        loadedAddresses=LoadedAddresses(writable=[], readonly=[])
    )
    
    # Теперь используем парсер
    parser = SystemProgramParser()
    data_bytes = base58.b58decode(instr["data"])
    result = parser.process_CreateAccount(tx, 0, data_bytes)
    
    # Проверяем результат
    assert result.lamports > 0
    assert result.space > 0
    assert 43 <= len(result.owner) <= 44  # base58 pubkey
    assert result.instruction_name == "CreateAccount"
    print("✓ SystemProgramParser.process_CreateAccount works as expected") 