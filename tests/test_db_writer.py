import pytest
from unittest.mock import MagicMock
from db.db_writer import save_parsed_transaction
import json

# Минимальный валидный tx_data для теста
TX_DATA = {
    "signature": "testsig123",
    "block_time": 1743512493,
    "meta": {"fee": 5000, "err": None, "computeUnitsConsumed": 12345},
    "transaction": {"message": {"accountKeys": ["testpayer"]}},
    "slot": 123456,
    "parsed_instructions": [],
}

def test_save_parsed_transaction_success():
    # Мокаем соединение и курсор
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # Вызываем функцию
    save_parsed_transaction(
        conn=mock_conn,
        tx_data=TX_DATA,
        current_token_mint="mint1",
        transaction_type="swap",
        source_query_type="token",
        source_query_address="address1",
        detected_patterns=["pattern1"],
        involved_platforms=["platform1"],
        net_token_flows_json={"mint1": 100},
        additional_context={"foo": "bar"},
        parsed_tx_version=1
    )
    # Проверяем, что был вызван execute (SQL-инъекция не тестируем)
    assert mock_cursor.execute.called
    # Проверяем, что сигнатура передана
    args, kwargs = mock_cursor.execute.call_args
    assert "testsig123" in str(args)

def test_save_parsed_transaction_enriched_events_and_version():
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    enriched_events = [{"event_id": "foo", "amount": 42}]
    parser_version = "raydium-1.2.3"
    save_parsed_transaction(
        conn=mock_conn,
        tx_data=TX_DATA,
        current_token_mint="mint1",
        transaction_type="swap",
        source_query_type="token",
        source_query_address="address1",
        detected_patterns=["pattern1"],
        involved_platforms=["platform1"],
        net_token_flows_json={"mint1": 100},
        additional_context={"foo": "bar"},
        parsed_tx_version=1,
        enriched_events=enriched_events,
        parser_version=parser_version
    )
    assert mock_cursor.execute.called
    args, kwargs = mock_cursor.execute.call_args
    # enriched_events должен быть сериализован как JSON-строка
    assert json.dumps(enriched_events, ensure_ascii=False) in str(args)
    # parser_version должен быть в аргументах
    assert parser_version in str(args) 