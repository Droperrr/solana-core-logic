import pytest
from unittest.mock import patch, MagicMock
from rpc.client import RPCClient, CanonicalRawTransaction
import json

TEST_SIGNATURE = "2WjgSSiEnMNT..."

# Загружаем эталонный raw_json из фикстуры
with open("tests/fixtures/transactions/raydium_2WjgSSiE.raw.json", "r", encoding="utf-8") as f:
    RAW_JSON_FIXTURE = json.load(f)

@pytest.fixture
def rpc_client():
    # Передаём фиктивные ключи явно
    return RPCClient(
        alchemy_keys=["fake_alchemy_key"],
        quicknode_keys=["fake_quicknode_key"],
        helius_keys=["fake_helius_key"]
    )

@patch("requests.post")
def test_get_transaction_success(mock_post, rpc_client):
    # Мокаем успешный ответ от RPC
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {"result": RAW_JSON_FIXTURE}
    mock_post.return_value = mock_response

    tx = rpc_client.get_transaction(TEST_SIGNATURE)
    assert isinstance(tx, CanonicalRawTransaction)
    assert tx.raw_json == RAW_JSON_FIXTURE
    assert tx.rpc_source in {"alchemy", "quicknode", "helius"}

@patch("requests.post")
def test_get_transaction_rate_limit_and_failover(mock_post, rpc_client):
    # Первый ключ — 429, второй — успешный
    mock_response_429 = MagicMock()
    mock_response_429.status_code = 429
    mock_response_429.json.return_value = {}
    mock_response_200 = MagicMock()
    mock_response_200.status_code = 200
    mock_response_200.json.return_value = {"result": RAW_JSON_FIXTURE}
    # Сначала 429, потом 200
    mock_post.side_effect = [mock_response_429, mock_response_200]

    tx = rpc_client.get_transaction(TEST_SIGNATURE)
    assert isinstance(tx, CanonicalRawTransaction)
    assert tx.raw_json == RAW_JSON_FIXTURE

@patch("requests.post")
def test_get_transaction_all_providers_fail(mock_post, rpc_client, tmp_path):
    # Все попытки — 500 ошибка
    mock_response_500 = MagicMock()
    mock_response_500.status_code = 500
    mock_response_500.json.return_value = {}
    mock_post.return_value = mock_response_500

    tx = rpc_client.get_transaction(TEST_SIGNATURE)
    assert tx is None 