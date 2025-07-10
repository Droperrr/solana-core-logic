import sys
import os
# Добавляем корневую директорию проекта в путь поиска модулей
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
import pytest
from analysis.analyzer_rules import BuySellResult, _analyze_from_token_transfers, _analyze_from_inner_instructions, _analyze_from_balances

# Пример мок-данных для buy/sell через token_transfers
MOCK_BUY_TX = {
    'meta': {
        'tokenTransfers': [
            {'fromUserAccount': 'A', 'toUserAccount': 'B', 'mint': 'TARGET', 'tokenAmount': {'amount': '1000'}},
            {'fromUserAccount': 'A', 'toUserAccount': 'B', 'mint': 'QUOTE', 'tokenAmount': {'amount': '2000'}},
        ]
    },
    'transaction': {'message': {'accountKeys': ['A', 'B']}}
}

MOCK_SELL_TX = {
    'meta': {
        'tokenTransfers': [
            {'fromUserAccount': 'B', 'toUserAccount': 'A', 'mint': 'TARGET', 'tokenAmount': {'amount': '1000'}},
            {'fromUserAccount': 'B', 'toUserAccount': 'A', 'mint': 'QUOTE', 'tokenAmount': {'amount': '2000'}},
        ]
    },
    'transaction': {'message': {'accountKeys': ['A', 'B']}}
}

MOCK_UNKNOWN_TX = {
    'meta': {},
    'transaction': {'message': {'accountKeys': ['A', 'B']}}
}

def test_analyze_from_token_transfers_buy():
    result = _analyze_from_token_transfers(MOCK_BUY_TX)
    assert result is None or isinstance(result, BuySellResult)
    # TODO: доработать, когда появится логика

def test_analyze_from_token_transfers_sell():
    result = _analyze_from_token_transfers(MOCK_SELL_TX)
    assert result is None or isinstance(result, BuySellResult)
    # TODO: доработать, когда появится логика

def test_analyze_from_token_transfers_unknown():
    result = _analyze_from_token_transfers(MOCK_UNKNOWN_TX)
    assert result is None

def test_analyze_from_inner_instructions():
    result = _analyze_from_inner_instructions(MOCK_BUY_TX)
    assert result is None or isinstance(result, BuySellResult)

def test_analyze_from_balances():
    result = _analyze_from_balances(MOCK_BUY_TX)
    assert result is None or isinstance(result, BuySellResult) 