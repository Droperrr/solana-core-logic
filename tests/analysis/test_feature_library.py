import pytest
import pandas as pd
from analysis.feature_library import (
    calculate_daily_volume,
    calculate_transaction_count,
    calculate_unique_wallets,
    calculate_buy_sell_ratio,
    calculate_gini_coefficient,
    calculate_concentration_top_N
)
import datetime

def make_events_df():
    data = [
        {'signature': 'tx1', 'block_time': pd.Timestamp('2024-06-28 12:00:00'), 'event_type': 'SWAP',
         'token_a_mint': 'AAA', 'token_b_mint': 'BBB', 'from_wallet': 'W1', 'to_wallet': 'W2', 'from_amount': 100, 'to_amount': 200},
        {'signature': 'tx2', 'block_time': pd.Timestamp('2024-06-28 13:00:00'), 'event_type': 'SWAP',
         'token_a_mint': 'BBB', 'token_b_mint': 'AAA', 'from_wallet': 'W3', 'to_wallet': 'W4', 'from_amount': 50, 'to_amount': 25},
        {'signature': 'tx3', 'block_time': pd.Timestamp('2024-06-28 14:00:00'), 'event_type': 'TRANSFER',
         'token_a_mint': 'AAA', 'token_b_mint': 'CCC', 'from_wallet': 'W1', 'to_wallet': 'W5', 'from_amount': 10, 'to_amount': 10},
    ]
    return pd.DataFrame(data)

def test_calculate_daily_volume():
    df = make_events_df()
    date = datetime.date(2024, 6, 28)
    assert calculate_daily_volume(df, 'AAA', date) == 100 + 25 + 10 + 200  # from_amount+to_amount, где AAA участвует
    assert calculate_daily_volume(df, 'BBB', date) == 50 + 200 + 100 + 25
    assert calculate_daily_volume(df, 'CCC', date) == 10 + 10
    assert calculate_daily_volume(df, 'ZZZ', date) == 0.0

def test_calculate_transaction_count():
    df = make_events_df()
    date = datetime.date(2024, 6, 28)
    assert calculate_transaction_count(df, 'AAA', date) == 3
    assert calculate_transaction_count(df, 'BBB', date) == 2
    assert calculate_transaction_count(df, 'CCC', date) == 1
    assert calculate_transaction_count(df, 'ZZZ', date) == 0

def test_calculate_unique_wallets():
    df = make_events_df()
    date = datetime.date(2024, 6, 28)
    assert calculate_unique_wallets(df, 'AAA', date) == 5  # W1, W2, W3, W4, W5
    assert calculate_unique_wallets(df, 'BBB', date) == 4  # W1, W2, W3, W4
    assert calculate_unique_wallets(df, 'CCC', date) == 2  # W1, W5
    assert calculate_unique_wallets(df, 'ZZZ', date) == 0

def test_calculate_buy_sell_ratio():
    df = make_events_df()
    date = datetime.date(2024, 6, 28)
    # Для AAA: buy = 1 (token_b_mint), sell = 2 (token_a_mint)
    assert calculate_buy_sell_ratio(df, 'AAA', date) == 1 / 2
    # Для BBB: buy = 1, sell = 1
    assert calculate_buy_sell_ratio(df, 'BBB', date) == 1.0
    # Для CCC: buy = 0, sell = 1
    assert calculate_buy_sell_ratio(df, 'CCC', date) == 0.0
    # Для ZZZ: buy = 0, sell = 0
    assert calculate_buy_sell_ratio(df, 'ZZZ', date) == 0.0

def test_calculate_gini_coefficient():
    # Пустой список
    assert calculate_gini_coefficient([]) == 0.0
    # Все равны
    assert calculate_gini_coefficient([10, 10, 10, 10]) == 0.0
    # Один большой, остальные нули
    assert calculate_gini_coefficient([100, 0, 0, 0]) == 0.75
    # Один большой, остальные маленькие
    assert pytest.approx(calculate_gini_coefficient([100, 1, 1, 1]), 0.01) == 0.735
    # Два равных
    assert calculate_gini_coefficient([50, 50]) == 0.0

def test_calculate_concentration_top_N():
    # Пустой список
    assert calculate_concentration_top_N([], n=5) == 0.0
    # Все равны
    assert calculate_concentration_top_N([10, 10, 10, 10], n=2) == 0.5
    # Один большой, остальные маленькие
    assert calculate_concentration_top_N([100, 1, 1, 1], n=1) == 100 / 103
    assert calculate_concentration_top_N([100, 1, 1, 1], n=2) == (100 + 1) / 103
    # n больше длины списка
    assert calculate_concentration_top_N([10, 20], n=5) == 1.0 