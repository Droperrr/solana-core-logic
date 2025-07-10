import json
import os
import pytest
from services.onchain_price_engine import OnChainPriceEngine
import sqlite3

FIXTURE_PATH = os.path.join(os.path.dirname(__file__), '../fixtures/enrichment/raydium/raydium_2WjgSSiE.raw.json')
DB_PATH = os.path.join(os.path.dirname(__file__), '../../db/solana_db.sqlite')

@pytest.fixture
def raydium_raw_tx():
    with open(FIXTURE_PATH, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_raydium_price_from_swap(raydium_raw_tx):
    # Минимальный swap_event для Raydium (детали можно расширить при необходимости)
    swap_event = {
        'protocol': 'raydium',
        'details': {
            'token_a_mint': 'AL2HhMQLkJqeeK5w4akoogzyYBZ6GYkBfxjscCf2L2yC',
            'token_b_mint': 'So11111111111111111111111111111111111111112',
            'pool_coin_token_account': '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1',
            'pool_pc_token_account': '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1',
        }
    }
    engine = OnChainPriceEngine()
    price = engine.calculate_price_from_swap(swap_event, raydium_raw_tx)
    assert price is not None, 'Raydium price calculation failed'
    # Можно добавить проверку на ожидаемое значение, если оно известно 

def test_calculates_price_for_jupiter_swap_from_db():
    signature = '5v5ujNVmmBjQi5BZCizdvKdb75G3NHewgxbpohFQeApmSaG2Ms4SPmH7X4ejBF4Qa4TiXVdJGZjEWfNun1SYWY5e'
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT raw_json FROM transactions WHERE signature = ?", (signature,))
    row = cursor.fetchone()
    assert row is not None, f'No raw_json found for signature {signature}'
    raw_json = row[0]
    raw_tx = json.loads(raw_json)
    # Jupiter swap_event: минимально необходимое
    swap_event = {
        'protocol': 'jupiter',
        'details': {
            'program_id': 'JUP4Fb2cqiRUcaTHdrPC8h2gNsA2ETXiPDD33WcGuJB'
        }
    }
    engine = OnChainPriceEngine()
    price = engine.calculate_price_from_swap(swap_event, raw_tx)
    assert isinstance(price, float) and price > 0, f'Jupiter price calculation failed or returned non-positive value: {price}' 

def test_is_price_dump():
    """Тест метода детекции дампа цены"""
    engine = OnChainPriceEngine()
    
    # Тест 1: Падение больше 50% (порог по умолчанию) - должно быть дампом
    assert engine.is_price_dump(100, 49) == True, "Падение с 100 до 49 (51%) должно быть дампом"
    
    # Тест 2: Падение меньше 50% - не должно быть дампом
    assert engine.is_price_dump(100, 51) == False, "Падение с 100 до 51 (49%) не должно быть дампом"
    
    # Тест 3: Рост цены - не должно быть дампом
    assert engine.is_price_dump(100, 110) == False, "Рост с 100 до 110 не должен быть дампом"
    
    # Тест 4: Точно на пороге (50%) - не должно быть дампом
    assert engine.is_price_dump(100, 50) == False, "Падение точно на 50% не должно быть дампом"
    
    # Тест 5: Кастомный порог 30%
    assert engine.is_price_dump(100, 69, threshold=0.3) == True, "Падение с 100 до 69 (31%) должно быть дампом при пороге 30%"
    assert engine.is_price_dump(100, 71, threshold=0.3) == False, "Падение с 100 до 71 (29%) не должно быть дампом при пороге 30%"
    
    # Тест 6: Некорректные значения цен - не должно быть дампом
    assert engine.is_price_dump(0, 50) == False, "Нулевая предыдущая цена не должна давать дамп"
    assert engine.is_price_dump(100, 0) == False, "Нулевая текущая цена не должна давать дамп"
    assert engine.is_price_dump(-100, 50) == False, "Отрицательная предыдущая цена не должна давать дамп"
    assert engine.is_price_dump(100, -50) == False, "Отрицательная текущая цена не должна давать дамп"

def test_find_first_dump_integration():
    """Интеграционный тест поиска первого дампа для реального токена.
    
    Критерий дампа: если после SWAP цена токена в SOL падает более чем на 50% по сравнению с предыдущей транзакцией.
    
    Проверяется, что:
      - Дамп найден (result не None)
      - В результате есть все ключевые поля
      - Падение цены действительно превышает 50%
    """
    token_address = "AGmXzgFQw8bLtsJvr5dgnHtyP6rP6GLwmZwgVqjFL6Q8"
    engine = OnChainPriceEngine()
    result = engine.find_first_dump(token_address)
    assert result is not None, "Дамп не найден, хотя должен быть"
    for key in ["signature", "block_time", "price_drop_percent", "previous_price", "dump_price"]:
        assert key in result, f"В результате отсутствует ключ {key}"
    assert result["price_drop_percent"] >= 50, (
        f"Падение цены недостаточно велико: {result['price_drop_percent']}% (< 50%)"
    )