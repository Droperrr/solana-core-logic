import json
import pytest

from decoder.normalizer import normalize, models

FIXTURE_PATH = 'soltxs-main/tests/.data'

def load_fixture(name):
    path = f'{FIXTURE_PATH}/{name}'
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)

def test_normalize_rpc():
    data = load_fixture('pumpfun_buy_rpc.json')
    tx = normalize(data)
    assert isinstance(tx, models.Transaction)
    assert tx.slot > 0
    assert isinstance(tx.signatures, list) and len(tx.signatures) > 0
    assert hasattr(tx.message, 'accountKeys')
    assert hasattr(tx.meta, 'fee')
    assert hasattr(tx, 'loadedAddresses') 