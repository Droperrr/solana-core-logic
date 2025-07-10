import pytest
from enrich_parser.enrich import raydium_price_impact

# Стандартный успешный свап Raydium V4

def test_standard_swap():
    VAULT_A = 'Va11111111111111111111111111111111111111111'
    VAULT_B = 'Vb11111111111111111111111111111111111111111'
    class MockRegistry:
        def validate_vaults(self, amm_id, coin_vault, pc_vault):
            return True
    mock_registry = MockRegistry()
    tx = {
        'meta': {
            'preTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '1000000'}, 'mint': 'mintA', 'owner': 'pool_owner'},
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '2000000'}, 'mint': 'mintB', 'owner': 'pool_owner'},
            ],
            'postTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '1100000'}, 'mint': 'mintA', 'owner': 'pool_owner'},
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '1900000'}, 'mint': 'mintB', 'owner': 'pool_owner'},
            ],
        }
    }
    swap_ix = {'accounts': ['x0', 'x1', 'x2', 'x3', 'x4', VAULT_A, VAULT_B]}
    result = raydium_price_impact.calculate_price_impact(
        tx, swap_ix, 'V4', 'mintA', 'mintB', 'user_wallet', registry=mock_registry
    )
    print('QC result (standard swap):', result)
    assert result['qc_status'] == 'success'
    assert result['pre_pool_in'] == 1000000
    assert result['pre_pool_out'] == 2000000
    assert result['amount_in'] == 100000
    assert result['amount_out'] == 100000
    assert result['price_impact'] is not None

# Edge-case: mint mismatch (QC error)
def test_mint_mismatch():
    VAULT_A = 'Va11111111111111111111111111111111111111111'
    VAULT_B = 'Vb11111111111111111111111111111111111111111'
    class MockRegistry:
        def validate_vaults(self, amm_id, coin_vault, pc_vault):
            return True
    mock_registry = MockRegistry()
    tx = {
        'meta': {
            'preTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '1000000'}, 'mint': 'mintX', 'owner': 'pool_owner'},
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '2000000'}, 'mint': 'mintY', 'owner': 'pool_owner'},
            ],
            'postTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '900000'}, 'mint': 'mintX', 'owner': 'pool_owner'},
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '2100000'}, 'mint': 'mintY', 'owner': 'pool_owner'},
            ],
        }
    }
    swap_ix = {'accounts': ['x0', 'x1', 'x2', 'x3', 'x4', VAULT_A, VAULT_B]}
    result = raydium_price_impact.calculate_price_impact(
        tx, swap_ix, 'V4', 'mintA', 'mintB', 'user_wallet', registry=mock_registry
    )
    assert result['qc_status'] == 'error'
    assert 'MINT_MISMATCH' in ''.join(result['qc_tags'])

# Edge-case: некорректные дельты (QC error)
def test_balance_change_mismatch():
    VAULT_A = 'Va11111111111111111111111111111111111111111'
    VAULT_B = 'Vb11111111111111111111111111111111111111111'
    class MockRegistry:
        def validate_vaults(self, amm_id, coin_vault, pc_vault):
            return True
    mock_registry = MockRegistry()
    tx = {
        'meta': {
            'preTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '1000000'}, 'mint': 'mintA', 'owner': 'pool_owner'},
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '2000000'}, 'mint': 'mintB', 'owner': 'pool_owner'},
            ],
            'postTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '1100000'}, 'mint': 'mintA', 'owner': 'pool_owner'},  # увеличился
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '2100000'}, 'mint': 'mintB', 'owner': 'pool_owner'},  # увеличился
            ],
        }
    }
    swap_ix = {'accounts': ['x0', 'x1', 'x2', 'x3', 'x4', VAULT_A, VAULT_B]}
    result = raydium_price_impact.calculate_price_impact(
        tx, swap_ix, 'V4', 'mintA', 'mintB', 'user_wallet', registry=mock_registry
    )
    assert result['qc_status'] == 'error'
    assert 'BALANCE_CHANGE_MISMATCH' in ''.join(result['qc_tags'])

# Edge-case: vault mismatch (QC warning, не error)
def test_vault_mismatch_warning():
    VAULT_A = 'Va11111111111111111111111111111111111111111'
    VAULT_B = 'Vb11111111111111111111111111111111111111111'
    class DummyRegistry:
        def validate_vaults(self, amm_id, coin_vault, pc_vault):
            return False
    dummy_registry = DummyRegistry()
    tx = {
        'meta': {
            'preTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '1000000'}, 'mint': 'mintA', 'owner': 'pool_owner'},
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '2000000'}, 'mint': 'mintB', 'owner': 'pool_owner'},
            ],
            'postTokenBalances': [
                {'pubkey': VAULT_A, 'uiTokenAmount': {'amount': '1100000'}, 'mint': 'mintA', 'owner': 'pool_owner'},
                {'pubkey': VAULT_B, 'uiTokenAmount': {'amount': '1900000'}, 'mint': 'mintB', 'owner': 'pool_owner'},
            ],
        }
    }
    swap_ix = {'accounts': ['x0', 'x1', 'x2', 'x3', 'x4', VAULT_A, VAULT_B]}
    result = raydium_price_impact.calculate_price_impact(
        tx, swap_ix, 'V4', 'mintA', 'mintB', 'user_wallet', registry=dummy_registry
    )
    print('QC result (vault mismatch):', result)
    assert result['qc_status'] == 'partial'
    assert 'VAULT_MISMATCH' in ''.join(result['qc_tags'])
    assert result['price_impact'] is not None

# Edge-case: CLMM пул (валидный)
def test_clmm_pool_success():
    VAULT_CLMM1 = 'Vc11111111111111111111111111111111111111111'
    VAULT_CLMM2 = 'Vd11111111111111111111111111111111111111111'
    class DummyRegistry:
        def get_pool_by_vault(self, vault):
            if vault == VAULT_CLMM1:
                return {'baseVault': VAULT_CLMM1, 'quoteVault': VAULT_CLMM2}
            return None
        def validate_vaults(self, amm_id, coin_vault, pc_vault):
            return True
    registry = DummyRegistry()
    tx = {
        'meta': {
            'preTokenBalances': [
                {'pubkey': VAULT_CLMM1, 'uiTokenAmount': {'amount': '5000000'}, 'mint': 'mintA', 'owner': 'pool_owner'},
                {'pubkey': VAULT_CLMM2, 'uiTokenAmount': {'amount': '8000000'}, 'mint': 'mintB', 'owner': 'pool_owner'},
            ],
            'postTokenBalances': [
                {'pubkey': VAULT_CLMM1, 'uiTokenAmount': {'amount': '5200000'}, 'mint': 'mintA', 'owner': 'pool_owner'},
                {'pubkey': VAULT_CLMM2, 'uiTokenAmount': {'amount': '7800000'}, 'mint': 'mintB', 'owner': 'pool_owner'},
            ],
        }
    }
    swap_ix = {'accounts': ['x0', 'x1', 'x2', 'x3', 'x4', 'x5', VAULT_CLMM1, VAULT_CLMM2]}
    result = raydium_price_impact.calculate_price_impact(
        tx, swap_ix, 'CLMM', 'mintA', 'mintB', 'user_wallet', registry=registry
    )
    print('QC result (CLMM):', result)
    assert result['qc_status'] == 'success'
    assert result['pre_pool_in'] == 5000000
    assert result['pre_pool_out'] == 8000000
    assert result['amount_in'] == 200000
    assert result['amount_out'] == 200000
    assert result['price_impact'] is not None

def test_missing_pre_balances():
    VAULT_A = 'Va11111111111111111111111111111111111111111'
    VAULT_B = 'Vb11111111111111111111111111111111111111111'
    class MockRegistry:
        def validate_vaults(self, amm_id, coin_vault, pc_vault):
            return True
    mock_registry = MockRegistry()
    tx = {'meta': {'preTokenBalances': [], 'postTokenBalances': []}}
    swap_ix = {'accounts': ['x0', 'x1', 'x2', 'x3', 'x4', VAULT_A, VAULT_B]}
    result = raydium_price_impact.calculate_price_impact(
        tx, swap_ix, 'V4', 'mintA', 'mintB', 'user_wallet', registry=mock_registry
    )
    assert result['qc_status'] == 'error'
    assert 'MISSING_PRE_BALANCES' in ''.join(result['qc_tags'])

# TODO: добавить тесты для multi-hop, временных ATA, mint mismatch, QC edge-cases 