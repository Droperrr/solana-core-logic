import unittest
from unittest.mock import patch
from processing import transaction_processor as tp

MOCK_SIGNATURE = 'testsig123'
MOCK_TOKEN = 'So11111111111111111111111111111111111111112'

MOCK_TX_DETAILS = {
    'transaction': {'message': {'accountKeys': ['payer1']}},
    'meta': {
        'fee': 10000,
        'computeUnitsConsumed': 12345,
        'logMessages': [
            'Program X consumed 12345 units',
            'instruction: BuyRaydium',
            'max_buy_reached'
        ],
        'preTokenBalances': [
            {
                'mint': MOCK_TOKEN,
                'owner': 'recipient1',
                'accountIndex': 1,
                'uiTokenAmount': {'amount': '0', 'decimals': 9}
            }
        ],
        'postTokenBalances': [
            {
                'mint': MOCK_TOKEN,
                'owner': 'recipient1',
                'accountIndex': 1,
                'uiTokenAmount': {'amount': '1000', 'decimals': 9}
            }
        ]
    },
    'signature': MOCK_SIGNATURE,
    'blockTime': 1234567890,
    'slot': 42
}

MOCK_PARSED_INSTRUCTIONS = [
    {
        'program_id': 'ComputeBudget111111111111111111111111111111',
        'instruction_type': 'setComputeUnitPrice',
        'details': {'manual_parsed_args': {'micro_lamports': 40000}},
        'program_name': 'Compute Budget Program'
    },
    {
        'program_id': 'ComputeBudget111111111111111111111111111111',
        'instruction_type': 'setComputeUnitLimit',
        'details': {'manual_parsed_args': {'units': 130803}},
        'program_name': 'Compute Budget Program'
    },
    {
        'program_id': tp.SPL_TOKEN_PROGRAM_ID,
        'instruction_type': 'transfer',
        'details': {'identified_transfer_destination': 'recipient1'}
    },
    {
        'program_id': tp.SPL_TOKEN_PROGRAM_ID,
        'instruction_type': 'closeAccount',
        'details': {}
    }
]

class TestTransactionProcessor(unittest.TestCase):
    @patch('processing.transaction_processor.get_transaction', return_value=MOCK_TX_DETAILS)
    @patch('processing.transaction_processor.parse_transaction_instructions', return_value=MOCK_PARSED_INSTRUCTIONS)
    @patch('processing.transaction_processor.is_valid_solana_address', return_value=True)
    def test_additional_context_fields(self, mock_valid_addr, mock_parse_instr, mock_get_tx):
        result = tp.process_single_transaction(MOCK_SIGNATURE, MOCK_TOKEN)
        self.assertIsNotNone(result)
        additional_context = result[-1]
        self.assertIsInstance(additional_context, dict)
        # Проверяем все признаки из плана
        self.assertIn('is_sniper', additional_context)
        self.assertIn('bad_sniper', additional_context)
        self.assertIn('token_recipients', additional_context)
        self.assertIn('closed_token_accounts', additional_context)
        self.assertIn('priority_fee_per_cu', additional_context)
        self.assertIn('recipients', additional_context)
        self.assertIn('compute_unit_limit', additional_context)
        self.assertIn('units_consumed_meta', additional_context)
        self.assertIn('units_consumed_log', additional_context)
        # patterns и sol_burned могут быть пустыми, но ключи должны появляться при наличии данных

if __name__ == '__main__':
    unittest.main() 