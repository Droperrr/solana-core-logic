import sys
import os
import json
import datetime
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from db import db_writer

class DummyConn:
    def cursor(self):
        class DummyCursor:
            def __enter__(self):
                return self
            def __exit__(self, exc_type, exc_val, exc_tb):
                return False
            def execute(self, *a, **kw):
                print(f"[DUMMY EXECUTE] {a[0][:60]} ...")
            def close(self):
                pass
            def fetchone(self):
                return None
        return DummyCursor()
    def commit(self):
        pass
    def rollback(self):
        pass

def test_gmgn_fee_wallet_link():
    conn = DummyConn()
    tx_data = {
        'signature': 'TESTSIG123',
        'block_time': int(datetime.datetime.now().timestamp()),
        'meta': {},
        'transaction': {'message': {'accountKeys': ['SRC', 'DST']}},
        'slot': 123456,
    }
    additional_context = {
        'has_gmgn_fee': True,
        'gmgn_fee': {
            'source': 'SRC',
            'destination': db_writer.GMGN_FEES_VAULT,
            'amount_lamports': 12345,
            'signature': 'TESTSIG123',
        }
    }
    print('--- RUNNING save_parsed_transaction WITH GMGN FEE ---')
    db_writer.save_parsed_transaction(
        conn,
        tx_data,
        current_token_mint='MINT',
        transaction_type='swap',
        source_query_type='token',
        source_query_address='SRC',
        detected_patterns=None,
        involved_platforms=None,
        net_token_flows_json=None,
        additional_context=additional_context,
        parsed_tx_version=0
    )
    print('--- TEST END ---')

test_gmgn_fee_wallet_link() 