"""
QC Checks: сюда переносятся функции-проверки из analysis/qc_check.py
"""

def check_tx_structure(tx_details: dict) -> dict:
    # Пример простой проверки структуры
    result = {'check': 'tx_structure', 'passed': True, 'details': ''}
    if not isinstance(tx_details, dict):
        result['passed'] = False
        result['details'] = 'tx_details is not a dict'
    return result

# --- DB-level QC checks ---
def check_missing_fields(conn) -> dict:
    query = '''
    SELECT 
      COUNT(*) AS total, 
      COUNT(*) FILTER (WHERE signature IS NULL) AS missing_signature,
      COUNT(*) FILTER (WHERE block_time IS NULL) AS missing_block_time,
      COUNT(*) FILTER (WHERE fee_payer IS NULL) AS missing_fee_payer
    FROM transactions;
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return {
        'check': 'missing_fields',
        'total': row[0],
        'missing_signature': row[1],
        'missing_block_time': row[2],
        'missing_fee_payer': row[3]
    }

def check_duplicate_signatures(conn) -> dict:
    query = '''
    SELECT signature, COUNT(*) 
    FROM transactions 
    GROUP BY signature 
    HAVING COUNT(*) > 1;
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        dups = cur.fetchall()
    return {
        'check': 'duplicate_signatures',
        'duplicates': [{'signature': sig, 'count': count} for sig, count in dups],
        'count': len(dups)
    }

def check_orphan_token_transfers(conn) -> dict:
    query = '''
    SELECT COUNT(*) AS orphan_transfers
    FROM token_transfers t
    LEFT JOIN transactions tx ON t.tx_signature = tx.signature
    WHERE tx.signature IS NULL;
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        count = cur.fetchone()[0]
    return {
        'check': 'orphan_token_transfers',
        'count': count
    }

def check_enrich_errors(conn) -> dict:
    query = '''
    SELECT COUNT(*) AS total, 
           COUNT(*) FILTER (WHERE additional_context ? 'enrich_errors') AS with_enrich_errors
    FROM transactions;
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
    return {
        'check': 'enrich_errors',
        'total': row[0],
        'with_enrich_errors': row[1]
    }

def check_orphan_wallet_links(conn) -> dict:
    query = '''
    SELECT COUNT(*) AS orphan_links
    FROM wallet_links wl
    LEFT JOIN transactions tx ON wl.tx_signature = tx.signature
    WHERE tx.signature IS NULL;
    '''
    with conn.cursor() as cur:
        cur.execute(query)
        count = cur.fetchone()[0]
    return {
        'check': 'orphan_wallet_links',
        'count': count
    }

# TODO: Перенести остальные проверки из analysis/qc_check.py 