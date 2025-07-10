from typing import List, Dict, Any

def detect_exact_first_buys(token_address: str, db_connection) -> List[Dict[str, Any]]:
    """
    Находит точные первые покупки токена по всем кошелькам.
    Возвращает список словарей с wallet, signature, block_time, amount и slot.
    """
    query = '''
    WITH ranked_buys AS (
        SELECT
            t."to" AS wallet,
            t.tx_signature AS signature,
            t.block_time,
            t.amount,
            t.slot,
            ROW_NUMBER() OVER (PARTITION BY t."to" ORDER BY t.block_time ASC) as rn
        FROM token_transfers t
        WHERE t.mint = %s AND t.amount > 0
    )
    SELECT wallet, signature, block_time, amount, slot
    FROM ranked_buys
    WHERE rn = 1
    ORDER BY block_time ASC;
    '''
    with db_connection.cursor() as cur:
        cur.execute(query, (token_address,))
        rows = cur.fetchall()
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in rows] 