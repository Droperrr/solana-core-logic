import psycopg2
import os
import json

DB_PARAMS = dict(
    dbname=os.environ.get('SOLANA_DB', 'solana_db'),
    user=os.environ.get('SOLANA_DB_USER', 'bantbot'),
    password=os.environ.get('SOLANA_DB_PASS', 'test'),
    host=os.environ.get('SOLANA_DB_HOST', 'localhost'),
)

WSOL_MINT = 'So11111111111111111111111111111111111111112'

SQL = """
SELECT
  signature,
  block_time,
  (additional_context->'swap_summary'->'input_token'->>'amount')::numeric /
    pow(10, COALESCE((additional_context->'swap_summary'->'input_token'->>'decimals')::int, 9)) AS sol_spent,
  additional_context->'swap_summary'->'input_token'->>'mint' AS input_mint,
  additional_context->'swap_summary'->'output_token'->>'mint' AS output_mint
FROM transactions
WHERE additional_context->'swap_summary'->'input_token'->>'mint' = %s
  AND (additional_context->'swap_summary'->'input_token'->>'amount') IS NOT NULL
ORDER BY sol_spent DESC
LIMIT 1;
"""

def find_largest_sol_buy():
    print("\n=== Самая крупная покупка за SOL/WSOL (по amount в swaps.token_in_amount) ===")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print(f"Ошибка подключения к базе: {e}")
        return
    cur = conn.cursor()
    cur.execute("SELECT signature, block_time, additional_context FROM transactions WHERE transaction_type = 'buy' AND additional_context IS NOT NULL")
    rows = cur.fetchall()
    max_amt = 0
    max_info = None
    for sig, bt, ctx in rows:
        try:
            ctx = ctx if isinstance(ctx, dict) else json.loads(ctx)
            swaps = ctx.get('swaps', [])
            for swap in swaps:
                in_mint = swap.get('token_in_mint', {}).get('pubkey')
                amt = swap.get('token_in_amount')
                dec = swap.get('token_in_decimals')
                out_mint = swap.get('token_out_mint', {}).get('pubkey')
                if in_mint == WSOL_MINT and amt and dec is not None:
                    amt_sol = float(amt) / (10 ** int(dec))
                    if amt_sol > max_amt:
                        max_amt = amt_sol
                        max_info = (sig, bt, amt_sol, in_mint, out_mint)
        except Exception as e:
            continue
    if max_info:
        sig, bt, amt_sol, in_mint, out_mint = max_info
        print(f"Signature:   {sig}\nBlock Time:  {bt}\nSOL Spent:   {amt_sol:.6f}\nInput Mint:  {in_mint}\nOutput Mint: {out_mint}")
    else:
        print("Нет данных.")
    cur.close()
    conn.close()

def print_db_overview():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    print('=== Структура базы данных ===')
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
    tables = [r[0] for r in cur.fetchall()]
    for t in tables:
        cur.execute(f'SELECT COUNT(*) FROM {t}')
        cnt = cur.fetchone()[0]
        cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s", (t,))
        cols = cur.fetchall()
        print(f'\nТаблица: {t} ({cnt} записей)')
        print('  Столбцы:')
        for col, typ in cols:
            print(f'    {col:30} {typ}')
    cur.close()
    conn.close()

def print_transactions_count():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    cur.execute('SELECT COUNT(*) FROM transactions')
    cnt = cur.fetchone()[0]
    print(f'\n=== Всего транзакций в базе: {cnt} ===')
    cur.close()
    conn.close()

def print_sample_contexts():
    print("\n=== Примеры additional_context из transactions ===")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print(f"Ошибка подключения к базе: {e}")
        return
    cur = conn.cursor()
    cur.execute("SELECT signature, transaction_type, left(cast(additional_context as text), 300) FROM transactions WHERE additional_context IS NOT NULL LIMIT 5")
    rows = cur.fetchall()
    for sig, ttype, ctx in rows:
        print(f"\nSignature: {sig}\nType: {ttype}\nContext: {ctx}\n---")
    cur.close()
    conn.close()

def print_buy_context_example():
    print("\n=== Пример additional_context для транзакции типа 'buy' ===")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print(f"Ошибка подключения к базе: {e}")
        return
    cur = conn.cursor()
    cur.execute("SELECT signature, left(cast(additional_context as text), 1000) FROM transactions WHERE transaction_type = 'buy' AND additional_context IS NOT NULL LIMIT 1")
    row = cur.fetchone()
    if row:
        sig, ctx = row
        print(f"Signature: {sig}\nContext: {ctx}\n---")
    else:
        print("Нет транзакций типа 'buy' с additional_context.")
    cur.close()
    conn.close()

def print_buy_swaps_with_amount():
    print("\n=== Примеры swaps с не-null amount для транзакций типа 'buy' ===")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print(f"Ошибка подключения к базе: {e}")
        return
    cur = conn.cursor()
    cur.execute("SELECT signature, additional_context FROM transactions WHERE transaction_type = 'buy' AND additional_context IS NOT NULL LIMIT 50")
    rows = cur.fetchall()
    shown = 0
    for sig, ctx in rows:
        try:
            ctx = ctx if isinstance(ctx, dict) else json.loads(ctx)
            swaps = ctx.get('swaps', [])
            for swap in swaps:
                in_mint = swap.get('token_in_mint', {}).get('pubkey')
                amt = swap.get('token_in_amount')
                dec = swap.get('token_in_decimals')
                out_mint = swap.get('token_out_mint', {}).get('pubkey')
                out_amt = swap.get('token_out_amount')
                out_dec = swap.get('token_out_decimals')
                if amt is not None:
                    print(f"Signature: {sig}\n  token_in_mint:  {in_mint}\n  token_in_amount: {amt}\n  token_in_decimals: {dec}\n  token_out_mint: {out_mint}\n  token_out_amount: {out_amt}\n  token_out_decimals: {out_dec}\n---")
                    shown += 1
                    if shown >= 5:
                        cur.close()
                        conn.close()
                        return
        except Exception as e:
            continue
    cur.close()
    conn.close()

def print_full_transaction_info(signature):
    print(f"\n=== Полная информация о транзакции {signature} ===")
    try:
        conn = psycopg2.connect(**DB_PARAMS)
    except Exception as e:
        print(f"Ошибка подключения к базе: {e}")
        return
    cur = conn.cursor()
    cur.execute("SELECT * FROM transactions WHERE signature = %s", (signature,))
    row = cur.fetchone()
    if row:
        desc = [d[0] for d in cur.description]
        for k, v in zip(desc, row):
            print(f"{k}: {v}\n")
    else:
        print("Нет такой транзакции.")
    cur.close()
    conn.close()

if __name__ == '__main__':
    print_full_transaction_info('2WjgSSiEnMNTynHFH5voUepgeB7RhbXDCK7ZZPwjKAMmBsmJiiJn5kcvT2aKsQuiJrHSzhWmNtJEDZYie9nE7RAz')
    print_sample_contexts()
    print_buy_context_example()
    print_buy_swaps_with_amount()
    find_largest_sol_buy()
    print_db_overview()
    print_transactions_count() 