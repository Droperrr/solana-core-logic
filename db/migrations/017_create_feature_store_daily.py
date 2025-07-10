def upgrade(conn):
    with conn.cursor() as cur:
        cur.execute('''
            CREATE TABLE IF NOT EXISTS feature_store_daily (
                token_address TEXT NOT NULL,
                date DATE NOT NULL,
                daily_volume NUMERIC,
                transaction_count NUMERIC,
                unique_wallets NUMERIC,
                buy_sell_ratio NUMERIC,
                gini_coefficient NUMERIC,
                concentration_top5 NUMERIC,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),
                PRIMARY KEY (token_address, date)
            );
        ''')
        conn.commit()

def downgrade(conn):
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS feature_store_daily;')
        conn.commit() 