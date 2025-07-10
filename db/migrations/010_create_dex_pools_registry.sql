CREATE TABLE IF NOT EXISTS dex_pools_registry (
    pool_address TEXT PRIMARY KEY,
    dex_name TEXT,
    token_a_mint TEXT,
    token_b_mint TEXT,
    token_a_vault TEXT,
    token_b_vault TEXT,
    lp_mint TEXT,
    initial_liquidity_provider TEXT,
    last_updated INTEGER
); 