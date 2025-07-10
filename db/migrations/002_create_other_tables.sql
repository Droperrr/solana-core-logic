-- db/migrations/002_create_other_tables.sql
CREATE TABLE IF NOT EXISTS instructions (
    id SERIAL PRIMARY KEY,
    signature TEXT,
    instruction_index INTEGER,
    program_id TEXT,
    instruction_type TEXT,
    raw JSONB,
    outer_index INTEGER,
    inner_index INTEGER,
    is_inner BOOLEAN,
    program_name TEXT,
    raw_data TEXT,
    raw_accounts_indices INTEGER[],
    is_parsed_by_rpc BOOLEAN,
    is_parsed_manually BOOLEAN,
    details JSONB,
    error_parsing TEXT,
    amm_id TEXT,
    pool_address TEXT,
    dex_id TEXT,
    pool_type TEXT,
    event_type TEXT,
    FOREIGN KEY (signature) REFERENCES transactions(signature) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS token_transfers (
    id SERIAL PRIMARY KEY,
    tx_signature TEXT,
    mint TEXT,
    "from" TEXT,
    "to" TEXT,
    amount NUMERIC,
    decimals INTEGER,
    slot BIGINT,
    block_time TIMESTAMP WITH TIME ZONE,
    ui_amount NUMERIC,
    FOREIGN KEY (tx_signature) REFERENCES transactions(signature) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS wallet_links (
    id SERIAL PRIMARY KEY,
    wallet_a TEXT,
    wallet_b TEXT,
    link_type TEXT,
    weight NUMERIC,
    confidence NUMERIC,
    first_seen TIMESTAMP WITH TIME ZONE,
    last_seen TIMESTAMP WITH TIME ZONE,
    source_event_ids TEXT[],
    related_token_mint TEXT,
    qc_tags TEXT[],
    version INTEGER,
    enrich_context JSONB,
    ttl INTEGER,
    tx_signature TEXT,
    source TEXT,
    context JSONB,
    src_role TEXT,
    dst_role TEXT
);

-- Добавляем очищенные таблицы для полноты
CREATE TABLE IF NOT EXISTS discovered_pools (
    pool_address TEXT NOT NULL,
    token_mint_address TEXT NOT NULL,
    dex_id TEXT,
    pool_type TEXT,
    first_seen_signature TEXT,
    last_seen_signature TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    PRIMARY KEY (pool_address, token_mint_address)
);

CREATE TABLE IF NOT EXISTS transaction_accounts (
  signature TEXT NOT NULL,
  account_index INTEGER NOT NULL,
  account_pubkey TEXT,
  is_signer BOOLEAN,
  is_writable BOOLEAN,
  PRIMARY KEY (signature, account_index),
  FOREIGN KEY (signature) REFERENCES transactions(signature) ON DELETE CASCADE
);

-- Добавляем индексы для ускорения запросов
CREATE INDEX IF NOT EXISTS idx_instructions_signature ON instructions(signature);
CREATE INDEX IF NOT EXISTS idx_token_transfers_tx_signature ON token_transfers(tx_signature);
CREATE INDEX IF NOT EXISTS idx_token_transfers_mint ON token_transfers(mint);
CREATE INDEX IF NOT EXISTS idx_wallet_links_wallets ON wallet_links(wallet_a, wallet_b);
CREATE INDEX IF NOT EXISTS idx_wallet_links_tx_signature ON wallet_links(tx_signature); 