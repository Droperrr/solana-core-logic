-- db/migrations/001_create_transactions_table.sql
CREATE TABLE IF NOT EXISTS transactions (
    signature TEXT PRIMARY KEY,
    block_time TIMESTAMP WITH TIME ZONE,
    slot BIGINT,
    fee_payer TEXT,
    fee NUMERIC,
    error JSONB,
    meta_raw JSONB,
    message_raw JSONB,
    transaction_type TEXT,
    source_query_type TEXT,
    source_query_address TEXT,
    detected_patterns TEXT[],
    involved_platforms TEXT[],
    involved_tokens_net_flow JSONB,
    additional_context JSONB,
    loaded_timestamp TIMESTAMP WITH TIME ZONE DEFAULT (NOW() AT TIME ZONE 'utc'),
    priority_micro_lamports NUMERIC,
    compute_unit_limit BIGINT,
    units_consumed BIGINT,
    transaction_version INTEGER,
    raw_tx_details JSONB,
    analysis_direction TEXT,
    analysis_tags TEXT,
    priority_fee_microlamports NUMERIC,
    exchange_details JSONB,
    is_complex_swap BOOLEAN,
    is_direction_ambiguous BOOLEAN,
    instruction_types_str TEXT,
    units_consumed_diff NUMERIC,
    is_sniper BOOLEAN,
    bad_sniper BOOLEAN,
    priority_fee_per_cu NUMERIC,
    priority_fee NUMERIC,
    raw_json JSONB,
    swap_instruction_location TEXT
);

CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time);
CREATE INDEX IF NOT EXISTS idx_transactions_slot ON transactions(slot);
CREATE INDEX IF NOT EXISTS idx_transactions_fee_payer ON transactions(fee_payer);
CREATE INDEX IF NOT EXISTS idx_transactions_transaction_type ON transactions(transaction_type); 