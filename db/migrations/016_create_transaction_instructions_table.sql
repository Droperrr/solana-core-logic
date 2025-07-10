CREATE TABLE transaction_instructions (
    id BIGSERIAL PRIMARY KEY,
    signature TEXT NOT NULL,
    instruction_index SMALLINT NOT NULL,
    outer_instruction_index SMALLINT NOT NULL,
    inner_instruction_index SMALLINT,
    is_inner BOOLEAN NOT NULL,
    program_id TEXT NOT NULL,
    instruction_type TEXT NOT NULL,
    details JSONB
);

CREATE INDEX idx_instructions_signature ON transaction_instructions (signature);
CREATE INDEX idx_instructions_program_id ON transaction_instructions (program_id);
CREATE INDEX idx_instructions_type ON transaction_instructions (instruction_type);
CREATE INDEX idx_instructions_sequence ON transaction_instructions (signature, instruction_index);
CREATE INDEX idx_instructions_details_gin ON transaction_instructions USING GIN (details); 