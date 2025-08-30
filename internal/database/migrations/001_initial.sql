-- Migration 001: Initial schema for blocks and transactions

-- Blocks table
CREATE TABLE IF NOT EXISTS blocks (
    number BIGINT PRIMARY KEY,
    hash VARCHAR(66) UNIQUE NOT NULL,
    parent_hash VARCHAR(66) NOT NULL,
    timestamp BIGINT NOT NULL,
    gas_limit BIGINT,
    gas_used BIGINT,
    transaction_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Add indexes for blocks
CREATE INDEX idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX idx_blocks_parent_hash ON blocks(parent_hash);
CREATE INDEX idx_blocks_created_at ON blocks(created_at);

-- Transactions table
CREATE TABLE IF NOT EXISTS transactions (
    hash VARCHAR(66) PRIMARY KEY,
    block_number BIGINT NOT NULL REFERENCES blocks(number) ON DELETE CASCADE,
    transaction_index INT NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    value NUMERIC(78,0) DEFAULT 0,
    gas_price NUMERIC(78,0),
    gas_limit BIGINT,
    gas_used BIGINT,
    nonce BIGINT,
    input TEXT,
    status INT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Add indexes for transactions
CREATE INDEX idx_tx_block_number ON transactions(block_number);
CREATE INDEX idx_tx_from_address ON transactions(from_address);
CREATE INDEX idx_tx_to_address ON transactions(to_address);
CREATE UNIQUE INDEX idx_tx_block_index ON transactions(block_number, transaction_index);

-- Indexer state table
CREATE TABLE IF NOT EXISTS indexer_state (
    id SERIAL PRIMARY KEY,
    chain_id INT NOT NULL,
    last_block_number BIGINT NOT NULL DEFAULT 0,
    last_block_hash VARCHAR(66),
    syncing BOOLEAN DEFAULT false,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Ensure only one state entry per chain
CREATE UNIQUE INDEX idx_state_chain_id ON indexer_state(chain_id);

-- Insert initial state
INSERT INTO indexer_state (chain_id, last_block_number) 
VALUES (32769, 0) 
ON CONFLICT (chain_id) DO NOTHING;