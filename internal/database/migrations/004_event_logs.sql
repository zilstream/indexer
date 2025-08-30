-- Event logs table for storing contract events
CREATE TABLE IF NOT EXISTS event_logs (
    id BIGSERIAL PRIMARY KEY,
    
    -- Block and transaction context
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    transaction_index INTEGER NOT NULL,
    log_index INTEGER NOT NULL,
    
    -- Contract information
    address VARCHAR(42) NOT NULL,
    
    -- Event data
    topics JSONB NOT NULL, -- Array of topic hashes
    data TEXT NOT NULL,     -- Hex encoded event data
    
    -- Indexing metadata
    removed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    -- Composite unique constraint to prevent duplicates
    CONSTRAINT unique_log UNIQUE (block_number, transaction_index, log_index)
);

-- Indexes for efficient querying
CREATE INDEX idx_event_logs_block_number ON event_logs(block_number);
CREATE INDEX idx_event_logs_transaction_hash ON event_logs(transaction_hash);
CREATE INDEX idx_event_logs_address ON event_logs(address);
CREATE INDEX idx_event_logs_topics ON event_logs USING GIN(topics);
CREATE INDEX idx_event_logs_block_tx_idx ON event_logs(block_number, transaction_index);

-- Common ERC-20 event signatures for reference
COMMENT ON TABLE event_logs IS 'Stores all event logs emitted by smart contracts';
COMMENT ON COLUMN event_logs.topics IS 'Array of indexed topics, first topic is event signature hash';
COMMENT ON COLUMN event_logs.data IS 'Non-indexed event parameters in hex format';

-- Create a view for decoded ERC-20 Transfer events (topic0 = 0xddf252ad...)
CREATE OR REPLACE VIEW erc20_transfer_events AS
SELECT 
    el.*,
    topics->1 AS from_address,
    topics->2 AS to_address,
    decode(substring(data from 3), 'hex') AS amount_bytes
FROM event_logs el
WHERE topics->0 = '"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"'
  AND jsonb_array_length(topics) >= 3;

-- Create a view for decoded ERC-20 Approval events (topic0 = 0x8c5be1e5...)
CREATE OR REPLACE VIEW erc20_approval_events AS
SELECT 
    el.*,
    topics->1 AS owner_address,
    topics->2 AS spender_address,
    decode(substring(data from 3), 'hex') AS amount_bytes
FROM event_logs el
WHERE topics->0 = '"0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925"'
  AND jsonb_array_length(topics) >= 3;