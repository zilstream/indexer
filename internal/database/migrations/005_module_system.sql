-- Migration 005: Module system and universal entities

-- Module state tracking table
CREATE TABLE IF NOT EXISTS module_state (
    module_name VARCHAR(50) PRIMARY KEY,
    version VARCHAR(20) NOT NULL,
    last_processed_block BIGINT DEFAULT 0,
    status VARCHAR(20) DEFAULT 'active', -- active, backfilling, paused, error
    backfill_from_block BIGINT,
    backfill_to_block BIGINT,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create index for efficient status queries
CREATE INDEX idx_module_state_status ON module_state(status);
CREATE INDEX idx_module_state_updated_at ON module_state(updated_at);

-- Universal tokens table (shared across all modules)
CREATE TABLE IF NOT EXISTS tokens (
    address VARCHAR(42) PRIMARY KEY,
    name VARCHAR(255),
    symbol VARCHAR(50),
    decimals INTEGER,
    total_supply NUMERIC(78,0),
    -- Pricing and liquidity metrics (can be updated by different modules)
    price_usd NUMERIC(78,18),
    price_eth NUMERIC(78,18),
    total_liquidity_usd NUMERIC(78,18) DEFAULT 0,
    total_volume_usd NUMERIC(78,18) DEFAULT 0,
    market_cap_usd NUMERIC(78,18),
    -- Metadata
    logo_uri TEXT,
    website TEXT,
    description TEXT,
    -- Tracking
    first_seen_block BIGINT,
    first_seen_timestamp BIGINT,
    last_updated_block BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for tokens table
CREATE INDEX idx_tokens_symbol ON tokens(symbol);
CREATE INDEX idx_tokens_name ON tokens(name);
CREATE INDEX idx_tokens_market_cap ON tokens(market_cap_usd DESC NULLS LAST);
CREATE INDEX idx_tokens_volume ON tokens(total_volume_usd DESC NULLS LAST);
CREATE INDEX idx_tokens_first_seen ON tokens(first_seen_block);

-- Uniswap V2 Factory entity
CREATE TABLE IF NOT EXISTS uniswap_v2_factory (
    address VARCHAR(42) PRIMARY KEY,
    pair_count INTEGER DEFAULT 0,
    total_volume_usd NUMERIC(78,18) DEFAULT 0,
    total_liquidity_usd NUMERIC(78,18) DEFAULT 0,
    total_txns INTEGER DEFAULT 0,
    created_at_block BIGINT,
    created_at_timestamp BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Uniswap V2 Pairs table
CREATE TABLE IF NOT EXISTS uniswap_v2_pairs (
    address VARCHAR(42) PRIMARY KEY,  -- pair contract address
    factory VARCHAR(42) REFERENCES uniswap_v2_factory(address),
    token0 VARCHAR(42) NOT NULL REFERENCES tokens(address),
    token1 VARCHAR(42) NOT NULL REFERENCES tokens(address),
    -- Current reserves
    reserve0 NUMERIC(78,0) DEFAULT 0,
    reserve1 NUMERIC(78,0) DEFAULT 0,
    total_supply NUMERIC(78,0) DEFAULT 0, -- LP token supply
    -- USD values
    reserve_usd NUMERIC(78,18) DEFAULT 0,
    -- Volume tracking
    volume_token0 NUMERIC(78,0) DEFAULT 0,
    volume_token1 NUMERIC(78,0) DEFAULT 0,
    volume_usd NUMERIC(78,18) DEFAULT 0,
    -- Transaction count
    txn_count INTEGER DEFAULT 0,
    -- Creation info
    created_at_block BIGINT NOT NULL,
    created_at_timestamp BIGINT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for pairs
CREATE INDEX idx_uniswap_v2_pairs_token0 ON uniswap_v2_pairs(token0);
CREATE INDEX idx_uniswap_v2_pairs_token1 ON uniswap_v2_pairs(token1);
CREATE INDEX idx_uniswap_v2_pairs_reserve_usd ON uniswap_v2_pairs(reserve_usd DESC);
CREATE INDEX idx_uniswap_v2_pairs_volume_usd ON uniswap_v2_pairs(volume_usd DESC);
CREATE INDEX idx_uniswap_v2_pairs_created_block ON uniswap_v2_pairs(created_at_block);
-- Composite index for token pair lookups
CREATE UNIQUE INDEX idx_uniswap_v2_pairs_tokens ON uniswap_v2_pairs(token0, token1);

-- Uniswap V2 Swaps table (individual swap transactions)
CREATE TABLE IF NOT EXISTS uniswap_v2_swaps (
    id VARCHAR(130) PRIMARY KEY, -- transaction_hash-log_index format
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    block_hash VARCHAR(66) NOT NULL,
    timestamp BIGINT NOT NULL,
    -- Pair and user info
    pair VARCHAR(42) NOT NULL REFERENCES uniswap_v2_pairs(address),
    sender VARCHAR(42) NOT NULL,
    recipient VARCHAR(42) NOT NULL,
    -- Swap amounts
    amount0_in NUMERIC(78,0) DEFAULT 0,
    amount1_in NUMERIC(78,0) DEFAULT 0,
    amount0_out NUMERIC(78,0) DEFAULT 0,
    amount1_out NUMERIC(78,0) DEFAULT 0,
    -- USD value
    amount_usd NUMERIC(78,18),
    -- Gas info
    gas_used BIGINT,
    gas_price NUMERIC(78,0),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for swaps
CREATE INDEX idx_uniswap_v2_swaps_pair ON uniswap_v2_swaps(pair);
CREATE INDEX idx_uniswap_v2_swaps_block ON uniswap_v2_swaps(block_number);
CREATE INDEX idx_uniswap_v2_swaps_timestamp ON uniswap_v2_swaps(timestamp DESC);
CREATE INDEX idx_uniswap_v2_swaps_sender ON uniswap_v2_swaps(sender);
CREATE INDEX idx_uniswap_v2_swaps_recipient ON uniswap_v2_swaps(recipient);
CREATE INDEX idx_uniswap_v2_swaps_amount_usd ON uniswap_v2_swaps(amount_usd DESC NULLS LAST);
CREATE INDEX idx_uniswap_v2_swaps_tx_hash ON uniswap_v2_swaps(transaction_hash);

-- Uniswap V2 Mints (liquidity additions)
CREATE TABLE IF NOT EXISTS uniswap_v2_mints (
    id VARCHAR(130) PRIMARY KEY, -- transaction_hash-log_index format
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    -- Pair and user info
    pair VARCHAR(42) NOT NULL REFERENCES uniswap_v2_pairs(address),
    to_address VARCHAR(42) NOT NULL,
    sender VARCHAR(42) NOT NULL,
    -- Liquidity amounts
    amount0 NUMERIC(78,0) DEFAULT 0,
    amount1 NUMERIC(78,0) DEFAULT 0,
    liquidity NUMERIC(78,0) DEFAULT 0, -- LP tokens minted
    -- USD value
    amount_usd NUMERIC(78,18),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for mints
CREATE INDEX idx_uniswap_v2_mints_pair ON uniswap_v2_mints(pair);
CREATE INDEX idx_uniswap_v2_mints_block ON uniswap_v2_mints(block_number);
CREATE INDEX idx_uniswap_v2_mints_timestamp ON uniswap_v2_mints(timestamp DESC);
CREATE INDEX idx_uniswap_v2_mints_to ON uniswap_v2_mints(to_address);

-- Uniswap V2 Burns (liquidity removals)
CREATE TABLE IF NOT EXISTS uniswap_v2_burns (
    id VARCHAR(130) PRIMARY KEY, -- transaction_hash-log_index format
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    -- Pair and user info
    pair VARCHAR(42) NOT NULL REFERENCES uniswap_v2_pairs(address),
    to_address VARCHAR(42) NOT NULL,
    sender VARCHAR(42) NOT NULL,
    -- Liquidity amounts
    amount0 NUMERIC(78,0) DEFAULT 0,
    amount1 NUMERIC(78,0) DEFAULT 0,
    liquidity NUMERIC(78,0) DEFAULT 0, -- LP tokens burned
    -- USD value
    amount_usd NUMERIC(78,18),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for burns
CREATE INDEX idx_uniswap_v2_burns_pair ON uniswap_v2_burns(pair);
CREATE INDEX idx_uniswap_v2_burns_block ON uniswap_v2_burns(block_number);
CREATE INDEX idx_uniswap_v2_burns_timestamp ON uniswap_v2_burns(timestamp DESC);
CREATE INDEX idx_uniswap_v2_burns_to ON uniswap_v2_burns(to_address);

-- Sync events for tracking reserve changes
CREATE TABLE IF NOT EXISTS uniswap_v2_syncs (
    id VARCHAR(130) PRIMARY KEY, -- transaction_hash-log_index format
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    -- Pair info
    pair VARCHAR(42) NOT NULL REFERENCES uniswap_v2_pairs(address),
    -- New reserves after sync
    reserve0 NUMERIC(78,0) NOT NULL,
    reserve1 NUMERIC(78,0) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for syncs
CREATE INDEX idx_uniswap_v2_syncs_pair ON uniswap_v2_syncs(pair);
CREATE INDEX idx_uniswap_v2_syncs_block ON uniswap_v2_syncs(block_number);
CREATE INDEX idx_uniswap_v2_syncs_timestamp ON uniswap_v2_syncs(timestamp DESC);

-- Views for common queries

-- Daily volume aggregation
CREATE OR REPLACE VIEW uniswap_v2_daily_volume AS
SELECT 
    pair,
    DATE(to_timestamp(timestamp)) as date,
    SUM(amount_usd) as volume_usd,
    COUNT(*) as swap_count
FROM uniswap_v2_swaps
WHERE amount_usd IS NOT NULL
GROUP BY pair, DATE(to_timestamp(timestamp))
ORDER BY date DESC;

-- Token liquidity across all pairs
CREATE OR REPLACE VIEW token_liquidity AS
SELECT 
    t.address,
    t.symbol,
    t.name,
    SUM(CASE 
        WHEN p.token0 = t.address THEN p.reserve0 * POWER(10, -COALESCE(t.decimals, 18))
        WHEN p.token1 = t.address THEN p.reserve1 * POWER(10, -COALESCE(t.decimals, 18))
        ELSE 0 
    END) as total_liquidity_normalized
FROM tokens t
LEFT JOIN uniswap_v2_pairs p ON (p.token0 = t.address OR p.token1 = t.address)
GROUP BY t.address, t.symbol, t.name
ORDER BY total_liquidity_normalized DESC NULLS LAST;

-- Comments
COMMENT ON TABLE module_state IS 'Tracks processing state for each indexer module';
COMMENT ON TABLE tokens IS 'Universal token registry shared across all modules';
COMMENT ON TABLE uniswap_v2_pairs IS 'Uniswap V2 trading pairs with reserves and metrics';
COMMENT ON TABLE uniswap_v2_swaps IS 'Individual token swaps on Uniswap V2';
COMMENT ON TABLE uniswap_v2_mints IS 'Liquidity additions to Uniswap V2 pairs';
COMMENT ON TABLE uniswap_v2_burns IS 'Liquidity removals from Uniswap V2 pairs';
COMMENT ON TABLE uniswap_v2_syncs IS 'Reserve synchronization events';