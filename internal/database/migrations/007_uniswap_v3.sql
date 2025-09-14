-- Migration 007: Uniswap V3 schema

-- Factory table
CREATE TABLE IF NOT EXISTS uniswap_v3_factory (
    address VARCHAR(42) PRIMARY KEY,
    pool_count INTEGER DEFAULT 0,
    total_volume_usd NUMERIC(78,18) DEFAULT 0,
    total_liquidity_usd NUMERIC(78,18) DEFAULT 0,
    total_txns INTEGER DEFAULT 0,
    created_at_block BIGINT,
    created_at_timestamp BIGINT,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Pools table
CREATE TABLE IF NOT EXISTS uniswap_v3_pools (
    address VARCHAR(42) PRIMARY KEY,  -- pool contract address
    factory VARCHAR(42) REFERENCES uniswap_v3_factory(address),
    token0 VARCHAR(42) NOT NULL REFERENCES tokens(address),
    token1 VARCHAR(42) NOT NULL REFERENCES tokens(address),
    fee NUMERIC(78,0) DEFAULT 0,
    liquidity NUMERIC(78,0) DEFAULT 0,
    sqrt_price_x96 NUMERIC(78,0) DEFAULT 0,
    tick NUMERIC(78,0) DEFAULT 0,
    reserve0 NUMERIC(78,0) DEFAULT 0,
    reserve1 NUMERIC(78,0) DEFAULT 0,
    volume_token0 NUMERIC(78,0) DEFAULT 0,
    volume_token1 NUMERIC(78,0) DEFAULT 0,
    volume_usd NUMERIC(78,18) DEFAULT 0,
    fees_usd NUMERIC(78,18) DEFAULT 0,
    txn_count INTEGER DEFAULT 0,
    created_at_block BIGINT,
    created_at_timestamp BIGINT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pools_token0 ON uniswap_v3_pools(token0);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pools_token1 ON uniswap_v3_pools(token1);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_pools_created_block ON uniswap_v3_pools(created_at_block);

-- Swaps
CREATE TABLE IF NOT EXISTS uniswap_v3_swaps (
    id VARCHAR(130) PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    pool VARCHAR(42) NOT NULL REFERENCES uniswap_v3_pools(address),
    sender VARCHAR(42) NOT NULL,
    recipient VARCHAR(42) NOT NULL,
    amount0 NUMERIC(78,0) DEFAULT 0,
    amount1 NUMERIC(78,0) DEFAULT 0,
    sqrt_price_x96 NUMERIC(78,0) DEFAULT 0,
    liquidity NUMERIC(78,0) DEFAULT 0,
    tick NUMERIC(78,0) DEFAULT 0,
    amount_usd NUMERIC(78,18),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_swaps_pool ON uniswap_v3_swaps(pool);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_swaps_block ON uniswap_v3_swaps(block_number);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_swaps_timestamp ON uniswap_v3_swaps(timestamp DESC);

-- Mints
CREATE TABLE IF NOT EXISTS uniswap_v3_mints (
    id VARCHAR(130) PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    pool VARCHAR(42) NOT NULL REFERENCES uniswap_v3_pools(address),
    owner VARCHAR(42) NOT NULL,
    tick_lower NUMERIC(78,0) DEFAULT 0,
    tick_upper NUMERIC(78,0) DEFAULT 0,
    amount NUMERIC(78,0) DEFAULT 0,
    amount0 NUMERIC(78,0) DEFAULT 0,
    amount1 NUMERIC(78,0) DEFAULT 0,
    amount_usd NUMERIC(78,18),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_mints_pool ON uniswap_v3_mints(pool);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_mints_block ON uniswap_v3_mints(block_number);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_mints_owner ON uniswap_v3_mints(owner);

-- Burns
CREATE TABLE IF NOT EXISTS uniswap_v3_burns (
    id VARCHAR(130) PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    pool VARCHAR(42) NOT NULL REFERENCES uniswap_v3_pools(address),
    owner VARCHAR(42) NOT NULL,
    tick_lower NUMERIC(78,0) DEFAULT 0,
    tick_upper NUMERIC(78,0) DEFAULT 0,
    amount NUMERIC(78,0) DEFAULT 0,
    amount0 NUMERIC(78,0) DEFAULT 0,
    amount1 NUMERIC(78,0) DEFAULT 0,
    amount_usd NUMERIC(78,18),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_burns_pool ON uniswap_v3_burns(pool);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_burns_block ON uniswap_v3_burns(block_number);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_burns_owner ON uniswap_v3_burns(owner);

-- Collects
CREATE TABLE IF NOT EXISTS uniswap_v3_collects (
    id VARCHAR(130) PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL,
    log_index INTEGER NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    pool VARCHAR(42) NOT NULL REFERENCES uniswap_v3_pools(address),
    owner VARCHAR(42) NOT NULL,
    recipient VARCHAR(42) NOT NULL,
    tick_lower NUMERIC(78,0) DEFAULT 0,
    tick_upper NUMERIC(78,0) DEFAULT 0,
    amount0 NUMERIC(78,0) DEFAULT 0,
    amount1 NUMERIC(78,0) DEFAULT 0,
    amount_usd NUMERIC(78,18),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_uniswap_v3_collects_pool ON uniswap_v3_collects(pool);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_collects_block ON uniswap_v3_collects(block_number);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_collects_owner ON uniswap_v3_collects(owner);
