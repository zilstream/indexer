-- 016_price_chart_indexes.sql
-- Add composite indexes for efficient time-based lookups in price chart queries

-- Index for V2 sync lookups: find latest sync before a timestamp for a given pair
CREATE INDEX IF NOT EXISTS idx_uniswap_v2_syncs_pair_ts_desc 
ON uniswap_v2_syncs(pair, timestamp DESC);

-- Index for V3 swap lookups: find latest swap before a timestamp for a given pool
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_swaps_pool_ts_desc 
ON uniswap_v3_swaps(pool, timestamp DESC);
