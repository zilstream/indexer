-- Migration: Backfill timestamps for existing swap events
-- This updates all swap events that have timestamp=0 with the correct timestamp from blocks table

-- Update UniswapV2 swaps
UPDATE uniswap_v2_swaps s
SET timestamp = b.timestamp
FROM blocks b
WHERE s.block_number = b.number
  AND s.timestamp = 0;

-- Update UniswapV3 swaps
UPDATE uniswap_v3_swaps s
SET timestamp = b.timestamp
FROM blocks b
WHERE s.block_number = b.number
  AND s.timestamp = 0;

-- Create index on timestamp for better query performance on 24h volumes
CREATE INDEX IF NOT EXISTS idx_uniswap_v2_swaps_timestamp ON uniswap_v2_swaps(timestamp) WHERE timestamp > 0;
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_swaps_timestamp ON uniswap_v3_swaps(timestamp) WHERE timestamp > 0;
CREATE INDEX IF NOT EXISTS idx_uniswap_v2_swaps_pair_timestamp ON uniswap_v2_swaps(pair, timestamp);
CREATE INDEX IF NOT EXISTS idx_uniswap_v3_swaps_pool_timestamp ON uniswap_v3_swaps(pool, timestamp);
