-- Migration 017: Add price change percentages to pairs

-- Add price change columns to uniswap_v2_pairs
ALTER TABLE uniswap_v2_pairs
ADD COLUMN IF NOT EXISTS price_change_24h NUMERIC(10,4),
ADD COLUMN IF NOT EXISTS price_change_7d NUMERIC(10,4);

-- Add price change columns to uniswap_v3_pools
ALTER TABLE uniswap_v3_pools
ADD COLUMN IF NOT EXISTS price_change_24h NUMERIC(10,4),
ADD COLUMN IF NOT EXISTS price_change_7d NUMERIC(10,4);

-- Add indexes for sorting
CREATE INDEX IF NOT EXISTS idx_v2_pairs_price_change_24h ON uniswap_v2_pairs(price_change_24h DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_v2_pairs_price_change_7d ON uniswap_v2_pairs(price_change_7d DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_v3_pools_price_change_24h ON uniswap_v3_pools(price_change_24h DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_v3_pools_price_change_7d ON uniswap_v3_pools(price_change_7d DESC NULLS LAST);

-- Comments
COMMENT ON COLUMN uniswap_v2_pairs.price_change_24h IS 'Price change percentage over 24 hours';
COMMENT ON COLUMN uniswap_v2_pairs.price_change_7d IS 'Price change percentage over 7 days';
COMMENT ON COLUMN uniswap_v3_pools.price_change_24h IS 'Price change percentage over 24 hours';
COMMENT ON COLUMN uniswap_v3_pools.price_change_7d IS 'Price change percentage over 7 days';
