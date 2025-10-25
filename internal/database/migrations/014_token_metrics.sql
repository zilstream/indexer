-- Migration 014: Add token metrics fields

-- Add new metrics columns to tokens table
ALTER TABLE tokens 
    ADD COLUMN IF NOT EXISTS volume_24h_usd NUMERIC(78,18) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS price_change_24h NUMERIC(10,4),  -- percentage
    ADD COLUMN IF NOT EXISTS price_change_7d NUMERIC(10,4);   -- percentage

-- Add indexes for sorting by new metrics
CREATE INDEX IF NOT EXISTS idx_tokens_volume_24h ON tokens(volume_24h_usd DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_tokens_price_change_24h ON tokens(price_change_24h DESC NULLS LAST);

-- Rename total_liquidity_usd for consistency (already exists, just documenting)
-- Note: total_liquidity_usd already exists in the schema

COMMENT ON COLUMN tokens.volume_24h_usd IS '24-hour trading volume in USD across all pairs';
COMMENT ON COLUMN tokens.price_change_24h IS 'Price change percentage over 24 hours';
COMMENT ON COLUMN tokens.price_change_7d IS 'Price change percentage over 7 days';
