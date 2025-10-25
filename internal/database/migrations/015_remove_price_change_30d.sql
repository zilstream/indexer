-- Migration 015: Remove price_change_30d column (deployed in error)

-- Drop the 30d price change column if it exists
ALTER TABLE tokens 
    DROP COLUMN IF EXISTS price_change_30d;
