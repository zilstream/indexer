-- Migration 003: Add gap tracking and validation tables

-- Create sync_gaps table to track detected gaps
CREATE TABLE IF NOT EXISTS sync_gaps (
    id SERIAL PRIMARY KEY,
    start_block BIGINT NOT NULL,
    end_block BIGINT NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    filled_at TIMESTAMP,
    attempts INTEGER DEFAULT 0,
    last_error TEXT,
    
    CONSTRAINT valid_range CHECK (start_block <= end_block)
);

-- Create indexes for gap tracking
CREATE INDEX IF NOT EXISTS idx_sync_gaps_unfilled ON sync_gaps(start_block) WHERE filled_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_sync_gaps_detected ON sync_gaps(detected_at);

-- Create block_validation table for tracking validation status
CREATE TABLE IF NOT EXISTS block_validation (
    block_number BIGINT PRIMARY KEY,
    validated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    parent_hash_valid BOOLEAN NOT NULL,
    receipts_valid BOOLEAN NOT NULL,
    logs_valid BOOLEAN NOT NULL,
    validation_errors TEXT[]
);

-- Create index for validation queries
CREATE INDEX IF NOT EXISTS idx_block_validation_invalid ON block_validation(block_number) 
    WHERE NOT (parent_hash_valid AND receipts_valid AND logs_valid);

-- Add sync statistics table
CREATE TABLE IF NOT EXISTS sync_statistics (
    id SERIAL PRIMARY KEY,
    recorded_at TIMESTAMP NOT NULL DEFAULT NOW(),
    blocks_synced BIGINT NOT NULL,
    transactions_synced BIGINT NOT NULL,
    gaps_detected INTEGER NOT NULL,
    gaps_filled INTEGER NOT NULL,
    sync_rate DECIMAL(10,2), -- blocks per second
    chain_tip BIGINT NOT NULL,
    behind_by BIGINT NOT NULL
);

-- Create index for statistics queries
CREATE INDEX IF NOT EXISTS idx_sync_stats_time ON sync_statistics(recorded_at DESC);

-- Add function to detect gaps
CREATE OR REPLACE FUNCTION detect_block_gaps(start_num BIGINT, end_num BIGINT)
RETURNS TABLE(gap_start BIGINT, gap_end BIGINT) AS $$
DECLARE
    prev_block BIGINT := start_num - 1;
    curr_block BIGINT;
    gap_start_temp BIGINT := NULL;
BEGIN
    FOR curr_block IN
        SELECT number FROM blocks 
        WHERE number BETWEEN start_num AND end_num
        ORDER BY number
    LOOP
        IF curr_block > prev_block + 1 THEN
            -- Gap detected
            gap_start_temp := prev_block + 1;
            gap_start := gap_start_temp;
            gap_end := curr_block - 1;
            RETURN NEXT;
        END IF;
        prev_block := curr_block;
    END LOOP;
    
    -- Check for gap at the end
    IF prev_block < end_num THEN
        gap_start := prev_block + 1;
        gap_end := end_num;
        RETURN NEXT;
    END IF;
END;
$$ LANGUAGE plpgsql;

-- Add comment
COMMENT ON TABLE sync_gaps IS 'Tracks detected gaps in block synchronization';
COMMENT ON TABLE block_validation IS 'Stores validation results for processed blocks';
COMMENT ON TABLE sync_statistics IS 'Historical sync performance metrics';