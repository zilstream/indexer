-- 011_performance_indexes.sql
-- Critical performance indexes for high-speed sync operations
-- +no-transaction

-- Indexes for blocks table
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocks_number_hash
ON blocks(number, hash);

-- This composite index significantly speeds up block lookups by number and hash verification
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocks_timestamp
ON blocks(timestamp);

-- Index for timestamp-based queries (useful for API)

-- Indexes for transactions table
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_block_tx
ON transactions(block_number, transaction_index);

-- Essential for transaction lookups by block and index ordering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_from_block
ON transactions(from_address, block_number);

-- Speeds up queries for transactions from specific addresses
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_to_block
ON transactions(to_address, block_number)
WHERE to_address IS NOT NULL;

-- Speeds up queries for transactions to specific addresses (partial index for efficiency)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_hash
ON transactions(hash);

-- Primary lookup by hash (if not already primary key)

-- Indexes for event_logs table (most critical for performance)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_logs_block_tx_log
ON event_logs(block_number, transaction_index, log_index);

-- Critical composite index for event log ordering and lookups
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_logs_address_block
ON event_logs(address, block_number DESC);

-- Essential for contract event queries, with DESC for recent events first
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_logs_topics_gin
ON event_logs USING GIN(topics);

-- GIN index for JSONB topic searches - massively speeds up event filtering
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_logs_address_topics
ON event_logs(address) INCLUDE (topics, block_number, transaction_index, log_index);

-- Covering index for contract events with topic data included
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_logs_block_addr
ON event_logs(block_number, address);

-- For block-range queries filtered by address

-- Indexes for module-specific tables (UniswapV2)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_uniswap_v2_pairs_token0_token1
ON uniswap_v2_pairs(token0, token1);

-- For pair lookups by token addresses
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_uniswap_v2_pairs_address
ON uniswap_v2_pairs(address);

-- For pair lookups by pair address
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_uniswap_v2_swaps_pair_block
ON uniswap_v2_swaps(pair, block_number DESC);

-- For recent swaps by pair
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_uniswap_v2_swaps_timestamp
ON uniswap_v2_swaps(timestamp DESC);

-- For recent swaps chronologically

-- Indexes for performance monitoring and gap detection
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_indexer_state_chain_updated
ON indexer_state(chain_id, updated_at);

-- For indexer state monitoring
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_sync_gaps_start_end
ON sync_gaps(start_block, end_block);

-- For gap detection and filling operations

-- Additional composite indexes for API queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_tokens_market_cap_symbol
ON tokens(market_cap_usd DESC NULLS LAST, symbol);

-- For token listings by market cap with symbol

-- Index on created_at for general timestamp-based queries
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_blocks_created_at
ON blocks(created_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transactions_created_at
ON transactions(created_at);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_event_logs_created_at
ON event_logs(created_at);

-- Analyze tables to update statistics after index creation
ANALYZE blocks;
ANALYZE transactions;
ANALYZE event_logs;
ANALYZE tokens;
ANALYZE uniswap_v2_pairs;
ANALYZE uniswap_v2_swaps;
