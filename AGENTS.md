# Zilstream Indexer - Project Context for Claude

## Project Overview

This is a high-performance blockchain indexer for the Zilliqa EVM network. The indexer is designed to handle Zilliqa's unique characteristics including 1-second block times and pre-EVM transaction types.

## Key Technical Details

### Zilliqa Specifics

- **RPC Endpoint**: https://api.zilliqa.com
- **Chain ID**: 32769
- **Block Time**: 1 second
- **Special Transactions**: Zilliqa has pre-EVM transaction types (e.g., type 0xdd870) that must be handled specially

### Architecture

The indexer uses a modular architecture with the following components:

1. **RPC Client** (`internal/rpc/`): Handles communication with Zilliqa nodes

   - Standard client for normal operations
   - Batch client for fast sync operations (supports up to 1000+ block requests)
   - Raw RPC handling for Zilliqa-specific transactions

2. **Database Layer** (`internal/database/`): PostgreSQL storage

   - Connection pooling with pgx/v5
   - BulkWriter using PostgreSQL COPY for fast inserts (20,000+ blocks/second)
   - AtomicBlockWriter for ACID guarantees near head
   - Migration system with 6 migration files

3. **Processors** (`internal/processor/`): Business logic

   - Block processor with Zilliqa-specific transaction handling
   - EventProcessor for log extraction and module routing
   - Transaction processor with original type preservation
   - Main indexer coordinator

4. **Sync System** (`internal/sync/`): Unified synchronization

   - UnifiedSync handles all sync scenarios adaptively
   - Automatic batch sizing (1-200 blocks based on gap)
   - Intelligent writer selection (bulk vs atomic)
   - Receipt batching with parallel processing

5. **Module System** (`internal/modules/`): Pluggable event processing

   - Core module interface inspired by The Graph Protocol
   - Module registry for event routing
   - UniswapV2 module with comprehensive event handling
   - Manifest-driven configuration system

6. **API/Health** (`internal/api/`): Monitoring endpoints
   - Health checks
   - Metrics
   - Sync status

## UnifiedSync System

The indexer uses a unified synchronization system that adaptively handles all sync scenarios:

### Key Features:

- **Adaptive Batching**: Intelligent batch sizing (1-200 blocks) based on sync gap
- **Dual Writer Strategy**: BulkWriter for large batches (50+ blocks), AtomicBlockWriter for small batches
- **Parallel Receipt Processing**: Fetches receipts in chunks of 1000-2000 for performance
- **Event Processing**: Integrated with module system for real-time event handling
- **PostgreSQL COPY**: Bulk inserts achieving 20,000+ blocks/second

### Performance Optimizations:

- Raw block fetching preserves Zilliqa transaction metadata
- Receipt batching with configurable chunk sizes
- Temporary tables for upsert operations
- Exponential backoff retry logic

## Current Implementation Status

### Completed:

- ✅ Stage 1: Basic indexing (blocks, transactions)
- ✅ Stage 2: Robust sync (gap detection, recovery)
- ✅ Stage 2.5: Fast sync system with UnifiedSync
- ✅ Stage 3: Event log processing and storage
- ✅ Stage 4: Module system foundation
- ✅ Zilliqa pre-EVM transaction support
- ✅ Automatic sync mode detection
- ✅ Bulk writing with PostgreSQL COPY
- ✅ AtomicBlockWriter for ACID guarantees

### In Progress:

- Stage 5: UniswapV2 module (handler structure complete)
- Uniswap V3 support planning

### Pending:

- ERC-20 token module
- Additional DeFi protocol modules

## Important Patterns

### Error Handling

- All RPC calls have retry logic with exponential backoff
- 429 rate limit errors are handled with adaptive delays
- Failed blocks are added to gap list for later recovery

### Transaction Type Classification

```go
// Standard EVM types: 0-3
// Zilliqa-specific: 1000 + original_type
if txType > 3 {
    dbTx.TransactionType = 1000 + int(txType)
    dbTx.OriginalTypeHex = hex_value
}
```

### Modern Sync Pattern

```go
// UnifiedSync with adaptive processing
UnifiedSync.SyncRange() -> calculateBatchSize() -> processBatch() ->
  fetchBlocksWithReceipts() -> convertRawBlocks() ->
  (BulkWriter|AtomicWriter) + ModuleRegistry.ProcessEvent()
```

### Module System Pattern

```go
// Event processing through modules
EventProcessor -> ModuleRegistry.ProcessEvent() ->
  Module.HandleEvent() -> EventHandler -> Database
```

## Configuration

The system is configured via `config.yaml` with environment variable overrides:

- `INDEXER_` prefix for env vars
- Viper for configuration management
- Sensible defaults for all settings

## Running the Indexer

### Main Indexer

```bash
# Standard operation
./indexer -config config.yaml

# The indexer automatically detects sync gaps and uses optimal strategy
```

### Backfill Tool

```bash
# Backfill specific range
./backfill -start 8470000 -end 8470100

# Large historical sync (uses BulkWriter automatically)
./backfill -start 0 -end 1000000
```

## Known Issues & Solutions

1. **"transaction type not supported" error**:

   - Caused by Zilliqa pre-EVM transactions
   - Solution: Use raw RPC parsing with custom types

2. **Rate limiting (429 errors)**:

   - Solution: Adaptive rate limiting, configurable delays
   - Default: 50 requests/second for fast sync

3. **Memory usage during fast sync**:
   - Solution: Streaming processing, bounded channels
   - Buffer sizes are configurable

## Development Guidelines

1. **Always handle Zilliqa-specific cases**: Check for transaction types > 3
2. **Use batch operations where possible**: Batch RPC, bulk inserts
3. **Consider 1-second block times**: Optimize for high throughput
4. **Test with real Zilliqa data**: Some blocks have special transactions

## Database Schema

### Core Tables

- `blocks`: Block data with EIP-1559 support
- `transactions`: Transaction data with Zilliqa type handling (1000+ for pre-EVM)
- `event_logs`: Event logs with JSONB topics for efficient filtering
- `indexer_state`: Gap tracking and last processed block
- `module_state`: Per-module sync state tracking

### Module-Specific Tables

- `uniswap_v2_factory`: Factory contracts and pair counts
- `uniswap_v2_pairs`: Trading pair information
- Additional tables created by modules as needed

## Module Development

### Creating a New Module

1. Implement the `core.Module` interface
2. Define event filters and handlers
3. Create manifest YAML with data sources
4. Add database schema migrations
5. Register with module registry

### UniswapV2 Module Example

The UniswapV2 module demonstrates the complete pattern:

- Event filters for Factory and Pair events
- Handlers for PairCreated, Swap, Mint, Burn, Sync
- Token metadata fetching
- Backfill support from event_logs table

## Performance Considerations

### Sync Performance

- UnifiedSync achieves 1000+ blocks/second for historical data
- Adaptive batching prevents RPC overload
- PostgreSQL COPY provides 20,000+ blocks/second write performance
- Intelligent receipt batching (1000-2000 per chunk)

### Memory Management

- Bounded channels prevent memory leaks
- Streaming processing for large ranges
- Temporary tables for bulk operations
- Connection pooling with pgx/v5

## Development Workflow

### Key Files to Know

- `internal/sync/unified_sync.go`: Main sync logic
- `internal/database/bulk_writer.go`: High-performance writes
- `internal/processor/event_processor.go`: Event extraction and routing
- `internal/modules/core/module.go`: Module interface definition
- Migration files in `internal/database/migrations/`

### Testing Strategy

- Use real Zilliqa data for testing Zilliqa-specific transactions
- Test module backfill functionality with historical events
- Verify bulk writer performance with large datasets
- Monitor memory usage during long-running syncs
