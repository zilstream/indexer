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
   - Batch client for fast sync operations
   - Raw RPC handling for Zilliqa-specific transactions

2. **Database Layer** (`internal/database/`): PostgreSQL storage
   - Connection pooling with pgx/v5
   - Bulk writer using PostgreSQL COPY for fast inserts
   - Migration system for schema management

3. **Processors** (`internal/processor/`): Business logic
   - Block processor
   - Transaction processor  
   - Main indexer coordinator

4. **Sync System** (`internal/sync/`): Synchronization management
   - Normal sync manager with gap detection
   - Fast sync for historical data (100-1000x faster)
   - Automatic mode switching based on sync gap

5. **API/Health** (`internal/api/`): Monitoring endpoints
   - Health checks
   - Metrics
   - Sync status

## Fast Sync System

The indexer includes a HyperSync-inspired fast sync system that dramatically improves historical data synchronization:

### Key Optimizations:
- **Batch RPC**: Bundle up to 100 block requests in single HTTP call
- **PostgreSQL COPY**: Bulk inserts achieving 20,000+ blocks/second
- **Pipeline Architecture**: Parallel fetch → process → store stages
- **Selective Data**: Skip receipts for old blocks to save time

### Automatic Detection:
- Fast sync triggers when >10,000 blocks behind
- Leaves last 1,000 blocks for normal sync (full processing)
- Configurable thresholds and parameters

## Current Implementation Status

### Completed:
- ✅ Stage 1: Basic indexing (blocks, transactions)
- ✅ Stage 2: Robust sync (gap detection, recovery)
- ✅ Stage 2.5: Fast sync system
- ✅ Zilliqa pre-EVM transaction support
- ✅ Automatic sync mode detection

### Pending:
- Stage 3: Event log processing
- Stage 4: Module system foundation
- Stage 5: ERC-20 token module
- Stage 6: Uniswap V2/V3 support

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

### Batch Processing Pattern
```go
// Fetch blocks in parallel
workers -> blockChan -> processor -> bulkWriter -> database
```

## Configuration

The system is configured via `config.yaml` with environment variable overrides:
- `INDEXER_` prefix for env vars
- Viper for configuration management
- Sensible defaults for all settings

## Testing

When testing fast sync performance:
```bash
# Test small range
./fastsync -start 8470000 -end 8470100 -workers 5 -batch 20

# Test large historical sync
./fastsync -start 0 -end 1000000 -workers 50 -skip-receipts
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

## Next Steps

For Stage 3 (Event Log Processing):
1. Implement log decoder for standard events
2. Add event storage schema
3. Create event processor pipeline
4. Support indexed event parameters
5. Consider using eth_getLogs with block ranges