# Zilstream Indexer

A high-performance EVM indexer for the Zilliqa blockchain, designed to handle 1-second block times with comprehensive data indexing capabilities.

## 🚀 Features

- **Ultra-Fast Historical Sync**: HyperSync-inspired fast sync achieves 100-1000x faster historical data synchronization
- **Automatic Sync Mode Detection**: Intelligently switches between fast sync and normal sync based on how far behind the indexer is
- **Zilliqa Pre-EVM Transaction Support**: Handles all transaction types including legacy Zilliqa transactions
- **Robust Recovery**: Gap detection, automatic retries, and recovery mechanisms
- **Production Ready**: Health endpoints, metrics, and configurable rate limiting

## Quick Start

### Prerequisites

- Go 1.21+
- PostgreSQL 15+
- Zilliqa RPC endpoint (default: https://api.zilliqa.com)

### Setup

1. **Start PostgreSQL**:
```bash
docker-compose up -d postgres
```

2. **Create database and run migrations**:
```bash
make db-create
make migrate-up
```

3. **Configure the indexer**:
Edit `config.yaml` to set your database credentials and RPC endpoint.

4. **Build and run**:
```bash
make build
./bin/indexer --config=config.yaml
```

Or run directly:
```bash
make run
```

## Configuration

See `config.yaml` for configuration options:

- `chain.rpc_endpoint`: Zilliqa RPC endpoint
- `chain.start_block`: Starting block (0 = latest)
- `database.*`: PostgreSQL connection settings
- `logging.level`: Log level (debug, info, warn, error)

## Development

### Project Structure

```
├── cmd/indexer/          # Main application entry point
├── internal/
│   ├── config/          # Configuration management
│   ├── database/        # Database layer and migrations
│   ├── rpc/            # RPC client for Zilliqa
│   └── processor/      # Block and transaction processing
├── config.yaml         # Configuration file
└── docker-compose.yml  # Local development setup
```

### Available Commands

```bash
make help         # Show all available commands
make build       # Build the binary
make run         # Run the indexer
make test        # Run tests
make db-reset    # Reset database
make docker-run  # Run with Docker Compose
```

## Fast Sync

The indexer automatically uses fast sync when it detects it's more than 10,000 blocks behind. Fast sync features:

- **Batch RPC Requests**: Bundles up to 100 blocks in a single HTTP request
- **PostgreSQL COPY**: Uses bulk inserts achieving 20,000+ blocks/second database write speed
- **Parallel Processing**: Configurable workers for concurrent block fetching
- **Selective Data**: Skips receipts for old blocks to maximize speed

### Manual Fast Sync

You can also run fast sync manually for specific ranges:

```bash
./fastsync -start 0 -end 1000000 -workers 20 -batch 50
```

### Fast Sync Configuration

Configure fast sync behavior in `config.yaml`:

```yaml
processor:
  fast_sync:
    enabled: true            # Enable automatic fast sync
    threshold: 10000         # Use fast sync when behind by this many blocks
    batch_size: 50           # Blocks per batch
    workers: 20              # Parallel workers
    skip_receipts: true      # Skip receipts for old blocks
```

## Current Features

- ✅ **Stage 1**: Basic block and transaction indexing
- ✅ **Stage 2**: Gap detection and recovery mechanisms
- ✅ **Stage 2.5**: Ultra-fast historical sync with batch processing
- ✅ **Stage 3**: Event log processing and indexing
- ✅ Zilliqa pre-EVM transaction support
- ✅ Health and metrics endpoints
- ✅ Configurable rate limiting
- ✅ Automatic sync mode switching

## Upcoming Features
- **Stage 4**: Module system
- **Stage 5**: ERC-20 token indexing
- **Stage 6**: Uniswap V2/V3 support
- **Stage 7**: WebSocket subscriptions

## Database Schema

The indexer uses four main tables:

- `blocks`: Block data including hash, timestamp, gas usage
- `transactions`: Transaction details with from/to addresses, value, gas
- `event_logs`: Smart contract event logs with topics and data
- `indexer_state`: Tracks synchronization progress

## Monitoring

Check indexer status:
```sql
SELECT * FROM indexer_state;
SELECT COUNT(*) FROM blocks;
SELECT COUNT(*) FROM transactions;
```

## License

MIT