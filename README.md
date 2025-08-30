# Zilstream Indexer

A high-performance EVM indexer for the Zilliqa blockchain, designed to handle 1-second block times with comprehensive data indexing capabilities.

## Stage 1: MVP Implementation

This is the initial MVP implementation featuring:
- Basic block and transaction indexing
- PostgreSQL storage
- Simple sync loop with error recovery
- Configurable RPC endpoint

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

## Current Features (Stage 1)

- ✅ Connects to Zilliqa RPC endpoint (chainId: 32769)
- ✅ Fetches and stores blocks
- ✅ Processes and stores transactions
- ✅ Maintains sync state across restarts
- ✅ Basic error handling and retry logic
- ✅ Configurable logging

## Upcoming Features

- **Stage 2**: Gap detection and recovery
- **Stage 3**: Event log processing
- **Stage 4**: Module system
- **Stage 5**: ERC-20 token indexing
- **Stage 6**: Performance optimizations
- **Stage 7**: Uniswap V2 support

## Database Schema

The indexer uses three main tables:

- `blocks`: Block data including hash, timestamp, gas usage
- `transactions`: Transaction details with from/to addresses, value, gas
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