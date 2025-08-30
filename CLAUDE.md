# Project Standards and Instructions for Zilstream Indexer

## Core Principles

1. **Performance First**: This indexer must handle Zilliqa's 1-second block times
   - Use batch operations for database writes
   - Implement connection pooling for both RPC and database
   - Use concurrent processing where appropriate
   - Profile and optimize bottlenecks

2. **Reliability**: Zero gaps in indexed data
   - Implement comprehensive error handling
   - Add retry logic with exponential backoff
   - Validate block continuity (parent hash)
   - Automatic recovery from failures

3. **Extensibility**: Module system for future protocols
   - Clean module interface
   - Modules should be self-contained
   - Configuration per module
   - Easy to add new DEX protocols

## Code Standards

### Go Conventions
- Use Go 1.21+ features where beneficial
- Follow standard Go naming conventions
- Keep functions small and focused
- Use context for cancellation and timeouts
- Prefer errors over panics

### Error Handling
```go
// Always wrap errors with context
if err != nil {
    return fmt.Errorf("failed to process block %d: %w", blockNumber, err)
}
```

### Database Operations
- Always use prepared statements or parameterized queries
- Use transactions for multi-table updates
- Implement batch inserts (minimum 100 records)
- Use COPY for large data imports
- Add appropriate indexes

### Logging
- Use structured logging with zerolog
- Include relevant context (block number, tx hash, etc.)
- Use appropriate log levels:
  - Debug: Detailed execution flow
  - Info: Important events
  - Warn: Recoverable issues
  - Error: Failures requiring attention

## Project Structure

```
internal/           # Private packages
├── config/        # Configuration management
├── database/      # Database layer
├── rpc/          # Blockchain RPC client
├── processor/    # Core processing logic
├── modules/      # Pluggable modules (Stage 4+)
├── queue/        # Job queue system (Stage 2+)
└── sync/         # Synchronization management (Stage 2+)

cmd/indexer/      # Main application entry point
scripts/          # Utility scripts
migrations/       # Database migrations
```

## Testing Requirements

- Unit tests for all core functions
- Integration tests for database operations
- Use table-driven tests where appropriate
- Mock external dependencies (RPC, database)
- Minimum 70% code coverage for Stage 1

## Performance Targets

### Stage 1 (MVP)
- Process 1 block per second minimum
- Handle 100 transactions per block
- Database batch size: 100 records

### Stage 2+ 
- Process 2+ blocks per second
- Handle 1000+ transactions per second
- Support concurrent processing

## Database Guidelines

1. **Migrations**
   - Number migrations sequentially (001, 002, etc.)
   - Always provide up and down migrations
   - Test rollback capability

2. **Indexes**
   - Add indexes for frequently queried columns
   - Use partial indexes where appropriate
   - Monitor query performance

3. **Data Types**
   - Use NUMERIC(78,0) for amounts (no precision loss)
   - VARCHAR(66) for hashes (with 0x prefix)
   - VARCHAR(42) for addresses (with 0x prefix)
   - BIGINT for block numbers and timestamps

## RPC Client Guidelines

1. **Connection Management**
   - Implement connection pooling (Stage 6)
   - Add retry logic with backoff
   - Handle rate limiting gracefully

2. **Error Handling**
   - Distinguish between temporary and permanent errors
   - Implement circuit breaker pattern (Stage 6)
   - Log all RPC errors with context

## Module System (Stage 4+)

1. **Module Interface**
   - Modules must be stateless
   - Use dependency injection
   - Configuration via JSON
   - Clear initialization/shutdown

2. **Module Types**
   - `token`: ERC-20, ERC-721, etc.
   - `dex`: Uniswap V2, V3, etc.
   - `defi`: Lending, staking, etc.

## Git Commit Standards

- Use conventional commits format
- Prefix with stage: `stage1: Add block processor`
- Keep commits focused and atomic
- Write clear commit messages

## Current Implementation Stage

**Stage 1: MVP (In Progress)**
- [x] Project structure
- [x] Configuration management
- [ ] Database setup and migrations
- [ ] RPC client
- [ ] Block processor
- [ ] Transaction processor
- [ ] Basic sync loop

## Important Notes

1. **Zilliqa Specifics**
   - Chain ID: 32769
   - Block time: ~1 second
   - EVM compatible
   - RPC endpoint: https://api.zilliqa.com

2. **Development Workflow**
   - Complete each stage before moving to next
   - Test thoroughly at each stage
   - Document any deviations from plan
   - Update this file with learnings

3. **Dependencies**
   - Keep dependencies minimal in early stages
   - Add only when necessary
   - Prefer standard library where possible
   - Document why each dependency is needed

## Commands

```bash
# Run indexer
go run cmd/indexer/main.go --config=config.yaml

# Run tests
go test -v ./...

# Run with race detector
go test -race ./...

# Database migrations
migrate -path migrations -database "postgres://..." up

# Build binary
go build -o bin/indexer cmd/indexer/main.go
```

## Environment Variables

- `INDEXER_DATABASE_PASSWORD`: Database password
- `INDEXER_CHAIN_RPC_ENDPOINT`: Override RPC endpoint
- `INDEXER_LOGGING_LEVEL`: Override log level

## Monitoring (Stage 8+)

- Prometheus metrics on :9090/metrics
- Health check on :8080/health
- Key metrics:
  - Blocks processed per second
  - Sync lag (blocks behind)
  - Processing errors
  - Database connection pool stats