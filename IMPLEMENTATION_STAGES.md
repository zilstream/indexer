# Staged Implementation Plan - Zilliqa EVM Indexer

## Overview

This document outlines a staged approach to building the indexer, with each stage producing a working system that can be tested and validated before moving to the next stage.

## Stage 1: Minimal Viable Indexer (MVP)
**Duration: 3-4 days**
**Goal: Basic block and transaction indexing with PostgreSQL storage**

### Deliverables
- Basic project structure
- PostgreSQL connection and simple schema
- Block fetching from RPC
- Transaction processing
- Simple sync loop
- Basic logging

### Implementation Steps

#### 1.1 Project Setup
```
zilstream-indexer/
├── cmd/
│   └── indexer/
│       └── main.go
├── internal/
│   ├── config/
│   │   └── config.go
│   ├── database/
│   │   ├── postgres.go
│   │   └── migrations/
│   │       └── 001_initial.sql
│   ├── rpc/
│   │   └── client.go
│   └── processor/
│       ├── block.go
│       └── transaction.go
├── go.mod
├── go.sum
├── Makefile
└── config.yaml
```

#### 1.2 Core Components

**Dependencies to add:**
```go
// Minimal dependencies for Stage 1
github.com/ethereum/go-ethereum v1.13.5
github.com/jackc/pgx/v5 v5.5.1
github.com/spf13/viper v1.18.2
github.com/rs/zerolog v1.31.0
```

**Database Schema (Simplified):**
```sql
-- Only essential tables
CREATE TABLE blocks (
    number BIGINT PRIMARY KEY,
    hash VARCHAR(66) UNIQUE NOT NULL,
    parent_hash VARCHAR(66) NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE transactions (
    hash VARCHAR(66) PRIMARY KEY,
    block_number BIGINT NOT NULL REFERENCES blocks(number),
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    value NUMERIC(78,0),
    gas_used BIGINT,
    status INT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE indexer_state (
    id SERIAL PRIMARY KEY,
    last_block_number BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);
```

**Basic RPC Client:**
```go
type RPCClient struct {
    client *ethclient.Client
    logger zerolog.Logger
}

func (c *RPCClient) GetBlock(num uint64) (*types.Block, error)
func (c *RPCClient) GetBlockReceipts(blockHash common.Hash) (types.Receipts, error)
```

**Simple Sync Loop:**
```go
func (i *Indexer) Start(ctx context.Context) error {
    lastBlock := i.getLastIndexedBlock()
    
    for {
        select {
        case <-ctx.Done():
            return ctx.Err()
        default:
            currentBlock := lastBlock + 1
            if err := i.processBlock(currentBlock); err != nil {
                i.logger.Error().Err(err).Uint64("block", currentBlock).Msg("Failed to process block")
                time.Sleep(5 * time.Second)
                continue
            }
            lastBlock = currentBlock
        }
    }
}
```

### Testing & Validation
- Unit tests for database operations
- Manual testing with local Zilliqa node
- Verify blocks and transactions are stored correctly
- Check sync recovery after restart

### Success Criteria
✅ Connects to Zilliqa RPC endpoint
✅ Fetches and stores blocks
✅ Processes and stores transactions
✅ Maintains sync state
✅ Handles basic errors and retries

---

## Stage 2: Robust Sync & Recovery
**Duration: 3-4 days**
**Goal: Add gap detection, validation, and recovery mechanisms**

### Deliverables
- Gap detection and tracking
- Block validation (parent hash)
- Batch processing for efficiency
- Retry logic with exponential backoff
- Concurrent block fetching
- Health check endpoint

### Implementation Steps

#### 2.1 Enhanced Database Schema
```sql
-- Add gap tracking
CREATE TABLE sync_gaps (
    id SERIAL PRIMARY KEY,
    from_block BIGINT NOT NULL,
    to_block BIGINT NOT NULL,
    filled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

-- Add indexes for performance
CREATE INDEX idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX idx_tx_block ON transactions(block_number);
CREATE INDEX idx_tx_from ON transactions(from_address);
```

#### 2.2 New Components

**Dependencies to add:**
```go
github.com/cenkalti/backoff/v4 v4.2.1
golang.org/x/sync/errgroup
github.com/gin-gonic/gin v1.9.1  // For health endpoint
```

**Gap Detection:**
```go
type GapDetector struct {
    db     *Database
    logger zerolog.Logger
}

func (g *GapDetector) FindGaps(from, to uint64) ([]Gap, error)
func (g *GapDetector) FillGap(gap Gap) error
```

**Batch Processor:**
```go
type BatchProcessor struct {
    batchSize int
    timeout   time.Duration
    items     []interface{}
}

func (b *BatchProcessor) Add(item interface{})
func (b *BatchProcessor) Flush() error
```

**Health Check:**
```go
GET /health
{
    "status": "healthy",
    "last_block": 1234567,
    "sync_lag": 2,
    "gaps": 0
}
```

### Testing & Validation
- Test gap detection by skipping blocks
- Verify recovery from network failures
- Load test with batch processing
- Validate parent hash chain

### Success Criteria
✅ Detects and fills gaps automatically
✅ Validates block continuity
✅ Batch inserts improve performance
✅ Recovers from RPC failures
✅ Health endpoint provides status

---

## Stage 3: Event Log Processing
**Duration: 2-3 days**
**Goal: Process and store event logs for later module use**

### Deliverables
- Event log storage
- Log decoding framework
- Topic indexing
- Event filtering system

### Implementation Steps

#### 3.1 Database Schema
```sql
CREATE TABLE logs (
    id BIGSERIAL PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL,
    block_number BIGINT NOT NULL,
    log_index INT NOT NULL,
    address VARCHAR(42) NOT NULL,
    topics TEXT[],
    data TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_logs_address ON logs(address);
CREATE INDEX idx_logs_topic0 ON logs((topics[1]));
```

#### 3.2 Event Processing
```go
type LogProcessor struct {
    db *Database
}

func (l *LogProcessor) ProcessLogs(receipt *types.Receipt) error
func (l *LogProcessor) GetLogsByFilter(filter ethereum.FilterQuery) ([]types.Log, error)
```

### Testing & Validation
- Verify logs are stored correctly
- Test topic filtering
- Validate log ordering

### Success Criteria
✅ Stores all event logs
✅ Can filter logs by address/topics
✅ Maintains log index order

---

## Stage 4: Module System Foundation
**Duration: 3-4 days**
**Goal: Implement pluggable module architecture**

### Deliverables
- Module interface definition
- Module registry
- Module lifecycle management
- Configuration per module
- Module processing pipeline

### Implementation Steps

#### 4.1 Module Interface
```go
type Module interface {
    Name() string
    Initialize(config json.RawMessage) error
    Process(ctx context.Context, block *Block, logs []Log) error
    GetFilters() []EventFilter
}
```

#### 4.2 Module Registry
```go
type ModuleRegistry struct {
    modules map[string]Module
}

func (r *ModuleRegistry) Register(module Module)
func (r *ModuleRegistry) Process(block *Block, logs []Log)
```

#### 4.3 Database Schema
```sql
CREATE TABLE indexer_modules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    enabled BOOLEAN DEFAULT true,
    last_processed_block BIGINT DEFAULT 0,
    config JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);
```

### Testing & Validation
- Create dummy module for testing
- Verify module initialization
- Test module enable/disable
- Validate module processing order

### Success Criteria
✅ Modules can be registered dynamically
✅ Module configuration works
✅ Modules process blocks/logs correctly
✅ Module state is persisted

---

## Stage 5: ERC-20 Token Module
**Duration: 4-5 days**
**Goal: First real module - index ERC-20 tokens**

### Deliverables
- ERC-20 event detection
- Token discovery
- Transfer tracking
- Balance calculation
- Token metadata fetching

### Implementation Steps

#### 5.1 Database Schema
```sql
CREATE TABLE tokens (
    address VARCHAR(42) PRIMARY KEY,
    name VARCHAR(255),
    symbol VARCHAR(50),
    decimals INT,
    total_supply NUMERIC(78,0),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE token_transfers (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(42) NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    value NUMERIC(78,0) NOT NULL,
    block_number BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE token_balances (
    token_address VARCHAR(42) NOT NULL,
    holder_address VARCHAR(42) NOT NULL,
    balance NUMERIC(78,0) NOT NULL DEFAULT 0,
    PRIMARY KEY (token_address, holder_address)
);
```

#### 5.2 ERC-20 Module
```go
type ERC20Module struct {
    db       *Database
    rpc      *RPCClient
    abi      abi.ABI
    cache    map[string]*Token
}

func (m *ERC20Module) detectToken(address common.Address) (*Token, error)
func (m *ERC20Module) processTransfer(log types.Log) error
func (m *ERC20Module) updateBalance(token, holder string, amount *big.Int) error
```

### Testing & Validation
- Deploy test ERC-20 token
- Verify transfer events are captured
- Validate balance calculations
- Test token metadata fetching

### Success Criteria
✅ Discovers new tokens automatically
✅ Tracks all transfers accurately
✅ Maintains correct balances
✅ Fetches token metadata

---

## Stage 6: Performance Optimization
**Duration: 3-4 days**
**Goal: Optimize for production workloads**

### Deliverables
- Worker pool implementation
- Connection pooling
- Caching layer
- Batch processing optimization
- Concurrent module processing

### Implementation Steps

#### 6.1 Dependencies to add
```go
github.com/redis/go-redis/v9 v9.4.0
github.com/dgraph-io/ristretto v0.1.1
```

#### 6.2 Worker Pool
```go
type WorkerPool struct {
    workers  int
    jobQueue chan Job
}

func (w *WorkerPool) Start()
func (w *WorkerPool) Submit(job Job)
```

#### 6.3 Caching
```go
type Cache struct {
    redis     *redis.Client
    memory    *ristretto.Cache
}

func (c *Cache) GetBlock(number uint64) (*Block, error)
func (c *Cache) SetBlock(block *Block) error
```

### Testing & Validation
- Benchmark before/after optimization
- Load test with high block volume
- Memory profiling
- Connection pool monitoring

### Success Criteria
✅ 50% performance improvement
✅ Handles 1000+ TPS
✅ Memory usage stable
✅ Connection pool efficient

---

## Stage 7: Uniswap V2 Module
**Duration: 4-5 days**
**Goal: Index Uniswap V2 DEX activity**

### Deliverables
- Factory monitoring
- Pair discovery
- Swap event processing
- Liquidity tracking
- Price calculation

### Implementation Steps

#### 7.1 Database Schema
```sql
CREATE TABLE dex_pairs (
    address VARCHAR(42) PRIMARY KEY,
    dex_type VARCHAR(20) NOT NULL,
    token0_address VARCHAR(42) NOT NULL,
    token1_address VARCHAR(42) NOT NULL,
    reserve0 NUMERIC(78,0),
    reserve1 NUMERIC(78,0),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE dex_swaps (
    id BIGSERIAL PRIMARY KEY,
    pair_address VARCHAR(42) NOT NULL,
    transaction_hash VARCHAR(66) NOT NULL,
    amount0_in NUMERIC(78,0),
    amount1_in NUMERIC(78,0),
    amount0_out NUMERIC(78,0),
    amount1_out NUMERIC(78,0),
    block_number BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
```

#### 7.2 Uniswap V2 Module
```go
type UniswapV2Module struct {
    factoryAddress common.Address
    pairABI        abi.ABI
    factoryABI     abi.ABI
}

func (m *UniswapV2Module) processPairCreated(log types.Log) error
func (m *UniswapV2Module) processSwap(log types.Log) error
func (m *UniswapV2Module) processSync(log types.Log) error
```

### Testing & Validation
- Test with Uniswap V2 fork
- Verify pair discovery
- Validate swap processing
- Check reserve updates

### Success Criteria
✅ Discovers all pairs
✅ Processes swaps correctly
✅ Updates reserves accurately
✅ Calculates prices

---

## Stage 8: Monitoring & Operations
**Duration: 3-4 days**
**Goal: Production-ready monitoring and operations**

### Deliverables
- Prometheus metrics
- Grafana dashboards
- Alerting rules
- Docker deployment
- Kubernetes manifests
- Operational runbooks

### Implementation Steps

#### 8.1 Dependencies to add
```go
github.com/prometheus/client_golang v1.18.0
```

#### 8.2 Metrics
```go
var (
    blocksProcessed = prometheus.NewCounterVec(...)
    processingDuration = prometheus.NewHistogramVec(...)
    syncLag = prometheus.NewGauge(...)
)
```

#### 8.3 Docker Setup
```dockerfile
FROM golang:1.21-alpine AS builder
# Build steps

FROM alpine:3.19
# Runtime setup
```

#### 8.4 Kubernetes
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zilstream-indexer
spec:
  replicas: 1
  # Deployment spec
```

### Testing & Validation
- Test Docker build
- Deploy to test Kubernetes
- Verify metrics collection
- Test alerting

### Success Criteria
✅ Metrics exposed correctly
✅ Dashboards show key metrics
✅ Alerts fire appropriately
✅ Deploys to Kubernetes

---

## Stage 9: Advanced Features
**Duration: 4-5 days**
**Goal: Add advanced capabilities**

### Deliverables
- Uniswap V3 module
- WebSocket support for real-time updates
- GraphQL API
- Historical data reprocessing
- Data export functionality

### Implementation Steps

#### 9.1 Uniswap V3 Module
- Pool discovery
- Position tracking
- Tick processing
- Fee tier handling

#### 9.2 WebSocket Support
```go
type WebSocketServer struct {
    upgrader websocket.Upgrader
    clients  map[*Client]bool
}

func (w *WebSocketServer) HandleConnection(conn *websocket.Conn)
func (w *WebSocketServer) Broadcast(event Event)
```

#### 9.3 GraphQL API
```go
// Using gqlgen or similar
type Query {
    block(number: Int!): Block
    transaction(hash: String!): Transaction
    token(address: String!): Token
}
```

### Testing & Validation
- Test V3 specific features
- WebSocket connection tests
- GraphQL query tests
- Reprocessing validation

### Success Criteria
✅ V3 pools indexed correctly
✅ WebSocket delivers updates
✅ GraphQL API functional
✅ Can reprocess historical data

---

## Stage 10: Production Hardening
**Duration: 3-4 days**
**Goal: Final polish and production readiness**

### Deliverables
- Comprehensive testing
- Performance tuning
- Security audit
- Documentation
- CI/CD pipeline
- Disaster recovery plan

### Implementation Steps

#### 10.1 Testing
- Unit test coverage > 80%
- Integration test suite
- Load testing
- Chaos testing

#### 10.2 Performance
- Query optimization
- Index tuning
- Connection pool tuning
- Memory optimization

#### 10.3 Security
- Input validation
- Rate limiting
- Authentication for admin APIs
- Secrets management

#### 10.4 Documentation
- API documentation
- Deployment guide
- Operations manual
- Troubleshooting guide

### Testing & Validation
- Full regression testing
- Security scanning
- Performance benchmarks
- Disaster recovery drill

### Success Criteria
✅ All tests passing
✅ Performance targets met
✅ Security scan clean
✅ Documentation complete

---

## Implementation Timeline

```
Week 1:  Stage 1 (MVP) + Stage 2 (Robust Sync)
Week 2:  Stage 3 (Events) + Stage 4 (Module System)
Week 3:  Stage 5 (ERC-20)
Week 4:  Stage 6 (Performance) + Stage 7 (Uniswap V2)
Week 5:  Stage 8 (Monitoring) + Stage 9 (Advanced)
Week 6:  Stage 10 (Production Hardening)
```

## Risk Mitigation by Stage

### Stage 1-2 Risks
- **RPC instability**: Start with single endpoint, add pooling in Stage 6
- **Database performance**: Simple schema first, optimize in Stage 6

### Stage 3-5 Risks
- **Module complexity**: Start with simple interface, extend as needed
- **Token variety**: Focus on standard ERC-20, add edge cases later

### Stage 6-7 Risks
- **Performance bottlenecks**: Profile extensively, optimize iteratively
- **DEX complexity**: Start with V2, simpler than V3

### Stage 8-10 Risks
- **Operational complexity**: Build monitoring early
- **Production issues**: Extensive testing in Stage 10

## Testing Strategy Per Stage

### Unit Testing
- Stage 1: Basic happy path tests
- Stage 2-3: Add error cases
- Stage 4-5: Module-specific tests
- Stage 6+: Performance tests

### Integration Testing
- Stage 2: Database integration
- Stage 3-4: End-to-end flow
- Stage 5-7: Module integration
- Stage 8+: Full system tests

### Load Testing
- Stage 6: Baseline performance
- Stage 7-8: Production loads
- Stage 9-10: Stress testing

## Go/No-Go Criteria

Each stage must meet these criteria before proceeding:

1. **Functionality**: All features working as specified
2. **Testing**: Tests written and passing
3. **Performance**: Meets stage-specific targets
4. **Stability**: Runs for 24 hours without crashes
5. **Documentation**: Code and usage documented

## Rollback Plan

Each stage maintains backward compatibility:

1. **Database migrations**: All reversible
2. **Code changes**: Feature flags where needed
3. **Module system**: Can disable modules
4. **Performance**: Can revert optimizations

## Success Metrics

### Stage 1-3: Foundation
- Sync maintains < 10 blocks behind
- Zero gaps in indexed data
- 99% uptime in testing

### Stage 4-6: Features
- All ERC-20 tokens discovered
- Transfer accuracy 100%
- Performance > 100 blocks/second

### Stage 7-9: Advanced
- DEX data accurate
- Real-time updates < 1 second
- API response < 100ms

### Stage 10: Production
- 99.9% uptime
- Zero data loss
- Full disaster recovery

## Conclusion

This staged approach allows us to:

1. **Build incrementally**: Each stage adds value
2. **Test thoroughly**: Validate before moving forward
3. **Minimize risk**: Problems caught early
4. **Maintain momentum**: Regular deliverables
5. **Learn and adapt**: Adjust based on findings

The plan prioritizes:
- **Early value delivery** (basic indexing in Stage 1)
- **Robust foundation** (sync and recovery in Stage 2)
- **Extensibility** (module system in Stage 4)
- **Performance** (optimization in Stage 6)
- **Production readiness** (monitoring in Stage 8)

Each stage builds on the previous, ensuring a solid, well-tested system that can handle Zilliqa's requirements while remaining maintainable and extensible.