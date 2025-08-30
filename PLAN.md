# Zilliqa EVM Indexer - Development Plan

## Project Overview

A high-performance, extensible EVM indexer for the Zilliqa blockchain designed to handle ~1 second block times with comprehensive data indexing capabilities.

## Core Requirements

### Functional Requirements
- Index blocks and transactions from Zilliqa EVM
- Track ERC-20 tokens and their transfers
- Index Uniswap V2/V3 DEX activity
- Support multiple DEX protocols simultaneously
- Extensible module system for future protocols
- Resilient syncing with automatic recovery
- Zero-gap validation ensuring no missed blocks

### Non-Functional Requirements
- Handle 1-second block times efficiently
- PostgreSQL as primary datastore
- Concurrent processing with job queues
- Automatic reconnection and recovery
- Comprehensive monitoring and metrics
- Horizontal scalability support

## Architecture Design

### System Components

```
┌─────────────────────────────────────────────────────────┐
│                    RPC Client Pool                      │
│              (Multiple Zilliqa Nodes)                   │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                   Block Fetcher                         │
│           (Continuous polling/websocket)                │
└────────────┬──────────────────┬─────────────────────────┘
             │                  │
   ┌─────────▼──────┐  ┌───────▼────────┐
   │ Priority Queue │  │ Validation     │
   │   (Blocks)     │  │   Service      │
   └─────────┬──────┘  └────────────────┘
             │
┌────────────▼────────────────────────────────────────────┐
│              Processing Pipeline                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────────────┐    │
│  │  Block   │  │   TX     │  │  Module Registry │    │
│  │ Processor│→ │ Processor│→ │  (ERC20, DEX...)│    │
│  └──────────┘  └──────────┘  └──────────────────┘    │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│                  PostgreSQL                             │
│   Core Tables | Token Tables | DEX Tables | Meta       │
└──────────────────────────────────────────────────────────┘
```

### Project Structure

```
zilstream-indexer/
├── cmd/
│   └── indexer/
│       └── main.go           # Application entry point
├── internal/
│   ├── blockchain/           # Blockchain interaction layer
│   │   ├── client.go         # RPC client with connection pooling
│   │   ├── types.go          # Block, Transaction, Log types
│   │   ├── decoder.go        # ABI decoding utilities
│   │   └── client_test.go
│   ├── processor/            # Core processing logic
│   │   ├── pipeline.go       # Main processing pipeline
│   │   ├── block.go          # Block processor
│   │   ├── transaction.go    # Transaction processor
│   │   ├── worker.go         # Worker pool implementation
│   │   └── processor_test.go
│   ├── storage/              # Database layer
│   │   ├── postgres.go       # PostgreSQL connection management
│   │   ├── models.go         # Database models
│   │   ├── migrations/       # SQL migration files
│   │   │   ├── 001_initial.up.sql
│   │   │   ├── 001_initial.down.sql
│   │   │   ├── 002_tokens.up.sql
│   │   │   └── 002_tokens.down.sql
│   │   └── repository/       # Repository pattern
│   │       ├── blocks.go
│   │       ├── transactions.go
│   │       └── tokens.go
│   ├── modules/              # Pluggable indexer modules
│   │   ├── registry.go       # Module registration system
│   │   ├── interface.go      # Module interface definition
│   │   ├── erc20/            # ERC20 token indexer
│   │   │   ├── indexer.go
│   │   │   ├── abi.go
│   │   │   └── events.go
│   │   ├── uniswapv2/        # Uniswap V2 indexer
│   │   │   ├── indexer.go
│   │   │   ├── pair.go
│   │   │   ├── factory.go
│   │   │   └── events.go
│   │   └── uniswapv3/        # Uniswap V3 indexer
│   │       ├── indexer.go
│   │       ├── pool.go
│   │       ├── factory.go
│   │       └── events.go
│   ├── queue/                # Queue/job system
│   │   ├── priority.go       # Priority queue implementation
│   │   ├── worker.go         # Async worker management
│   │   └── queue_test.go
│   └── sync/                 # Synchronization management
│       ├── manager.go        # Sync state management
│       ├── validator.go      # Block continuity validation
│       ├── recovery.go       # Recovery mechanisms
│       └── sync_test.go
├── pkg/
│   ├── config/               # Configuration management
│   │   └── config.go
│   ├── logger/               # Structured logging
│   │   └── logger.go
│   └── metrics/              # Prometheus metrics
│       └── metrics.go
├── deployments/
│   ├── docker/
│   │   ├── Dockerfile
│   │   └── docker-compose.yml
│   └── kubernetes/
│       ├── deployment.yaml
│       └── service.yaml
├── scripts/
│   ├── setup.sh              # Development setup script
│   └── migrate.sh            # Database migration runner
├── go.mod
├── go.sum
├── Makefile
├── README.md
├── .env.example
└── config.yaml
```

## Database Schema

### Core Tables

```sql
-- Blocks table
CREATE TABLE blocks (
    number BIGINT PRIMARY KEY,
    hash VARCHAR(66) UNIQUE NOT NULL,
    parent_hash VARCHAR(66) NOT NULL,
    timestamp BIGINT NOT NULL,
    gas_limit BIGINT,
    gas_used BIGINT,
    base_fee_per_gas NUMERIC(78,0),
    transaction_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_blocks_timestamp ON blocks(timestamp);
CREATE INDEX idx_blocks_parent ON blocks(parent_hash);
CREATE INDEX idx_blocks_created ON blocks(created_at);

-- Transactions table
CREATE TABLE transactions (
    hash VARCHAR(66) PRIMARY KEY,
    block_number BIGINT NOT NULL REFERENCES blocks(number) ON DELETE CASCADE,
    transaction_index INT NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42),
    value NUMERIC(78,0) DEFAULT 0,
    gas_price NUMERIC(78,0),
    gas_limit BIGINT,
    gas_used BIGINT,
    nonce BIGINT,
    input TEXT,
    status INT,
    contract_address VARCHAR(42),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_tx_block ON transactions(block_number);
CREATE INDEX idx_tx_from ON transactions(from_address);
CREATE INDEX idx_tx_to ON transactions(to_address);
CREATE INDEX idx_tx_contract ON transactions(contract_address) WHERE contract_address IS NOT NULL;
CREATE UNIQUE INDEX idx_tx_block_index ON transactions(block_number, transaction_index);

-- Event logs table
CREATE TABLE logs (
    id BIGSERIAL PRIMARY KEY,
    transaction_hash VARCHAR(66) NOT NULL REFERENCES transactions(hash) ON DELETE CASCADE,
    block_number BIGINT NOT NULL,
    log_index INT NOT NULL,
    address VARCHAR(42) NOT NULL,
    topics TEXT[],
    data TEXT,
    removed BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_logs_tx ON logs(transaction_hash);
CREATE INDEX idx_logs_block ON logs(block_number);
CREATE INDEX idx_logs_address ON logs(address);
CREATE INDEX idx_logs_topic0 ON logs((topics[1])) WHERE array_length(topics, 1) >= 1;
CREATE UNIQUE INDEX idx_logs_unique ON logs(transaction_hash, log_index);
```

### Module Management Tables

```sql
-- Module registry
CREATE TABLE indexer_modules (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    type VARCHAR(50) NOT NULL,
    version VARCHAR(20),
    config JSONB,
    enabled BOOLEAN DEFAULT true,
    last_processed_block BIGINT DEFAULT 0,
    last_error TEXT,
    error_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_modules_enabled ON indexer_modules(enabled);
CREATE INDEX idx_modules_type ON indexer_modules(type);

-- Module processing history
CREATE TABLE module_processing_history (
    id BIGSERIAL PRIMARY KEY,
    module_id INT NOT NULL REFERENCES indexer_modules(id),
    block_number BIGINT NOT NULL,
    items_processed INT DEFAULT 0,
    processing_time_ms INT,
    status VARCHAR(20) NOT NULL, -- 'success', 'failed', 'skipped'
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_module_history_module ON module_processing_history(module_id);
CREATE INDEX idx_module_history_block ON module_processing_history(block_number);
CREATE INDEX idx_module_history_status ON module_processing_history(status);
```

### Token Tables

```sql
-- ERC20 Tokens
CREATE TABLE tokens (
    address VARCHAR(42) PRIMARY KEY,
    name VARCHAR(255),
    symbol VARCHAR(50),
    decimals INT,
    total_supply NUMERIC(78,0),
    holder_count INT DEFAULT 0,
    transfer_count BIGINT DEFAULT 0,
    created_at_block BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_tokens_symbol ON tokens(symbol);
CREATE INDEX idx_tokens_name ON tokens(name);

-- Token transfers
CREATE TABLE token_transfers (
    id BIGSERIAL PRIMARY KEY,
    token_address VARCHAR(42) NOT NULL REFERENCES tokens(address),
    transaction_hash VARCHAR(66) NOT NULL REFERENCES transactions(hash),
    log_index INT NOT NULL,
    from_address VARCHAR(42) NOT NULL,
    to_address VARCHAR(42) NOT NULL,
    value NUMERIC(78,0) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_transfers_token ON token_transfers(token_address);
CREATE INDEX idx_transfers_from ON token_transfers(from_address);
CREATE INDEX idx_transfers_to ON token_transfers(to_address);
CREATE INDEX idx_transfers_block ON token_transfers(block_number);
CREATE UNIQUE INDEX idx_transfers_unique ON token_transfers(transaction_hash, log_index);

-- Token balances (materialized view or table)
CREATE TABLE token_balances (
    token_address VARCHAR(42) NOT NULL REFERENCES tokens(address),
    holder_address VARCHAR(42) NOT NULL,
    balance NUMERIC(78,0) NOT NULL DEFAULT 0,
    last_updated_block BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (token_address, holder_address)
);

CREATE INDEX idx_balances_holder ON token_balances(holder_address);
CREATE INDEX idx_balances_token ON token_balances(token_address);
CREATE INDEX idx_balances_value ON token_balances(balance) WHERE balance > 0;
```

### DEX Tables

```sql
-- DEX registry
CREATE TABLE dex_protocols (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) NOT NULL,
    type VARCHAR(20) NOT NULL, -- 'uniswap-v2', 'uniswap-v3', etc
    factory_address VARCHAR(42) UNIQUE NOT NULL,
    router_address VARCHAR(42),
    created_at TIMESTAMP DEFAULT NOW()
);

-- DEX pairs/pools
CREATE TABLE dex_pairs (
    address VARCHAR(42) PRIMARY KEY,
    protocol_id INT NOT NULL REFERENCES dex_protocols(id),
    token0_address VARCHAR(42) NOT NULL,
    token1_address VARCHAR(42) NOT NULL,
    fee_tier INT, -- For V3 pools (500, 3000, 10000)
    tick_spacing INT, -- For V3 pools
    reserve0 NUMERIC(78,0),
    reserve1 NUMERIC(78,0),
    total_supply NUMERIC(78,0),
    swap_count BIGINT DEFAULT 0,
    created_at_block BIGINT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_pairs_protocol ON dex_pairs(protocol_id);
CREATE INDEX idx_pairs_token0 ON dex_pairs(token0_address);
CREATE INDEX idx_pairs_token1 ON dex_pairs(token1_address);
CREATE INDEX idx_pairs_tokens ON dex_pairs(token0_address, token1_address);

-- Swap events
CREATE TABLE dex_swaps (
    id BIGSERIAL PRIMARY KEY,
    pair_address VARCHAR(42) NOT NULL REFERENCES dex_pairs(address),
    transaction_hash VARCHAR(66) NOT NULL REFERENCES transactions(hash),
    log_index INT NOT NULL,
    sender VARCHAR(42) NOT NULL,
    recipient VARCHAR(42) NOT NULL,
    amount0_in NUMERIC(78,0),
    amount1_in NUMERIC(78,0),
    amount0_out NUMERIC(78,0),
    amount1_out NUMERIC(78,0),
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_swaps_pair ON dex_swaps(pair_address);
CREATE INDEX idx_swaps_sender ON dex_swaps(sender);
CREATE INDEX idx_swaps_recipient ON dex_swaps(recipient);
CREATE INDEX idx_swaps_block ON dex_swaps(block_number);
CREATE INDEX idx_swaps_timestamp ON dex_swaps(timestamp);
CREATE UNIQUE INDEX idx_swaps_unique ON dex_swaps(transaction_hash, log_index);

-- Liquidity events
CREATE TABLE dex_liquidity_events (
    id BIGSERIAL PRIMARY KEY,
    pair_address VARCHAR(42) NOT NULL REFERENCES dex_pairs(address),
    transaction_hash VARCHAR(66) NOT NULL REFERENCES transactions(hash),
    log_index INT NOT NULL,
    type VARCHAR(10) NOT NULL, -- 'add' or 'remove'
    provider VARCHAR(42) NOT NULL,
    amount0 NUMERIC(78,0),
    amount1 NUMERIC(78,0),
    liquidity NUMERIC(78,0),
    block_number BIGINT NOT NULL,
    timestamp BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_liquidity_pair ON dex_liquidity_events(pair_address);
CREATE INDEX idx_liquidity_provider ON dex_liquidity_events(provider);
CREATE INDEX idx_liquidity_type ON dex_liquidity_events(type);
CREATE INDEX idx_liquidity_block ON dex_liquidity_events(block_number);
CREATE UNIQUE INDEX idx_liquidity_unique ON dex_liquidity_events(transaction_hash, log_index);
```

### Sync State Tables

```sql
-- Indexer state tracking
CREATE TABLE indexer_state (
    id SERIAL PRIMARY KEY,
    chain_id INT NOT NULL,
    last_block_number BIGINT NOT NULL DEFAULT 0,
    last_block_hash VARCHAR(66),
    last_block_timestamp BIGINT,
    highest_block_seen BIGINT DEFAULT 0,
    syncing BOOLEAN DEFAULT false,
    sync_started_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_state_chain ON indexer_state(chain_id);

-- Gap tracking for validation
CREATE TABLE sync_gaps (
    id SERIAL PRIMARY KEY,
    from_block BIGINT NOT NULL,
    to_block BIGINT NOT NULL,
    reason TEXT,
    filled BOOLEAN DEFAULT FALSE,
    attempts INT DEFAULT 0,
    last_attempt_at TIMESTAMP,
    filled_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_gaps_unfilled ON sync_gaps(filled) WHERE filled = FALSE;
CREATE INDEX idx_gaps_range ON sync_gaps(from_block, to_block);

-- Processing queue
CREATE TABLE processing_queue (
    id BIGSERIAL PRIMARY KEY,
    item_type VARCHAR(20) NOT NULL, -- 'block', 'transaction', 'reprocess'
    item_id VARCHAR(66) NOT NULL,
    priority INT DEFAULT 5, -- 1-10, 1 being highest
    status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
    attempts INT DEFAULT 0,
    max_attempts INT DEFAULT 3,
    error_message TEXT,
    metadata JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    processed_at TIMESTAMP
);

CREATE INDEX idx_queue_status ON processing_queue(status);
CREATE INDEX idx_queue_priority ON processing_queue(priority, created_at) WHERE status = 'pending';
CREATE INDEX idx_queue_type ON processing_queue(item_type);
```

## Implementation Phases

### Phase 1: Core Infrastructure (Week 1-2)

#### 1.1 Project Setup
- Initialize Go module structure
- Set up development environment
- Configure PostgreSQL database
- Implement configuration management with Viper
- Set up structured logging with zerolog

#### 1.2 Database Layer
- Create database schema and migrations
- Implement migration runner
- Set up connection pooling with pgx/v5
- Create repository interfaces
- Implement basic CRUD operations

#### 1.3 RPC Client
- Implement RPC client with connection pooling
- Add retry logic with exponential backoff
- Create block and transaction type definitions
- Implement batch RPC calls
- Add health check for RPC endpoints

#### 1.4 Basic Processors
- Implement block processor
- Implement transaction processor
- Create event log decoder
- Add basic validation logic
- Implement database batch inserts

### Phase 2: Sync Management (Week 3)

#### 2.1 Sync Manager
- Implement sync state management
- Create block fetcher with polling
- Add gap detection algorithm
- Implement backfill mechanism
- Create recovery procedures

#### 2.2 Validation System
- Implement block continuity validation
- Add parent hash verification
- Create reorganization detection
- Implement rollback mechanism
- Add integrity checks

#### 2.3 Queue System
- Implement priority queue
- Create worker pool
- Add job scheduling
- Implement retry mechanism
- Add dead letter queue

### Phase 3: Module System (Week 4-5)

#### 3.1 Module Framework
- Design module interface
- Implement module registry
- Create module lifecycle management
- Add configuration per module
- Implement module metrics

#### 3.2 ERC20 Module
- Implement Transfer event decoder
- Create token discovery mechanism
- Add balance tracking
- Implement approval tracking
- Create token metadata fetcher

#### 3.3 Uniswap V2 Module
- Implement factory event monitoring
- Create pair discovery
- Add swap event processing
- Implement liquidity tracking
- Create price calculation

#### 3.4 Uniswap V3 Module
- Implement pool discovery
- Add position tracking
- Create tick processing
- Implement fee tier handling
- Add liquidity range tracking

### Phase 4: Performance & Operations (Week 6)

#### 4.1 Performance Optimization
- Implement connection pooling optimization
- Add caching layer with Redis
- Optimize database queries
- Implement batch processing
- Add concurrent processing

#### 4.2 Monitoring
- Add Prometheus metrics
- Implement health endpoints
- Create performance dashboards
- Add alerting rules
- Implement distributed tracing

#### 4.3 Operations
- Create Docker containers
- Set up Kubernetes manifests
- Implement graceful shutdown
- Add backup procedures
- Create operational runbooks

### Phase 5: Testing & Documentation (Week 7)

#### 5.1 Testing
- Unit tests for all components
- Integration tests
- Load testing
- Chaos testing
- End-to-end tests

#### 5.2 Documentation
- API documentation
- Module development guide
- Deployment guide
- Operations manual
- Performance tuning guide

## Technical Specifications

### Module Interface

```go
type IndexerModule interface {
    // Metadata
    Name() string
    Type() string
    Version() string
    
    // Lifecycle
    Initialize(ctx context.Context, config json.RawMessage) error
    Start(ctx context.Context) error
    Stop(ctx context.Context) error
    
    // Processing
    Process(ctx context.Context, block *types.Block, logs []types.Log) error
    Reprocess(ctx context.Context, fromBlock, toBlock uint64) error
    
    // Filtering
    GetEventFilters() []EventFilter
    ShouldProcessBlock(blockNumber uint64) bool
    
    // State
    GetLastProcessedBlock() (uint64, error)
    SetLastProcessedBlock(blockNumber uint64) error
    
    // Metrics
    GetMetrics() ModuleMetrics
}

type EventFilter struct {
    Address   []common.Address
    Topics    [][]common.Hash
    FromBlock uint64
    ToBlock   uint64
}

type ModuleMetrics struct {
    ProcessedBlocks   uint64
    ProcessedEvents   uint64
    ProcessingTime    time.Duration
    LastError         error
    LastProcessedAt   time.Time
}
```

### Priority Queue Implementation

```go
type Priority int

const (
    PriorityUrgent Priority = iota + 1
    PriorityHigh
    PriorityMedium
    PriorityLow
    PriorityDeferred
)

type QueueItem struct {
    ID        string
    Type      string
    Priority  Priority
    Payload   interface{}
    Attempts  int
    CreatedAt time.Time
}

type PriorityQueue interface {
    Push(item *QueueItem) error
    Pop() (*QueueItem, error)
    Peek() (*QueueItem, error)
    Size() int
    Clear() error
}
```

### Configuration Structure

```yaml
# config.yaml
server:
  port: 8080
  metrics_port: 9090
  health_check_interval: 30s

chain:
  name: "zilliqa"
  chain_id: 32769
  rpc_endpoints:
    - url: "https://api.zilliqa.com"
      weight: 1
      max_connections: 10
    - url: "https://api2.zilliqa.com"
      weight: 1
      max_connections: 10
  block_time: 1s
  confirmations: 12
  batch_size: 100

database:
  host: "localhost"
  port: 5432
  name: "zilstream"
  user: "indexer"
  password: "${DB_PASSWORD}"
  ssl_mode: "require"
  max_connections: 20
  max_idle_connections: 5
  connection_max_lifetime: 1h

processor:
  workers: 10
  batch_size: 1000
  queue_size: 10000
  retry_attempts: 3
  retry_delay: 1s
  processing_timeout: 30s

modules:
  erc20:
    enabled: true
    type: "token"
    config:
      batch_size: 100
      cache_ttl: 5m
      
  uniswap_v2:
    enabled: true
    type: "dex"
    config:
      factory_address: "0x..."
      router_address: "0x..."
      minimum_liquidity: "1000000"
      
  uniswap_v3:
    enabled: true
    type: "dex"
    config:
      factory_address: "0x..."
      position_manager: "0x..."
      fee_tiers: [500, 3000, 10000]

cache:
  redis:
    enabled: true
    address: "localhost:6379"
    password: "${REDIS_PASSWORD}"
    db: 0
    ttl: 5m

monitoring:
  prometheus:
    enabled: true
    namespace: "indexer"
    subsystem: "zilliqa"
  
  logging:
    level: "info"
    format: "json"
    output: "stdout"
```

## Performance Considerations

### Optimization Strategies

1. **Database Optimizations**
   - Use COPY for bulk inserts
   - Implement table partitioning for large tables
   - Use BRIN indexes for time-series data
   - Implement materialized views for complex queries
   - Regular VACUUM and ANALYZE

2. **Concurrency Model**
   - Worker pool pattern for parallel processing
   - Channel-based communication
   - Context-based cancellation
   - Graceful shutdown handling

3. **Memory Management**
   - Object pooling for frequent allocations
   - Bounded channels to prevent memory overflow
   - Regular garbage collection tuning
   - Memory profiling and optimization

4. **Network Optimization**
   - Connection pooling for RPC and database
   - HTTP/2 for RPC connections
   - Request batching
   - Circuit breaker pattern

### Scalability Considerations

1. **Horizontal Scaling**
   - Stateless indexer design
   - Distributed locking for coordination
   - Sharded database architecture
   - Load balancing across RPC nodes

2. **Vertical Scaling**
   - Efficient CPU utilization
   - Optimized memory usage
   - SSD storage for database
   - Network bandwidth optimization

## Monitoring & Alerting

### Key Metrics

1. **Application Metrics**
   - Blocks processed per second
   - Transaction processing rate
   - Module processing time
   - Queue depth and latency
   - Error rates by component

2. **System Metrics**
   - CPU utilization
   - Memory usage
   - Disk I/O
   - Network throughput
   - Database connection pool stats

3. **Business Metrics**
   - Token discovery rate
   - DEX volume tracking
   - Active addresses
   - Gas usage trends

### Alert Conditions

1. **Critical Alerts**
   - Indexer stopped processing
   - Database connection failure
   - RPC endpoint unavailable
   - Gap detected in blocks
   - High error rate (>5%)

2. **Warning Alerts**
   - Processing lag > 100 blocks
   - Queue depth > 80% capacity
   - Memory usage > 80%
   - Slow query performance
   - Module processing errors

## Security Considerations

1. **Access Control**
   - Database user permissions
   - API authentication
   - Environment variable management
   - Secrets rotation

2. **Data Validation**
   - Input sanitization
   - Transaction verification
   - Block hash validation
   - Event signature verification

3. **Operational Security**
   - Audit logging
   - Rate limiting
   - DDoS protection
   - Backup encryption

## Deployment Strategy

### Development Environment
```bash
# Local development
docker-compose up -d postgres redis
go run cmd/indexer/main.go --config=config.dev.yaml
```

### Production Deployment
```bash
# Build Docker image
docker build -t zilstream-indexer:latest .

# Deploy to Kubernetes
kubectl apply -f deployments/kubernetes/
```

### CI/CD Pipeline
1. Code commit triggers build
2. Run unit tests
3. Run integration tests
4. Build Docker image
5. Push to registry
6. Deploy to staging
7. Run smoke tests
8. Deploy to production
9. Monitor deployment

## Testing Strategy

### Test Coverage Goals
- Unit tests: 80% coverage
- Integration tests: Critical paths
- End-to-end tests: Main workflows
- Performance tests: Load scenarios

### Test Categories

1. **Unit Tests**
   - Individual component testing
   - Mock external dependencies
   - Fast execution
   - High coverage

2. **Integration Tests**
   - Component interaction
   - Database operations
   - RPC communication
   - Module processing

3. **End-to-End Tests**
   - Complete workflows
   - Real blockchain data
   - Performance validation
   - Recovery scenarios

## Risk Mitigation

### Technical Risks

1. **RPC Node Reliability**
   - Mitigation: Multiple RPC endpoints
   - Fallback mechanisms
   - Health monitoring

2. **Database Performance**
   - Mitigation: Query optimization
   - Indexing strategy
   - Partitioning scheme

3. **Chain Reorganizations**
   - Mitigation: Confirmation depth
   - Rollback capability
   - State validation

### Operational Risks

1. **Data Loss**
   - Mitigation: Regular backups
   - Point-in-time recovery
   - Disaster recovery plan

2. **Service Downtime**
   - Mitigation: High availability setup
   - Automatic failover
   - Monitoring and alerting

## Success Criteria

### Performance Targets
- Process blocks within 2 seconds of production
- Handle 1000+ transactions per second
- 99.9% uptime SLA
- < 100ms query response time

### Functional Goals
- Zero gaps in indexed data
- Accurate token balances
- Complete DEX activity tracking
- Extensible module system

### Operational Goals
- Automated deployment
- Self-healing capabilities
- Comprehensive monitoring
- Clear documentation

## Timeline

### Week 1-2: Foundation
- Project setup
- Core infrastructure
- Basic indexing

### Week 3: Synchronization
- Sync management
- Validation system
- Queue implementation

### Week 4-5: Modules
- Module framework
- ERC20 implementation
- DEX implementations

### Week 6: Operations
- Performance optimization
- Monitoring setup
- Deployment preparation

### Week 7: Polish
- Testing completion
- Documentation
- Production readiness

## Conclusion

This plan provides a comprehensive roadmap for building a production-grade EVM indexer for Zilliqa. The modular architecture ensures extensibility, while the focus on performance and reliability ensures the system can handle the demanding requirements of a 1-second block time blockchain.

Key success factors:
- Robust error handling and recovery
- Efficient concurrent processing
- Comprehensive monitoring
- Extensible module system
- Zero-gap validation

The implementation should prioritize reliability and performance while maintaining clean, maintainable code that can be extended for future requirements.