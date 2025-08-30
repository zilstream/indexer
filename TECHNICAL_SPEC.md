# Technical Specification - Zilliqa EVM Indexer

## External Dependencies

### Core Dependencies

#### 1. Ethereum/Blockchain Libraries
```go
// go-ethereum - Core Ethereum types and utilities
github.com/ethereum/go-ethereum v1.13.5
// Used for:
// - Common types (Address, Hash, Block, Transaction)
// - ABI encoding/decoding
// - RLP encoding
// - Crypto utilities
// - Event log filtering

// Example usage:
import (
    "github.com/ethereum/go-ethereum/common"
    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethereum/go-ethereum/accounts/abi"
    "github.com/ethereum/go-ethereum/ethclient"
    "github.com/ethereum/go-ethereum/rpc"
)
```

#### 2. Database Driver
```go
// pgx - PostgreSQL driver and toolkit
github.com/jackc/pgx/v5 v5.5.1
// Used for:
// - Connection pooling
// - Batch operations
// - COPY protocol support
// - Native PostgreSQL types
// - Prepared statements

// Supporting packages
github.com/jackc/pgtype v1.14.0  // PostgreSQL type handling
github.com/jackc/pgconn v1.14.0  // Low-level PostgreSQL connection

// Example connection pool setup:
type Database struct {
    pool *pgxpool.Pool
}

func NewDatabase(ctx context.Context, connString string) (*Database, error) {
    config, err := pgxpool.ParseConfig(connString)
    if err != nil {
        return nil, err
    }
    
    config.MaxConns = 20
    config.MinConns = 5
    config.MaxConnLifetime = time.Hour
    config.MaxConnIdleTime = time.Minute * 30
    
    pool, err := pgxpool.NewWithConfig(ctx, config)
    if err != nil {
        return nil, err
    }
    
    return &Database{pool: pool}, nil
}
```

#### 3. Database Migrations
```go
// golang-migrate - Database migration tool
github.com/golang-migrate/migrate/v4 v4.17.0
// Used for:
// - Schema versioning
// - Up/down migrations
// - Embedded migration files

// Supporting packages
github.com/golang-migrate/migrate/v4/database/postgres
github.com/golang-migrate/migrate/v4/source/file
github.com/golang-migrate/migrate/v4/source/embed

// Example usage:
//go:embed migrations/*.sql
var migrationsFS embed.FS

func RunMigrations(databaseURL string) error {
    source, err := iofs.New(migrationsFS, "migrations")
    if err != nil {
        return err
    }
    
    m, err := migrate.NewWithSourceInstance("iofs", source, databaseURL)
    if err != nil {
        return err
    }
    
    return m.Up()
}
```

#### 4. Configuration Management
```go
// viper - Configuration management
github.com/spf13/viper v1.18.2
// Used for:
// - YAML/JSON/TOML config files
// - Environment variable binding
// - Hot reload support
// - Default values

// cobra - CLI framework
github.com/spf13/cobra v1.8.0
// Used for:
// - Command-line interface
// - Subcommands
// - Flag parsing

// Example configuration:
type Config struct {
    Server   ServerConfig   `mapstructure:"server"`
    Chain    ChainConfig    `mapstructure:"chain"`
    Database DatabaseConfig `mapstructure:"database"`
    Modules  []ModuleConfig `mapstructure:"modules"`
}

func LoadConfig(path string) (*Config, error) {
    viper.SetConfigFile(path)
    viper.SetEnvPrefix("INDEXER")
    viper.AutomaticEnv()
    
    if err := viper.ReadInConfig(); err != nil {
        return nil, err
    }
    
    var config Config
    if err := viper.Unmarshal(&config); err != nil {
        return nil, err
    }
    
    return &config, nil
}
```

#### 5. Logging
```go
// zerolog - Structured logging
github.com/rs/zerolog v1.31.0
// Used for:
// - JSON structured logs
// - High performance
// - Context logging
// - Log levels

// Example setup:
func SetupLogger(level string) zerolog.Logger {
    zerolog.TimeFieldFormat = time.RFC3339Nano
    
    logLevel, err := zerolog.ParseLevel(level)
    if err != nil {
        logLevel = zerolog.InfoLevel
    }
    
    logger := zerolog.New(os.Stdout).
        Level(logLevel).
        With().
        Timestamp().
        Caller().
        Logger()
    
    return logger
}

// Usage:
log.Info().
    Str("module", "indexer").
    Uint64("block", blockNumber).
    Dur("duration", duration).
    Msg("Block processed")
```

#### 6. Metrics & Monitoring
```go
// prometheus - Metrics collection
github.com/prometheus/client_golang v1.18.0
// Used for:
// - Counter, Gauge, Histogram metrics
// - HTTP metrics endpoint
// - Custom collectors

// Example metrics:
var (
    blocksProcessed = prometheus.NewCounterVec(
        prometheus.CounterOpts{
            Name: "indexer_blocks_processed_total",
            Help: "Total number of blocks processed",
        },
        []string{"status"},
    )
    
    processingDuration = prometheus.NewHistogramVec(
        prometheus.HistogramOpts{
            Name:    "indexer_processing_duration_seconds",
            Help:    "Processing duration in seconds",
            Buckets: prometheus.DefBuckets,
        },
        []string{"type"},
    )
    
    currentBlockHeight = prometheus.NewGauge(
        prometheus.GaugeOpts{
            Name: "indexer_current_block_height",
            Help: "Current indexed block height",
        },
    )
)

func init() {
    prometheus.MustRegister(blocksProcessed)
    prometheus.MustRegister(processingDuration)
    prometheus.MustRegister(currentBlockHeight)
}
```

#### 7. HTTP Server & Middleware
```go
// gin - HTTP web framework
github.com/gin-gonic/gin v1.9.1
// Used for:
// - REST API endpoints
// - Middleware support
// - Request validation
// - Response formatting

// chi - Alternative lightweight router
github.com/go-chi/chi/v5 v5.0.11
// Used for:
// - API routing
// - Middleware stacking
// - Context handling

// Example API setup:
func SetupRouter(db *Database) *gin.Engine {
    r := gin.New()
    
    // Middleware
    r.Use(gin.Recovery())
    r.Use(gin.Logger())
    r.Use(cors.Default())
    
    // Health check
    r.GET("/health", func(c *gin.Context) {
        c.JSON(200, gin.H{"status": "healthy"})
    })
    
    // API routes
    api := r.Group("/api/v1")
    {
        api.GET("/blocks/:number", getBlock(db))
        api.GET("/transactions/:hash", getTransaction(db))
        api.GET("/tokens", getTokens(db))
    }
    
    return r
}
```

#### 8. Caching
```go
// go-redis - Redis client
github.com/redis/go-redis/v9 v9.4.0
// Used for:
// - Response caching
// - Temporary data storage
// - Pub/sub messaging
// - Distributed locking

// ristretto - In-memory cache
github.com/dgraph-io/ristretto v0.1.1
// Used for:
// - High-performance caching
// - TTL support
// - Cost-based eviction

// Example Redis cache:
type Cache struct {
    client *redis.Client
}

func NewCache(addr string) *Cache {
    client := redis.NewClient(&redis.Options{
        Addr:         addr,
        PoolSize:     10,
        MinIdleConns: 5,
        MaxRetries:   3,
    })
    
    return &Cache{client: client}
}

func (c *Cache) GetBlock(ctx context.Context, number uint64) (*Block, error) {
    key := fmt.Sprintf("block:%d", number)
    
    data, err := c.client.Get(ctx, key).Bytes()
    if err == redis.Nil {
        return nil, nil
    }
    if err != nil {
        return nil, err
    }
    
    var block Block
    if err := json.Unmarshal(data, &block); err != nil {
        return nil, err
    }
    
    return &block, nil
}
```

#### 9. Retry & Circuit Breaker
```go
// backoff - Exponential backoff
github.com/cenkalti/backoff/v4 v4.2.1
// Used for:
// - Retry logic
// - Exponential backoff
// - Jittered delays

// go-resiliency - Circuit breaker
github.com/eapache/go-resiliency v1.5.0
// Used for:
// - Circuit breaker pattern
// - Failure detection
// - Automatic recovery

// Example retry logic:
func RetryableRPCCall(fn func() error) error {
    backoffConfig := backoff.NewExponentialBackOff()
    backoffConfig.InitialInterval = 100 * time.Millisecond
    backoffConfig.MaxInterval = 10 * time.Second
    backoffConfig.MaxElapsedTime = 2 * time.Minute
    
    return backoff.Retry(fn, backoffConfig)
}
```

#### 10. Testing
```go
// testify - Testing toolkit
github.com/stretchr/testify v1.8.4
// Used for:
// - Assertions
// - Mocking
// - Suite testing

// gomock - Mock framework
github.com/golang/mock v1.6.0
// Used for:
// - Interface mocking
// - Behavior verification

// dockertest - Integration testing
github.com/ory/dockertest/v3 v3.10.0
// Used for:
// - PostgreSQL test containers
// - Redis test containers
// - Integration tests

// Example test setup:
func TestDatabaseIntegration(t *testing.T) {
    pool, err := dockertest.NewPool("")
    require.NoError(t, err)
    
    resource, err := pool.RunWithOptions(&dockertest.RunOptions{
        Repository: "postgres",
        Tag:        "15-alpine",
        Env: []string{
            "POSTGRES_PASSWORD=test",
            "POSTGRES_DB=indexer_test",
        },
    })
    require.NoError(t, err)
    
    defer pool.Purge(resource)
    
    // Wait for database to be ready
    var db *Database
    err = pool.Retry(func() error {
        connString := fmt.Sprintf(
            "postgres://postgres:test@localhost:%s/indexer_test",
            resource.GetPort("5432/tcp"),
        )
        db, err = NewDatabase(context.Background(), connString)
        return err
    })
    require.NoError(t, err)
    
    // Run tests
    t.Run("InsertBlock", func(t *testing.T) {
        // Test implementation
    })
}
```

#### 11. Additional Utilities
```go
// decimal - Arbitrary precision decimals
github.com/shopspring/decimal v1.3.1
// Used for:
// - Token amounts
// - Price calculations
// - No floating point errors

// uuid - UUID generation
github.com/google/uuid v1.5.0
// Used for:
// - Unique identifiers
// - Request tracing

// errgroup - Goroutine synchronization
golang.org/x/sync/errgroup
// Used for:
// - Concurrent operations
// - Error propagation
// - Context cancellation

// rate - Rate limiting
golang.org/x/time/rate
// Used for:
// - RPC rate limiting
// - API throttling
```

## go.mod File

```go
module github.com/zilstream/indexer

go 1.21

require (
    // Core
    github.com/ethereum/go-ethereum v1.13.5
    
    // Database
    github.com/jackc/pgx/v5 v5.5.1
    github.com/golang-migrate/migrate/v4 v4.17.0
    
    // Configuration
    github.com/spf13/viper v1.18.2
    github.com/spf13/cobra v1.8.0
    
    // Logging & Metrics
    github.com/rs/zerolog v1.31.0
    github.com/prometheus/client_golang v1.18.0
    
    // HTTP
    github.com/gin-gonic/gin v1.9.1
    github.com/gorilla/websocket v1.5.1
    
    // Cache
    github.com/redis/go-redis/v9 v9.4.0
    github.com/dgraph-io/ristretto v0.1.1
    
    // Utilities
    github.com/shopspring/decimal v1.3.1
    github.com/google/uuid v1.5.0
    github.com/cenkalti/backoff/v4 v4.2.1
    golang.org/x/sync v0.5.0
    golang.org/x/time v0.5.0
    
    // Testing
    github.com/stretchr/testify v1.8.4
    github.com/golang/mock v1.6.0
    github.com/ory/dockertest/v3 v3.10.0
)
```

## Code Architecture Patterns

### 1. Repository Pattern

```go
// Repository interface for data access
type BlockRepository interface {
    Insert(ctx context.Context, block *Block) error
    InsertBatch(ctx context.Context, blocks []*Block) error
    GetByNumber(ctx context.Context, number uint64) (*Block, error)
    GetByHash(ctx context.Context, hash common.Hash) (*Block, error)
    GetRange(ctx context.Context, from, to uint64) ([]*Block, error)
    GetLatest(ctx context.Context) (*Block, error)
    Exists(ctx context.Context, number uint64) (bool, error)
}

// PostgreSQL implementation
type postgresBlockRepository struct {
    db *pgxpool.Pool
}

func (r *postgresBlockRepository) InsertBatch(ctx context.Context, blocks []*Block) error {
    batch := &pgx.Batch{}
    
    query := `
        INSERT INTO blocks (number, hash, parent_hash, timestamp, gas_limit, gas_used)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (number) DO NOTHING
    `
    
    for _, block := range blocks {
        batch.Queue(query, 
            block.Number, 
            block.Hash.Hex(), 
            block.ParentHash.Hex(),
            block.Timestamp,
            block.GasLimit,
            block.GasUsed,
        )
    }
    
    br := r.db.SendBatch(ctx, batch)
    defer br.Close()
    
    for i := 0; i < batch.Len(); i++ {
        if _, err := br.Exec(); err != nil {
            return fmt.Errorf("failed to insert block %d: %w", blocks[i].Number, err)
        }
    }
    
    return nil
}
```

### 2. Worker Pool Pattern

```go
type WorkerPool struct {
    workers   int
    jobQueue  chan Job
    resultCh  chan Result
    errorCh   chan error
    wg        sync.WaitGroup
    ctx       context.Context
    cancel    context.CancelFunc
}

type Job struct {
    ID      string
    Type    JobType
    Payload interface{}
}

type Result struct {
    JobID string
    Data  interface{}
}

func NewWorkerPool(workers int, queueSize int) *WorkerPool {
    ctx, cancel := context.WithCancel(context.Background())
    
    return &WorkerPool{
        workers:  workers,
        jobQueue: make(chan Job, queueSize),
        resultCh: make(chan Result, queueSize),
        errorCh:  make(chan error, workers),
        ctx:      ctx,
        cancel:   cancel,
    }
}

func (wp *WorkerPool) Start() {
    for i := 0; i < wp.workers; i++ {
        wp.wg.Add(1)
        go wp.worker(i)
    }
}

func (wp *WorkerPool) worker(id int) {
    defer wp.wg.Done()
    
    for {
        select {
        case <-wp.ctx.Done():
            return
        case job := <-wp.jobQueue:
            result, err := wp.processJob(job)
            if err != nil {
                wp.errorCh <- fmt.Errorf("worker %d: %w", id, err)
                continue
            }
            wp.resultCh <- result
        }
    }
}

func (wp *WorkerPool) Submit(job Job) {
    select {
    case wp.jobQueue <- job:
    case <-wp.ctx.Done():
    }
}

func (wp *WorkerPool) Stop() {
    wp.cancel()
    wp.wg.Wait()
    close(wp.jobQueue)
    close(wp.resultCh)
    close(wp.errorCh)
}
```

### 3. Pipeline Pattern

```go
type Pipeline struct {
    stages []Stage
}

type Stage interface {
    Process(ctx context.Context, data interface{}) (interface{}, error)
    Name() string
}

func NewPipeline(stages ...Stage) *Pipeline {
    return &Pipeline{stages: stages}
}

func (p *Pipeline) Execute(ctx context.Context, input interface{}) (interface{}, error) {
    var data interface{} = input
    
    for _, stage := range p.stages {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()
        default:
            result, err := stage.Process(ctx, data)
            if err != nil {
                return nil, fmt.Errorf("stage %s failed: %w", stage.Name(), err)
            }
            data = result
        }
    }
    
    return data, nil
}

// Example stages
type FetchBlockStage struct {
    client *ethclient.Client
}

func (s *FetchBlockStage) Process(ctx context.Context, data interface{}) (interface{}, error) {
    blockNumber := data.(uint64)
    block, err := s.client.BlockByNumber(ctx, big.NewInt(int64(blockNumber)))
    if err != nil {
        return nil, err
    }
    return block, nil
}

type ValidateBlockStage struct {
    validator *BlockValidator
}

func (s *ValidateBlockStage) Process(ctx context.Context, data interface{}) (interface{}, error) {
    block := data.(*types.Block)
    if err := s.validator.Validate(block); err != nil {
        return nil, err
    }
    return block, nil
}

type StoreBlockStage struct {
    repo BlockRepository
}

func (s *StoreBlockStage) Process(ctx context.Context, data interface{}) (interface{}, error) {
    block := data.(*types.Block)
    if err := s.repo.Insert(ctx, convertBlock(block)); err != nil {
        return nil, err
    }
    return block, nil
}
```

### 4. Event-Driven Architecture

```go
type EventBus struct {
    subscribers map[EventType][]chan Event
    mu          sync.RWMutex
}

type EventType string

const (
    EventBlockProcessed EventType = "block.processed"
    EventTokenDiscovered EventType = "token.discovered"
    EventSyncGapDetected EventType = "sync.gap_detected"
)

type Event struct {
    Type      EventType
    Timestamp time.Time
    Data      interface{}
}

func NewEventBus() *EventBus {
    return &EventBus{
        subscribers: make(map[EventType][]chan Event),
    }
}

func (eb *EventBus) Subscribe(eventType EventType, ch chan Event) {
    eb.mu.Lock()
    defer eb.mu.Unlock()
    
    eb.subscribers[eventType] = append(eb.subscribers[eventType], ch)
}

func (eb *EventBus) Publish(event Event) {
    eb.mu.RLock()
    defer eb.mu.RUnlock()
    
    for _, ch := range eb.subscribers[event.Type] {
        select {
        case ch <- event:
        default:
            // Channel full, skip
        }
    }
}
```

### 5. Circuit Breaker Implementation

```go
type CircuitBreaker struct {
    maxFailures  int
    resetTimeout time.Duration
    
    failures     int
    lastFailTime time.Time
    state        State
    mu           sync.RWMutex
}

type State int

const (
    StateClosed State = iota
    StateOpen
    StateHalfOpen
)

func NewCircuitBreaker(maxFailures int, resetTimeout time.Duration) *CircuitBreaker {
    return &CircuitBreaker{
        maxFailures:  maxFailures,
        resetTimeout: resetTimeout,
        state:        StateClosed,
    }
}

func (cb *CircuitBreaker) Call(fn func() error) error {
    cb.mu.Lock()
    defer cb.mu.Unlock()
    
    if cb.state == StateOpen {
        if time.Since(cb.lastFailTime) > cb.resetTimeout {
            cb.state = StateHalfOpen
            cb.failures = 0
        } else {
            return ErrCircuitOpen
        }
    }
    
    err := fn()
    if err != nil {
        cb.failures++
        cb.lastFailTime = time.Now()
        
        if cb.failures >= cb.maxFailures {
            cb.state = StateOpen
        }
        
        return err
    }
    
    if cb.state == StateHalfOpen {
        cb.state = StateClosed
    }
    cb.failures = 0
    
    return nil
}
```

## Performance Optimizations

### 1. Batch Processing

```go
type BatchProcessor struct {
    batchSize     int
    flushInterval time.Duration
    processor     func(items []interface{}) error
    
    items  []interface{}
    mu     sync.Mutex
    timer  *time.Timer
}

func NewBatchProcessor(size int, interval time.Duration, processor func([]interface{}) error) *BatchProcessor {
    return &BatchProcessor{
        batchSize:     size,
        flushInterval: interval,
        processor:     processor,
        items:         make([]interface{}, 0, size),
    }
}

func (bp *BatchProcessor) Add(item interface{}) error {
    bp.mu.Lock()
    defer bp.mu.Unlock()
    
    bp.items = append(bp.items, item)
    
    if len(bp.items) >= bp.batchSize {
        return bp.flush()
    }
    
    if bp.timer == nil {
        bp.timer = time.AfterFunc(bp.flushInterval, func() {
            bp.mu.Lock()
            defer bp.mu.Unlock()
            bp.flush()
        })
    }
    
    return nil
}

func (bp *BatchProcessor) flush() error {
    if len(bp.items) == 0 {
        return nil
    }
    
    items := bp.items
    bp.items = make([]interface{}, 0, bp.batchSize)
    
    if bp.timer != nil {
        bp.timer.Stop()
        bp.timer = nil
    }
    
    return bp.processor(items)
}
```

### 2. Connection Pool Management

```go
type RPCPool struct {
    clients   []*ethclient.Client
    weights   []int
    totalWeight int
    current   int
    mu        sync.Mutex
}

func NewRPCPool(endpoints []RPCEndpoint) (*RPCPool, error) {
    pool := &RPCPool{
        clients: make([]*ethclient.Client, 0, len(endpoints)),
        weights: make([]int, 0, len(endpoints)),
    }
    
    for _, endpoint := range endpoints {
        client, err := ethclient.Dial(endpoint.URL)
        if err != nil {
            return nil, err
        }
        
        pool.clients = append(pool.clients, client)
        pool.weights = append(pool.weights, endpoint.Weight)
        pool.totalWeight += endpoint.Weight
    }
    
    return pool, nil
}

func (p *RPCPool) GetClient() *ethclient.Client {
    p.mu.Lock()
    defer p.mu.Unlock()
    
    // Weighted round-robin selection
    selected := p.current % len(p.clients)
    p.current++
    
    return p.clients[selected]
}

func (p *RPCPool) CallWithRetry(ctx context.Context, fn func(*ethclient.Client) error) error {
    var lastErr error
    
    for i := 0; i < len(p.clients); i++ {
        client := p.GetClient()
        
        if err := fn(client); err != nil {
            lastErr = err
            continue
        }
        
        return nil
    }
    
    return fmt.Errorf("all clients failed: %w", lastErr)
}
```

### 3. Memory Pool for Object Reuse

```go
var (
    blockPool = sync.Pool{
        New: func() interface{} {
            return &Block{}
        },
    }
    
    transactionPool = sync.Pool{
        New: func() interface{} {
            return &Transaction{}
        },
    }
)

func GetBlock() *Block {
    return blockPool.Get().(*Block)
}

func PutBlock(b *Block) {
    b.Reset() // Clear the block data
    blockPool.Put(b)
}

// Usage example
func ProcessBlocks(blocks []uint64) {
    for _, num := range blocks {
        block := GetBlock()
        defer PutBlock(block)
        
        // Process block
        if err := fetchAndProcess(num, block); err != nil {
            log.Error().Err(err).Msg("Failed to process block")
        }
    }
}
```

## Testing Strategies

### 1. Unit Test Example

```go
func TestBlockProcessor_Process(t *testing.T) {
    ctrl := gomock.NewController(t)
    defer ctrl.Finish()
    
    mockRepo := mocks.NewMockBlockRepository(ctrl)
    mockClient := mocks.NewMockEthClient(ctrl)
    
    processor := NewBlockProcessor(mockClient, mockRepo)
    
    testBlock := &types.Block{
        Number: big.NewInt(100),
        Hash:   common.HexToHash("0x123"),
    }
    
    mockClient.EXPECT().
        BlockByNumber(gomock.Any(), big.NewInt(100)).
        Return(testBlock, nil)
    
    mockRepo.EXPECT().
        Insert(gomock.Any(), gomock.Any()).
        Return(nil)
    
    err := processor.Process(context.Background(), 100)
    assert.NoError(t, err)
}
```

### 2. Integration Test Example

```go
func TestIntegration_FullPipeline(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping integration test")
    }
    
    // Setup test database
    db := setupTestDatabase(t)
    defer db.Close()
    
    // Setup test RPC server
    rpcServer := setupMockRPCServer(t)
    defer rpcServer.Close()
    
    // Create indexer
    indexer := NewIndexer(
        WithDatabase(db),
        WithRPCEndpoint(rpcServer.URL),
        WithWorkers(5),
    )
    
    // Start indexer
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()
    
    go indexer.Start(ctx)
    
    // Wait for blocks to be processed
    time.Sleep(5 * time.Second)
    
    // Verify results
    var count int
    err := db.QueryRow("SELECT COUNT(*) FROM blocks").Scan(&count)
    require.NoError(t, err)
    assert.Greater(t, count, 0)
}
```

### 3. Benchmark Example

```go
func BenchmarkBlockProcessing(b *testing.B) {
    processor := setupTestProcessor(b)
    blocks := generateTestBlocks(1000)
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        for _, block := range blocks {
            _ = processor.Process(context.Background(), block)
        }
    }
}

func BenchmarkBatchInsert(b *testing.B) {
    db := setupBenchDatabase(b)
    defer db.Close()
    
    repo := NewBlockRepository(db)
    blocks := generateTestBlocks(1000)
    
    b.Run("Single", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            for _, block := range blocks {
                _ = repo.Insert(context.Background(), block)
            }
        }
    })
    
    b.Run("Batch", func(b *testing.B) {
        for i := 0; i < b.N; i++ {
            _ = repo.InsertBatch(context.Background(), blocks)
        }
    })
}
```

## Deployment Configuration

### Dockerfile

```dockerfile
# Build stage
FROM golang:1.21-alpine AS builder

RUN apk add --no-cache git make

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN make build

# Final stage
FROM alpine:3.19

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/bin/indexer /app/indexer
COPY --from=builder /app/migrations /app/migrations
COPY --from=builder /app/config.yaml /app/config.yaml

EXPOSE 8080 9090

ENTRYPOINT ["/app/indexer"]
CMD ["start"]
```

### Makefile

```makefile
.PHONY: build test clean docker

# Variables
BINARY_NAME=indexer
DOCKER_IMAGE=zilstream-indexer
VERSION=$(shell git describe --tags --always --dirty)
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(shell date -u '+%Y-%m-%d_%H:%M:%S')"

# Build
build:
	@echo "Building..."
	go build $(LDFLAGS) -o bin/$(BINARY_NAME) cmd/indexer/main.go

# Test
test:
	@echo "Running tests..."
	go test -v -race -coverprofile=coverage.out ./...

test-integration:
	@echo "Running integration tests..."
	go test -v -tags=integration ./tests/...

# Benchmarks
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./...

# Lint
lint:
	@echo "Running linter..."
	golangci-lint run

# Generate
generate:
	@echo "Generating code..."
	go generate ./...
	mockgen -source=internal/storage/repository.go -destination=mocks/repository.go

# Database
migrate-up:
	migrate -path migrations -database "postgres://localhost/zilstream?sslmode=disable" up

migrate-down:
	migrate -path migrations -database "postgres://localhost/zilstream?sslmode=disable" down

# Docker
docker-build:
	docker build -t $(DOCKER_IMAGE):$(VERSION) .
	docker tag $(DOCKER_IMAGE):$(VERSION) $(DOCKER_IMAGE):latest

docker-push:
	docker push $(DOCKER_IMAGE):$(VERSION)
	docker push $(DOCKER_IMAGE):latest

# Development
dev:
	air -c .air.toml

# Clean
clean:
	@echo "Cleaning..."
	rm -rf bin/ coverage.out

# Install dependencies
deps:
	@echo "Installing dependencies..."
	go mod download
	go install github.com/golang/mock/mockgen@latest
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install github.com/cosmtrek/air@latest
```

## Performance Targets

### Metrics to Monitor

```go
// Key performance indicators
type PerformanceMetrics struct {
    // Throughput
    BlocksPerSecond      float64
    TransactionsPerSecond float64
    EventsPerSecond      float64
    
    // Latency
    BlockProcessingP50   time.Duration
    BlockProcessingP95   time.Duration
    BlockProcessingP99   time.Duration
    
    // Resource Usage
    CPUUsagePercent      float64
    MemoryUsageMB        uint64
    GoroutineCount       int
    DatabaseConnections  int
    
    // Reliability
    ErrorRate            float64
    RecoveryTime         time.Duration
    UptimePercent        float64
}

// Performance targets
var targets = PerformanceMetrics{
    BlocksPerSecond:       1.5,  // Handle 50% above chain speed
    TransactionsPerSecond: 1000, // 1000 TPS minimum
    EventsPerSecond:       5000, // 5000 events/second
    
    BlockProcessingP50:    100 * time.Millisecond,
    BlockProcessingP95:    500 * time.Millisecond,
    BlockProcessingP99:    1 * time.Second,
    
    CPUUsagePercent:       80,   // Max 80% CPU
    MemoryUsageMB:         4096, // Max 4GB RAM
    DatabaseConnections:   20,   // Connection pool size
    
    ErrorRate:     0.001, // 0.1% error rate
    RecoveryTime:  30 * time.Second,
    UptimePercent: 99.9,  // 99.9% uptime
}
```

## Conclusion

This technical specification provides a comprehensive foundation for building a production-grade EVM indexer for Zilliqa. The chosen technologies and patterns prioritize:

1. **Performance**: Concurrent processing, batching, and caching
2. **Reliability**: Circuit breakers, retry logic, and error handling
3. **Maintainability**: Clean architecture, testing, and monitoring
4. **Extensibility**: Module system and plugin architecture
5. **Operations**: Comprehensive metrics and deployment support

The architecture is designed to handle Zilliqa's 1-second block times while maintaining data integrity and providing real-time indexing capabilities.