package uniswapv3

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"

	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/modules/core"
	"github.com/zilstream/indexer/internal/modules/loader"
	"github.com/zilstream/indexer/internal/prices"
)

// UniswapV3Module implements the Module interface for Uniswap V3 indexing
type UniswapV3Module struct {
	db        *database.Database
	manifest  *core.Manifest
	logger    zerolog.Logger
	parser    *core.EventParser
	rpcClient *ethclient.Client

	// Contract addresses and ABIs
	factoryAddress common.Address
	factoryABI     *abi.ABI
	poolABI        *abi.ABI

	// Configuration
	config      *Config
	wethAddress common.Address

	// Event handlers
	handlers map[common.Hash]EventHandler

	// Pricing
	priceProvider prices.Provider
	priceRouter   prices.TokenRouter

	// Realtime publisher (optional)
	publisher PairPublisher

	// Cached config sets
	stablecoinSet map[string]struct{}
}

// PairPublisher interface for realtime updates
type PairPublisher interface {
	EnqueuePairChanged(address string)
	PublishEvent(address string, eventType string, data interface{})
}

// Config represents the module configuration
type Config struct {
	FactoryAddress string   `yaml:"factoryAddress"`
	WethAddress    string   `yaml:"wethAddress"`
	RPCEndpoint    string   `yaml:"rpcEndpoint"`
	Stablecoins    []string `yaml:"stablecoins"`
}

// EventHandler function type for handling specific events
type EventHandler func(ctx context.Context, module *UniswapV3Module, event *core.ParsedEvent) error

// TokenMetadata holds token information fetched from the contract
type TokenMetadata struct {
	Name        string
	Symbol      string
	Decimals    int
	TotalSupply *big.Int
}

// NewUniswapV3Module creates a new UniswapV3 module
func NewUniswapV3Module(logger zerolog.Logger) (*UniswapV3Module, error) {
	// Load manifest
	manifestLoader := loader.NewManifestLoader(logger)
	manifest, err := manifestLoader.LoadFromFile("manifests/uniswap-v3.yaml")
	if err != nil {
		return nil, fmt.Errorf("failed to load v3 manifest: %w", err)
	}

	// Parse configuration from manifest context
	var config Config
	if manifest.Context != nil {
		contextBytes, _ := yaml.Marshal(manifest.Context)
		if err := yaml.Unmarshal(contextBytes, &config); err != nil {
			return nil, fmt.Errorf("failed to parse v3 module config: %w", err)
		}
	}
	// Normalize config addresses to lowercase to avoid casing issues
	if config.FactoryAddress != "" { config.FactoryAddress = strings.ToLower(config.FactoryAddress) }
	if config.WethAddress != "" { config.WethAddress = strings.ToLower(config.WethAddress) }
	if len(config.Stablecoins) > 0 {
		for i := range config.Stablecoins { config.Stablecoins[i] = strings.ToLower(config.Stablecoins[i]) }
	}

	// Set up addresses - allow override via context
	factoryAddress := common.HexToAddress("0x0000000000000000000000000000000000000000")
	if config.FactoryAddress != "" {
		factoryAddress = common.HexToAddress(config.FactoryAddress)
	}
	wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2") // default; override via context
	if config.WethAddress != "" {
		wethAddress = common.HexToAddress(config.WethAddress)
	}

	module := &UniswapV3Module{
		manifest:       manifest,
		logger:         logger.With().Str("module", "uniswap-v3").Logger(),
		parser:         core.NewEventParser(),
		factoryAddress: factoryAddress,
		config:         &config,
		wethAddress:    wethAddress,
		handlers:       make(map[common.Hash]EventHandler),
	}

	// Initialize ABIs and event handlers
	if err := module.initializeABIs(); err != nil {
		return nil, fmt.Errorf("failed to initialize v3 ABIs: %w", err)
	}

	if err := module.registerEventHandlers(); err != nil {
		return nil, fmt.Errorf("failed to register v3 handlers: %w", err)
	}

	return module, nil
}

// blockTime returns the UTC minute bucket for the given block number by
// reading from the DB if present, otherwise falling back to RPC header.
// ok=false when neither source is available.
func (m *UniswapV3Module) blockTime(ctx context.Context, blockNumber uint64) (time.Time, bool) {
	// Try DB first (fast path)
	var tsSec int64
	if err := m.db.Pool().QueryRow(ctx, `SELECT timestamp FROM blocks WHERE number = $1`, blockNumber).Scan(&tsSec); err == nil && tsSec > 0 {
		return time.Unix(tsSec, 0).UTC(), true
	}
	// Fallback to RPC header (unified sync may call modules before DB commit)
	if m.rpcClient != nil {
		h, err := m.rpcClient.HeaderByNumber(ctx, big.NewInt(int64(blockNumber)))
		if err == nil && h != nil {
			return time.Unix(int64(h.Time), 0).UTC(), true
		}
	}
	return time.Time{}, false
}

// Name returns the module name
func (m *UniswapV3Module) Name() string { return m.manifest.Name }

// Version returns the module version
func (m *UniswapV3Module) Version() string { return m.manifest.Version }

// Manifest returns the module manifest
func (m *UniswapV3Module) Manifest() *core.Manifest { return m.manifest }

// SetRPCClient sets the RPC client for the module
func (m *UniswapV3Module) SetRPCClient(client *ethclient.Client) { m.rpcClient = client }

// SetPriceProvider injects the price provider
func (m *UniswapV3Module) SetPriceProvider(p prices.Provider) { m.priceProvider = p }

func (m *UniswapV3Module) SetPublisher(publisher PairPublisher) {
	m.publisher = publisher
}

// internal: build stablecoin set from config
func (m *UniswapV3Module) buildStablecoinSet() {
	set := make(map[string]struct{})
	if m.config != nil {
		for _, a := range m.config.Stablecoins {
			set[strings.ToLower(a)] = struct{}{}
		}
	}
	m.stablecoinSet = set
}

// Initialize sets up the module with database connection
func (m *UniswapV3Module) Initialize(ctx context.Context, db *database.Database) error {
	m.db = db

	// Connect to RPC if we have an endpoint configured
	if m.config != nil && m.config.RPCEndpoint != "" {
		client, err := ethclient.Dial(m.config.RPCEndpoint)
		if err != nil {
			m.logger.Warn().Err(err).Msg("Failed to connect to RPC for v3 (metadata optional)")
		} else {
			m.rpcClient = client
			m.logger.Info().Str("endpoint", m.config.RPCEndpoint).Msg("Connected to RPC for v3")
		}
	}

	// Ensure factory exists in database if a concrete address is configured
	var startBlock uint64 = 0
	if len(m.manifest.DataSources) > 0 && m.manifest.DataSources[0].Source.StartBlock != nil {
		startBlock = *m.manifest.DataSources[0].Source.StartBlock
	}

	zeroAddr := common.Address{}
	if m.factoryAddress != zeroAddr {
		query := `
			INSERT INTO uniswap_v3_factory (address, pool_count, created_at_block, created_at_timestamp, updated_at)
			VALUES ($1, 0, $2, extract(epoch from now())::bigint, NOW())
			ON CONFLICT (address) DO NOTHING`

		_, err := db.Pool().Exec(ctx, query, strings.ToLower(m.factoryAddress.Hex()), startBlock)
		if err != nil {
			return fmt.Errorf("failed to initialize v3 factory: %w", err)
		}
	}

	// Build stablecoin set and price router if provider is available
	m.buildStablecoinSet()
	if m.priceProvider != nil {
		m.priceRouter = prices.NewDBRouter(db.Pool(), m.priceProvider, m.wethAddress.Hex(), m.config.Stablecoins)
	}

	m.logger.Info().Str("factory", m.factoryAddress.Hex()).Msg("UniswapV3 module initialized")
	return nil
}

// fetchTokenMetadata fetches token metadata via ERC20 calls (only used when token is new)
func (m *UniswapV3Module) fetchTokenMetadata(ctx context.Context, tokenAddress common.Address) (*TokenMetadata, error) {
	metadata := &TokenMetadata{
		TotalSupply: big.NewInt(0),
		Name:        "Unknown",
		Symbol:      "???",
		Decimals:    18,
	}

	if m.rpcClient == nil {
		return metadata, nil
	}

	// ERC20 ABI variants: string and bytes32 fallbacks for name/symbol
	const erc20ABIString = `[
		{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"NAME","outputs":[{"name":"","type":"bytes32"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"SYMBOL","outputs":[{"name":"","type":"bytes32"}],"type":"function"}
	]`

	erc20ABI, err := abi.JSON(strings.NewReader(erc20ABIString))
	if err != nil {
		return metadata, fmt.Errorf("failed to parse ERC20 ABI: %w", err)
	}

	contract := bind.NewBoundContract(tokenAddress, erc20ABI, m.rpcClient, m.rpcClient, m.rpcClient)

	// name (string) with fallback NAME() bytes32
	var results []interface{}
	results = make([]interface{}, 1)
	results[0] = new(string)
	if err := contract.Call(nil, &results, "name"); err == nil {
		if name, ok := results[0].(*string); ok && name != nil && *name != "" { metadata.Name = *name }
	}
	if metadata.Name == "Unknown" || metadata.Name == "" {
		results = make([]interface{}, 1)
		results[0] = new([32]byte)
		if err := contract.Call(nil, &results, "NAME"); err == nil {
			if b32, ok := results[0].(*[32]byte); ok && b32 != nil {
				metadata.Name = strings.TrimRight(string(b32[:]), "\x00")
			}
		}
	}

	// symbol (string) with fallback SYMBOL() bytes32
	results = make([]interface{}, 1)
	results[0] = new(string)
	if err := contract.Call(nil, &results, "symbol"); err == nil {
		if sym, ok := results[0].(*string); ok && sym != nil && *sym != "" { metadata.Symbol = *sym }
	}
	if metadata.Symbol == "???" || metadata.Symbol == "" {
		results = make([]interface{}, 1)
		results[0] = new([32]byte)
		if err := contract.Call(nil, &results, "SYMBOL"); err == nil {
			if b32, ok := results[0].(*[32]byte); ok && b32 != nil {
				metadata.Symbol = strings.TrimRight(string(b32[:]), "\x00")
			}
		}
	}

	// decimals
	results = make([]interface{}, 1)
	results[0] = new(uint8)
	if err := contract.Call(nil, &results, "decimals"); err == nil {
		if dec, ok := results[0].(*uint8); ok && dec != nil { metadata.Decimals = int(*dec) }
	}

	// totalSupply
	results = make([]interface{}, 1)
	results[0] = new(*big.Int)
	if err := contract.Call(nil, &results, "totalSupply"); err == nil {
		if ts, ok := results[0].(**big.Int); ok && ts != nil && *ts != nil { metadata.TotalSupply = *ts }
	}

	return metadata, nil
}

// fetchPoolBalances reads token balances held by the pool contract via balanceOf
func (m *UniswapV3Module) fetchPoolBalances(ctx context.Context, pool common.Address, token0, token1 common.Address) (*big.Int, *big.Int, error) {
	if m.rpcClient == nil {
		return nil, nil, fmt.Errorf("no RPC client for balance fetch")
	}
	const abiStr = `[{"constant":true,"inputs":[{"name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"}]`
	erc20ABI, err := abi.JSON(strings.NewReader(abiStr))
	if err != nil { return nil, nil, fmt.Errorf("parse balanceOf ABI: %w", err) }
	c0 := bind.NewBoundContract(token0, erc20ABI, m.rpcClient, m.rpcClient, m.rpcClient)
	c1 := bind.NewBoundContract(token1, erc20ABI, m.rpcClient, m.rpcClient, m.rpcClient)
	// call balanceOf(pool)
	var out0, out1 []interface{}
	out0 = make([]interface{}, 1)
	out0[0] = new(*big.Int)
	if err := c0.Call(nil, &out0, "balanceOf", pool); err != nil { return nil, nil, err }
	b0, _ := out0[0].(**big.Int)
	if b0 == nil || *b0 == nil { bz := big.NewInt(0); b0 = &bz }
	out1 = make([]interface{}, 1)
	out1[0] = new(*big.Int)
	if err := c1.Call(nil, &out1, "balanceOf", pool); err != nil { return nil, nil, err }
	b1, _ := out1[0].(**big.Int)
	if b1 == nil || *b1 == nil { bz := big.NewInt(0); b1 = &bz }
	return *b0, *b1, nil
}

// refreshReservesIfEmpty seeds reserves from on-chain balances if both are zero/NULL
func (m *UniswapV3Module) refreshReservesIfEmpty(ctx context.Context, pool common.Address) {
	var r0, r1 sql.NullString
	var t0, t1 string
	addr := strings.ToLower(pool.Hex())
	if err := m.db.Pool().QueryRow(ctx, `SELECT reserve0, reserve1, token0, token1 FROM uniswap_v3_pools WHERE address = $1`, addr).Scan(&r0, &r1, &t0, &t1); err != nil {
		return
	}
	isZero := (!r0.Valid || r0.String == "0") && (!r1.Valid || r1.String == "0")
	if !isZero || m.rpcClient == nil { return }
	b0, b1, err := m.fetchPoolBalances(ctx, pool, common.HexToAddress(t0), common.HexToAddress(t1))
	if err != nil { return }
	_, _ = m.db.Pool().Exec(ctx, `UPDATE uniswap_v3_pools SET reserve0 = $2::numeric, reserve1 = $3::numeric, updated_at = CURRENT_TIMESTAMP WHERE address = $1`, addr, b0.String(), b1.String())
}

// HandleEvent processes a single event log
func (m *UniswapV3Module) HandleEvent(ctx context.Context, log *types.Log) error {
	if len(log.Topics) == 0 { return nil }
		eventSignature := log.Topics[0]
		handler, exists := m.handlers[eventSignature]
		if !exists {
			return nil
		}

	// Parse the event
	parsedEvent, err := m.parser.ParseEvent(log)
	if err != nil {
		// If we know the topic and have a handler, fallback to a minimal parsed event
		// to let the handler decode or store a basic record.
		if _, exists := m.handlers[eventSignature]; exists {
			// Try to set a reasonable name for known alt topics
			evtName := "Unknown"
			if eventSignature.Hex() == "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83" {
				evtName = "Swap"
			}
			parsedEvent = &core.ParsedEvent{
				Log:              log,
				EventName:        evtName,
				Address:          log.Address,
				Args:             map[string]interface{}{},
				TransactionHash:  log.TxHash,
				TransactionIndex: log.TxIndex,
				BlockNumber:      log.BlockNumber,
				BlockHash:        log.BlockHash,
				LogIndex:         log.Index,
				Timestamp:        big.NewInt(0),
			}
			m.logger.Debug().Str("event_sig", eventSignature.Hex()).Str("address", log.Address.Hex()).Msg("V3: fallback to minimal parsed event")
		} else {
			m.logger.Error().Err(err).Str("event_sig", eventSignature.Hex()).Str("address", log.Address.Hex()).Msg("V3: failed to parse event")
			return nil
		}
	}
	blockTimestamp := m.getBlockTimestamp(ctx, log.BlockNumber)
	parsedEvent.Timestamp = big.NewInt(blockTimestamp)

	if err := handler(ctx, m, parsedEvent); err != nil {
		m.logger.Error().Err(err).Str("event", parsedEvent.EventName).Str("address", parsedEvent.Address.Hex()).Msg("V3 handler failed")
		m.updateModuleState(ctx, log.BlockNumber, "active")
		return nil
	}
	m.updateModuleState(ctx, log.BlockNumber, "active")
	return nil
}

// getBlockTimestamp fetches block timestamp (seconds); falls back to RPC when DB not yet visible
func (m *UniswapV3Module) getBlockTimestamp(ctx context.Context, blockNumber uint64) int64 {
	// Try DB first
	var ts int64
	_ = m.db.Pool().QueryRow(ctx, `SELECT timestamp FROM blocks WHERE number = $1`, blockNumber).Scan(&ts)
	if ts > 0 {
		return ts
	}
	// Fallback: RPC header (helps during in-tx processing when DB row not visible yet)
	if m.rpcClient != nil {
		hdr, err := m.rpcClient.HeaderByNumber(ctx, new(big.Int).SetUint64(blockNumber))
		if err == nil && hdr != nil {
			return int64(hdr.Time)
		}
	}
	return 0
}

// GetEventFilters returns the event filters this module is interested in
func (m *UniswapV3Module) GetEventFilters() []core.EventFilter {
	var filters []core.EventFilter

	// Factory: PoolCreated
	if m.factoryABI != nil {
		if ev, ok := m.factoryABI.Events["PoolCreated"]; ok {
			zero := common.Address{}
			if m.factoryAddress != zero {
				filters = append(filters, core.EventFilter{ Address: m.factoryAddress.Hex(), Topic0: ev.ID.Hex() })
			} else {
				// Unknown factory address – filter by topic only
				filters = append(filters, core.EventFilter{ Topic0: ev.ID.Hex() })
			}
		}
	}

	// Pool events: Swap, Initialize, Mint, Burn, Collect (topic-only filters)
	if m.poolABI != nil {
		for _, name := range []string{"Swap", "Initialize", "Mint", "Burn", "Collect"} {
			if ev, ok := m.poolABI.Events[name]; ok {
				filters = append(filters, core.EventFilter{ Topic0: ev.ID.Hex() })
			}
		}
		// Include known Zilliqa Swap topic variant to ensure routing
		filters = append(filters, core.EventFilter{ Topic0: "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83" })
	}
	return filters
}

// GetStartBlock returns the block number to start indexing from
func (m *UniswapV3Module) GetStartBlock() uint64 {
	if len(m.manifest.DataSources) > 0 && m.manifest.DataSources[0].Source.StartBlock != nil {
		return *m.manifest.DataSources[0].Source.StartBlock
	}
	return 0
}

// Backfill processes historical events from the event_logs table
func (m *UniswapV3Module) Backfill(ctx context.Context, fromBlock, toBlock uint64) error {
	m.logger.Info().Uint64("from", fromBlock).Uint64("to", toBlock).Msg("Starting UniswapV3 backfill")

	// Build topic list from ABI
	var topics []string
	if m.factoryABI != nil {
		if ev, ok := m.factoryABI.Events["PoolCreated"]; ok { topics = append(topics, fmt.Sprintf("\"%s\"", strings.ToLower(ev.ID.Hex()))) }
	}
	if m.poolABI != nil {
		for _, name := range []string{"Swap", "Initialize", "Mint", "Burn", "Collect"} {
			if ev, ok := m.poolABI.Events[name]; ok { topics = append(topics, fmt.Sprintf("\"%s\"", strings.ToLower(ev.ID.Hex()))) }
		}
	}
	if len(topics) == 0 { return nil }

	query := fmt.Sprintf(`
		SELECT block_number, block_hash, transaction_hash, transaction_index, 
		       log_index, address, topics, data, removed
		FROM event_logs 
		WHERE block_number >= $1 AND block_number <= $2
		  AND (
		    address = $3 OR  -- Factory address
		    topics->0 IN (%s)  -- Event signatures we handle
		  )
		ORDER BY block_number, transaction_index, log_index`, strings.Join(topics, ", "))

	rows, err := m.db.Pool().Query(ctx, query, fromBlock, toBlock, strings.ToLower(m.factoryAddress.Hex()))
	if err != nil { return fmt.Errorf("failed to query v3 events for backfill: %w", err) }
	defer rows.Close()

	processed := 0
	for rows.Next() {
		var logData LogData
		if err := rows.Scan(
			&logData.BlockNumber,
			&logData.BlockHash,
			&logData.TransactionHash,
			&logData.TransactionIndex,
			&logData.LogIndex,
			&logData.Address,
			&logData.Topics,
			&logData.Data,
			&logData.Removed,
		); err != nil { return fmt.Errorf("failed to scan v3 log data: %w", err) }

		ethLog, err := logData.ToEthereumLog()
		if err != nil { m.logger.Warn().Err(err).Msg("V3: failed to convert log data, skipping"); continue }
		if err := m.HandleEvent(ctx, ethLog); err != nil { m.logger.Error().Err(err).Uint64("block", ethLog.BlockNumber).Str("tx", ethLog.TxHash.Hex()).Msg("V3 backfill process error") } else { processed++ }
	}
	if err := rows.Err(); err != nil { return fmt.Errorf("v3 backfill iteration error: %w", err) }

	m.logger.Info().Uint64("from", fromBlock).Uint64("to", toBlock).Int("processed", processed).Msg("Completed UniswapV3 backfill")
	return nil
}

// GetSyncState returns the last processed block for this module
func (m *UniswapV3Module) GetSyncState(ctx context.Context) (uint64, error) {
	var lastBlock uint64
	query := `SELECT last_processed_block FROM module_state WHERE module_name = $1`
	if err := m.db.Pool().QueryRow(ctx, query, m.Name()).Scan(&lastBlock); err != nil { return 0, fmt.Errorf("failed to get v3 sync state: %w", err) }
	return lastBlock, nil
}

// UpdateSyncState updates the last processed block for this module
func (m *UniswapV3Module) UpdateSyncState(ctx context.Context, blockNumber uint64) error {
	query := `UPDATE module_state SET last_processed_block = $2, updated_at = CURRENT_TIMESTAMP WHERE module_name = $1`
	_, err := m.db.Pool().Exec(ctx, query, m.Name(), blockNumber)
	return err
}

// updateModuleState updates the module's state in the database
func (m *UniswapV3Module) updateModuleState(ctx context.Context, blockNumber uint64, status string) {
	query := `UPDATE module_state SET last_processed_block = GREATEST(last_processed_block, $2), status = $3, updated_at = CURRENT_TIMESTAMP WHERE module_name = $1`
	if _, err := m.db.Pool().Exec(ctx, query, m.Name(), blockNumber, status); err != nil {
		m.logger.Error().Err(err).Uint64("block", blockNumber).Str("status", status).Msg("V3: failed to update module state")
	}
}

// LogData represents a log entry from the database (same as v2)
type LogData struct {
	BlockNumber      uint64 `db:"block_number"`
	BlockHash        string `db:"block_hash"`
	TransactionHash  string `db:"transaction_hash"`
	TransactionIndex uint   `db:"transaction_index"`
	LogIndex         uint   `db:"log_index"`
	Address          string `db:"address"`
	Topics           []byte `db:"topics"` // JSON
	Data             string `db:"data"`
	Removed          bool   `db:"removed"`
}

// ToEthereumLog converts LogData to types.Log
func (ld *LogData) ToEthereumLog() (*types.Log, error) {
	// Parse topics JSON
	var topicStrings []string
	if err := yaml.Unmarshal(ld.Topics, &topicStrings); err != nil { return nil, fmt.Errorf("failed to parse topics: %w", err) }
	topics := make([]common.Hash, len(topicStrings))
	for i, t := range topicStrings { topics[i] = common.HexToHash(t) }
	return &types.Log{
		Address:     common.HexToAddress(ld.Address),
		Topics:      topics,
		Data:        common.Hex2Bytes(ld.Data),
		BlockNumber: ld.BlockNumber,
		TxHash:      common.HexToHash(ld.TransactionHash),
		TxIndex:     ld.TransactionIndex,
		BlockHash:   common.HexToHash(ld.BlockHash),
		Index:       ld.LogIndex,
		Removed:     ld.Removed,
	}, nil
}
