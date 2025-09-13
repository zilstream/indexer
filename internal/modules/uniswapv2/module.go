package uniswapv2

import (
"context"
"fmt"
"math/big"
"strings"
	"time"

"github.com/ethereum/go-ethereum/accounts/abi"
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

// UniswapV2Module implements the Module interface for Uniswap V2 indexing
type UniswapV2Module struct {
db       *database.Database
manifest *core.Manifest
logger   zerolog.Logger
	parser   *core.EventParser
rpcClient *ethclient.Client

// Contract addresses and ABIs
factoryAddress common.Address
wethAddress    common.Address
	factoryABI     *abi.ABI
pairABI        *abi.ABI

	// Configuration
config *Config

	// Event handlers
	handlers map[common.Hash]EventHandler

	// Pricing
priceProvider prices.Provider
}

// Config represents the module configuration
type Config struct {
	FactoryAddress  string       `yaml:"factoryAddress"`
	WethAddress     string       `yaml:"wethAddress"`
	RPCEndpoint     string       `yaml:"rpcEndpoint"`
Stablecoins     []Stablecoin `yaml:"stablecoins"`
Settings        Settings     `yaml:"settings"`
}

type Stablecoin struct {
	Address  string `yaml:"address"`
Symbol   string `yaml:"symbol"`
Decimals int    `yaml:"decimals"`
}

type Settings struct {
	MinLiquidityUSD float64 `yaml:"minLiquidityUSD"`
	MinVolumeUSD    float64 `yaml:"minVolumeUSD"`
	MinReservesUSD  float64 `yaml:"minReservesUSD"`
}

// EventHandler function type for handling specific events
type EventHandler func(ctx context.Context, module *UniswapV2Module, event *core.ParsedEvent) error

// TokenMetadata holds token information fetched from the contract
type TokenMetadata struct {
	Name        string
	Symbol      string
	Decimals    int
TotalSupply *big.Int
}

// NewUniswapV2Module creates a new UniswapV2 module
func NewUniswapV2Module(logger zerolog.Logger) (*UniswapV2Module, error) {
// Load manifest
	manifestLoader := loader.NewManifestLoader(logger)
manifest, err := manifestLoader.LoadFromFile("manifests/uniswap-v2.yaml")
if err != nil {
 return nil, fmt.Errorf("failed to load manifest: %w", err)
}

// Parse configuration from manifest context  
var config Config
if manifest.Context != nil {
		contextBytes, _ := yaml.Marshal(manifest.Context)
 if err := yaml.Unmarshal(contextBytes, &config); err != nil {
  return nil, fmt.Errorf("failed to parse module config: %w", err)
 }
}

// Set up addresses - use the addresses from the context if available
factoryAddress := common.HexToAddress("0xf42d1058f233329185A36B04B7f96105afa1adD2") // Default Zilliqa factory
wethAddress := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")     // Default WETH

if config.FactoryAddress != "" {
		factoryAddress = common.HexToAddress(config.FactoryAddress)
}
if config.WethAddress != "" {
wethAddress = common.HexToAddress(config.WethAddress)
}

module := &UniswapV2Module{
manifest:       manifest,
logger:         logger.With().Str("module", "uniswap-v2").Logger(),
 parser:         core.NewEventParser(),
		factoryAddress: factoryAddress,
 wethAddress:    wethAddress,
 config:         &config,
handlers:       make(map[common.Hash]EventHandler),
}

// Initialize ABIs and event handlers
if err := module.initializeABIs(); err != nil {
 return nil, fmt.Errorf("failed to initialize ABIs: %w", err)
	}

	if err := module.registerEventHandlers(); err != nil {
		return nil, fmt.Errorf("failed to register event handlers: %w", err)
	}

return module, nil
}

// Name returns the module name
func (m *UniswapV2Module) Name() string {
return m.manifest.Name
}

// Version returns the module version
func (m *UniswapV2Module) Version() string {
return m.manifest.Version
}

// Manifest returns the module manifest
func (m *UniswapV2Module) Manifest() *core.Manifest {
return m.manifest
}

// SetRPCClient sets the RPC client for the module
func (m *UniswapV2Module) SetRPCClient(client *ethclient.Client) {
m.rpcClient = client
}

// SetPriceProvider injects the price provider
func (m *UniswapV2Module) SetPriceProvider(p prices.Provider) {
m.priceProvider = p
}

// Initialize sets up the module with database connection
func (m *UniswapV2Module) Initialize(ctx context.Context, db *database.Database) error {
m.db = db

// Connect to RPC if we have an endpoint configured
if m.config != nil && m.config.RPCEndpoint != "" {
 client, err := ethclient.Dial(m.config.RPCEndpoint)
 if err != nil {
 m.logger.Warn().Err(err).Msg("Failed to connect to RPC for token metadata fetching")
 } else {
  m.rpcClient = client
  m.logger.Info().Str("endpoint", m.config.RPCEndpoint).Msg("Connected to RPC for token metadata")
}
}

// Ensure factory exists in database
var startBlock uint64 = 0
if len(m.manifest.DataSources) > 0 && m.manifest.DataSources[0].Source.StartBlock != nil {
startBlock = *m.manifest.DataSources[0].Source.StartBlock
}

query := `
INSERT INTO uniswap_v2_factory (address, pair_count, created_at_block, created_at_timestamp, updated_at)
VALUES ($1, 0, $2, extract(epoch from now())::bigint, NOW())
 ON CONFLICT (address) DO NOTHING`
	
	_, err := db.Pool().Exec(ctx, query, strings.ToLower(m.factoryAddress.Hex()), startBlock)
	if err != nil {
		return fmt.Errorf("failed to initialize factory: %w", err)
}

m.logger.Info().
 Str("factory", m.factoryAddress.Hex()).
		Msg("UniswapV2 module initialized")
return nil
}

// HandleEvent processes a single event log
func (m *UniswapV2Module) HandleEvent(ctx context.Context, log *types.Log) error {
// Check if we have a handler for this event type
	if len(log.Topics) == 0 {
 return nil
}

eventSignature := log.Topics[0]
handler, exists := m.handlers[eventSignature]
if !exists {
 // This event is not handled by this module
 return nil
}

// Parse the event
m.logger.Debug().
Str("event_sig", eventSignature.Hex()).
Str("address", log.Address.Hex()).
Uint64("block", log.BlockNumber).
 Msg("Attempting to parse event")
	
parsedEvent, err := m.parser.ParseEvent(log)
if err != nil {
 m.logger.Error().
  Err(err).
  Str("event_sig", eventSignature.Hex()).
  Str("address", log.Address.Hex()).
 Msg("Failed to parse event")
return fmt.Errorf("failed to parse event: %w", err)
}

// Add timestamp to parsed event (from block)
// We'll need to fetch this if not available in the event
parsedEvent.Timestamp = big.NewInt(0) // TODO: Add proper timestamp

// Call the specific handler
m.logger.Debug().
Str("event", parsedEvent.EventName).
Str("address", parsedEvent.Address.Hex()).
Msg("Calling event handler")

	if err := handler(ctx, m, parsedEvent); err != nil {
 m.logger.Error().
  Err(err).
			Str("event", parsedEvent.EventName).
  Str("address", parsedEvent.Address.Hex()).
 Msg("Handler failed")
// Update state but don't return error to prevent getting stuck
m.updateModuleState(ctx, log.BlockNumber, "active")
return nil
	}

	// Update module state with successfully processed block
	m.updateModuleState(ctx, log.BlockNumber, "active")

	m.logger.Debug().
 Str("event", parsedEvent.EventName).
		Str("address", parsedEvent.Address.Hex()).
 Uint64("block", parsedEvent.BlockNumber).
 Msg("Processed event")

return nil
}

// helper: fetch block timestamp (seconds) -> time.Time
func (m *UniswapV2Module) blockTime(ctx context.Context, blockNumber uint64) (time.Time, error) {
var ts int64
row := m.db.Pool().QueryRow(ctx, `SELECT timestamp FROM blocks WHERE number = $1`, blockNumber)
if err := row.Scan(&ts); err != nil { return time.Time{}, err }
return time.Unix(ts, 0).UTC(), nil
}

// GetEventFilters returns the event filters this module is interested in
func (m *UniswapV2Module) GetEventFilters() []core.EventFilter {
var filters []core.EventFilter

// Add factory events
filters = append(filters, core.EventFilter{
Address: m.factoryAddress.Hex(),
Topic0:  "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9", // PairCreated
})

// Add pair events (we'll add specific pairs dynamically)
	// For now, we listen to all swap/mint/burn/sync/transfer events
filters = append(filters,
		core.EventFilter{
			Topic0: "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822", // Swap
		},
		core.EventFilter{
  Topic0: "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1", // Sync
 },
core.EventFilter{
  Topic0: "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f", // Mint
 },
		core.EventFilter{
			Topic0: "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496", // Burn
		},
		core.EventFilter{
  Topic0: "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // Transfer (ERC20)
},
)

	return filters
}

// GetStartBlock returns the block number to start indexing from
func (m *UniswapV2Module) GetStartBlock() uint64 {
// Return the factory deployment block
if len(m.manifest.DataSources) > 0 && m.manifest.DataSources[0].Source.StartBlock != nil {
		return *m.manifest.DataSources[0].Source.StartBlock
	}
	return 0
}

// Backfill processes historical events from the event_logs table
func (m *UniswapV2Module) Backfill(ctx context.Context, fromBlock, toBlock uint64) error {
	m.logger.Info().
		Uint64("from", fromBlock).
		Uint64("to", toBlock).
		Msg("Starting UniswapV2 backfill")

	// Get all relevant events from the event_logs table
	query := `
		SELECT block_number, block_hash, transaction_hash, transaction_index, 
		       log_index, address, topics, data, removed
		FROM event_logs 
		WHERE block_number >= $1 AND block_number <= $2
		  AND (
		    address = $3 OR  -- Factory address
		    topics->0 IN ($4, $5, $6, $7, $8)  -- Event signatures we handle
		  )
		ORDER BY block_number, transaction_index, log_index`

	rows, err := m.db.Pool().Query(ctx, query,
		fromBlock, toBlock,
		strings.ToLower(m.factoryAddress.Hex()),
		"\"0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9\"", // PairCreated
		"\"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822\"", // Swap
		"\"0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1\"", // Sync
		"\"0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f\"", // Mint
		"\"0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496\"", // Burn
	)
	if err != nil {
		return fmt.Errorf("failed to query events for backfill: %w", err)
	}
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
		); err != nil {
			return fmt.Errorf("failed to scan log data: %w", err)
		}

		// Convert to types.Log
		log, err := logData.ToEthereumLog()
		if err != nil {
			m.logger.Warn().Err(err).Msg("Failed to convert log data, skipping")
			continue
		}

		// Process the event
		if err := m.HandleEvent(ctx, log); err != nil {
			m.logger.Error().
				Err(err).
				Uint64("block", log.BlockNumber).
				Str("tx", log.TxHash.Hex()).
				Msg("Failed to process event during backfill")
			// Continue processing other events
		} else {
			processed++
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating over backfill results: %w", err)
	}

	m.logger.Info().
		Uint64("from", fromBlock).
		Uint64("to", toBlock).
		Int("processed", processed).
		Msg("Completed UniswapV2 backfill")

	return nil
}

// GetSyncState returns the last processed block for this module
func (m *UniswapV2Module) GetSyncState(ctx context.Context) (uint64, error) {
	var lastBlock uint64
	query := `SELECT last_processed_block FROM module_state WHERE module_name = $1`
	
	err := m.db.Pool().QueryRow(ctx, query, m.Name()).Scan(&lastBlock)
	if err != nil {
		return 0, fmt.Errorf("failed to get sync state: %w", err)
	}

	return lastBlock, nil
}

// UpdateSyncState updates the last processed block for this module
func (m *UniswapV2Module) UpdateSyncState(ctx context.Context, blockNumber uint64) error {
	query := `
		UPDATE module_state 
		SET last_processed_block = $2, updated_at = CURRENT_TIMESTAMP 
		WHERE module_name = $1`

	_, err := m.db.Pool().Exec(ctx, query, m.Name(), blockNumber)
	if err != nil {
		return fmt.Errorf("failed to update sync state: %w", err)
	}

	return nil
}

// updateModuleState updates the module's state in the database
func (m *UniswapV2Module) updateModuleState(ctx context.Context, blockNumber uint64, status string) {
	query := `
		UPDATE module_state 
		SET last_processed_block = GREATEST(last_processed_block, $2), 
		    status = $3,
		    updated_at = CURRENT_TIMESTAMP 
		WHERE module_name = $1`

	_, err := m.db.Pool().Exec(ctx, query, m.Name(), blockNumber, status)
	if err != nil {
		m.logger.Error().
			Err(err).
			Uint64("block", blockNumber).
			Str("status", status).
			Msg("Failed to update module state")
	}
}

// LogData represents a log entry from the database
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
	if err := yaml.Unmarshal(ld.Topics, &topicStrings); err != nil {
		return nil, fmt.Errorf("failed to parse topics: %w", err)
	}

	// Convert to common.Hash slice
	topics := make([]common.Hash, len(topicStrings))
	for i, topic := range topicStrings {
		topics[i] = common.HexToHash(topic)
	}

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