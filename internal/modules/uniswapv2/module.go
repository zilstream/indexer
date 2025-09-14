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
"github.com/jackc/pgx/v5"
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
	priceRouter   prices.TokenRouter
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
// Normalize address casing from config to avoid user formatting requirements
if config.FactoryAddress != "" { config.FactoryAddress = strings.ToLower(config.FactoryAddress) }
if config.WethAddress != "" { config.WethAddress = strings.ToLower(config.WethAddress) }
if len(config.Stablecoins) > 0 {
	for i := range config.Stablecoins {
		config.Stablecoins[i].Address = strings.ToLower(config.Stablecoins[i].Address)
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

// Build price router if provider is available
if m.priceProvider != nil {
  // collect stablecoin addresses from config
    stables := make([]string, 0, len(m.config.Stablecoins))
     for _, s := range m.config.Stablecoins {
         stables = append(stables, strings.ToLower(s.Address))
     }
     m.logger.Info().
         Strs("stablecoins", stables).
         Str("weth", m.wethAddress.Hex()).
         Int("num_stablecoins", len(stables)).
         Msg("Initializing price router with stablecoins")
     m.priceRouter = prices.NewDBRouter(db.Pool(), m.priceProvider, m.wethAddress.Hex(), stables)
 } else {
     m.logger.Warn().Msg("No price provider available, USD calculations will be limited")
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

// helper: fetch block timestamp (seconds); falls back to RPC when DB not yet visible
func (m *UniswapV2Module) getBlockTimestamp(ctx context.Context, blockNumber uint64) int64 {
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

// tokenDecimals fetches token decimals from DB, fallback 18
func (m *UniswapV2Module) tokenDecimals(ctx context.Context, addr string) int {
 var d int
 if err := m.db.Pool().QueryRow(ctx, `SELECT COALESCE(decimals, 18) FROM tokens WHERE address = $1`, strings.ToLower(addr)).Scan(&d); err != nil {
     return 18
 }
 return d
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

// updatePairStatistics updates pair reserves, volumes, and transaction counts from processed events
func (m *UniswapV2Module) updatePairStatistics(ctx context.Context, tx pgx.Tx, swapEvents, syncEvents []*types.Log) error {
	// Update reserves from the latest sync events per pair
	if len(syncEvents) > 0 {
		// Group syncs by pair and find the latest one for each
		latestSyncByPair := make(map[string]*types.Log)
		for _, event := range syncEvents {
			pairAddr := strings.ToLower(event.Address.Hex())
			if existing, ok := latestSyncByPair[pairAddr]; !ok || event.BlockNumber > existing.BlockNumber ||
				(event.BlockNumber == existing.BlockNumber && event.Index > existing.Index) {
				latestSyncByPair[pairAddr] = event
			}
		}

		// Update reserves for each pair with its latest sync
		for pairAddr, event := range latestSyncByPair {
			if len(event.Data) < 64 {
				continue
			}
			reserve0 := new(big.Int).SetBytes(event.Data[0:32])
			reserve1 := new(big.Int).SetBytes(event.Data[32:64])

			_, err := tx.Exec(ctx, `
				UPDATE uniswap_v2_pairs
				SET reserve0 = $2::numeric, reserve1 = $3::numeric, updated_at = NOW()
				WHERE address = $1`,
				pairAddr, reserve0.String(), reserve1.String())

			if err != nil {
				m.logger.Error().Err(err).Str("pair", pairAddr).Msg("Failed to update reserves")
			} else {
				m.logger.Debug().
					Str("pair", pairAddr).
					Str("reserve0", reserve0.String()).
					Str("reserve1", reserve1.String()).
					Msg("Updated pair reserves from batch")
			}

			// Update reserve_usd if we have pricing
			if m.priceRouter != nil {
				// This would require fetching token addresses and calculating USD values
				// For now, we'll rely on a separate process to update USD values
			}
		}
	}

	// Update volumes and transaction counts from swaps
	if len(swapEvents) > 0 {
		// Group swaps by pair to update statistics
		swapsByPair := make(map[string][]*types.Log)
		for _, event := range swapEvents {
			pairAddr := strings.ToLower(event.Address.Hex())
			swapsByPair[pairAddr] = append(swapsByPair[pairAddr], event)
		}

		for pairAddr, pairSwaps := range swapsByPair {
			var totalVolume0, totalVolume1 big.Int

			for _, event := range pairSwaps {
				if len(event.Data) < 128 {
					continue
				}
				amount0In := new(big.Int).SetBytes(event.Data[0:32])
				amount1In := new(big.Int).SetBytes(event.Data[32:64])
				amount0Out := new(big.Int).SetBytes(event.Data[64:96])
				amount1Out := new(big.Int).SetBytes(event.Data[96:128])

				// Volume is the absolute difference between in and out
				vol0 := new(big.Int).Sub(amount0Out, amount0In)
				vol0.Abs(vol0)
				totalVolume0.Add(&totalVolume0, vol0)

				vol1 := new(big.Int).Sub(amount1Out, amount1In)
				vol1.Abs(vol1)
				totalVolume1.Add(&totalVolume1, vol1)
			}

			// Update volumes and transaction count
			_, err := tx.Exec(ctx, `
				UPDATE uniswap_v2_pairs
				SET
					volume_token0 = COALESCE(volume_token0, 0) + $2::numeric,
					volume_token1 = COALESCE(volume_token1, 0) + $3::numeric,
					txn_count = COALESCE(txn_count, 0) + $4,
					updated_at = NOW()
				WHERE address = $1`,
				pairAddr, totalVolume0.String(), totalVolume1.String(), len(pairSwaps))

			if err != nil {
				m.logger.Error().Err(err).Str("pair", pairAddr).Msg("Failed to update volumes")
			} else {
				m.logger.Debug().
					Str("pair", pairAddr).
					Str("volume0_added", totalVolume0.String()).
					Str("volume1_added", totalVolume1.String()).
					Int("swaps", len(pairSwaps)).
					Msg("Updated pair volumes from batch")
			}
		}

		// Update volume_usd from the sum of swap amounts_usd
		_, err := tx.Exec(ctx, `
			UPDATE uniswap_v2_pairs p
			SET volume_usd = (
				SELECT COALESCE(SUM(s.amount_usd), 0)
				FROM uniswap_v2_swaps s
				WHERE s.pair = p.address
			)
			WHERE p.address IN (SELECT DISTINCT pair FROM uniswap_v2_swaps)`)

		if err != nil {
			m.logger.Error().Err(err).Msg("Failed to update volume_usd")
		}
	}

	// Update reserve_usd based on current prices
	if m.priceRouter != nil {
		// Update reserve_usd based on prices
		_, err := tx.Exec(ctx, `
			UPDATE uniswap_v2_pairs p
			SET reserve_usd = COALESCE(
				CASE
					WHEN t0.price_usd IS NOT NULL AND t1.price_usd IS NOT NULL THEN
						((p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18))) * t0.price_usd) +
						((p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18))) * t1.price_usd)
					WHEN t0.price_usd IS NOT NULL THEN
						2 * ((p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18))) * t0.price_usd)
					WHEN t1.price_usd IS NOT NULL THEN
						2 * ((p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18))) * t1.price_usd)
					ELSE reserve_usd
				END, reserve_usd)
			FROM tokens t0, tokens t1
			WHERE t0.address = p.token0 AND t1.address = p.token1
				AND (p.reserve0 > 0 OR p.reserve1 > 0)`)

		if err != nil {
			m.logger.Error().Err(err).Msg("Failed to update reserve_usd")
		}
	}

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

// HandleEventBatch processes multiple events in a single database transaction for performance
func (m *UniswapV2Module) HandleEventBatch(ctx context.Context, events []*types.Log) error {
	if len(events) == 0 {
		return nil
	}

	start := time.Now()

	// Deduplicate events by transaction hash + log index
	seen := make(map[string]bool)
	uniqueEvents := make([]*types.Log, 0, len(events))
	duplicateCount := 0

	for _, event := range events {
		key := fmt.Sprintf("%s-%d", event.TxHash.Hex(), event.Index)
		if seen[key] {
			duplicateCount++
			continue
		}
		seen[key] = true
		uniqueEvents = append(uniqueEvents, event)
	}

	if duplicateCount > 0 {
		m.logger.Warn().
			Int("duplicates_removed", duplicateCount).
			Int("unique_events", len(uniqueEvents)).
			Msg("Removed duplicate events from batch")
	}

	// Group events by type for batch processing
	var pairCreatedEvents, swapEvents, syncEvents, mintEvents, burnEvents, transferEvents []*types.Log

	for _, event := range uniqueEvents {
		if len(event.Topics) == 0 {
			continue
		}

		eventSignature := event.Topics[0]
		if _, exists := m.handlers[eventSignature]; !exists {
			continue // This event is not handled by this module
		}

		// Group by event type for different batch operations
		switch eventSignature.Hex() {
		case "0x0d3648bd0f6ba80134a33ba9275ac585d9d315f0ad8355cddefde31afa28d0e9": // PairCreated
			pairCreatedEvents = append(pairCreatedEvents, event)
		case "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": // Swap
			swapEvents = append(swapEvents, event)
		case "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1": // Sync
			syncEvents = append(syncEvents, event)
		case "0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f": // Mint
			mintEvents = append(mintEvents, event)
		case "0xdccd412f0b1252819cb1fd330b93224ca42612892bb3f4f789976e6d81936496": // Burn
			burnEvents = append(burnEvents, event)
		case "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef": // Transfer
			transferEvents = append(transferEvents, event)
		}
	}

	// Use a single database transaction for all operations
	tx, err := m.db.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to begin batch transaction: %w", err)
	}
	defer tx.Rollback(ctx)

	// Process each event type with bulk operations - ORDER MATTERS!
	// 1. PairCreated events first (creates pairs)
	if len(pairCreatedEvents) > 0 {
		if err := m.batchHandlePairCreated(ctx, tx, pairCreatedEvents); err != nil {
			return fmt.Errorf("batch PairCreated failed: %w", err)
		}
	}

	// 2. Transfer events (creates pending mints/burns)
	if len(transferEvents) > 0 {
		if err := m.batchHandleTransfers(ctx, tx, transferEvents); err != nil {
			return fmt.Errorf("batch Transfers failed: %w", err)
		}
	}

	if len(swapEvents) > 0 {
		if err := m.batchHandleSwaps(ctx, tx, swapEvents); err != nil {
			return fmt.Errorf("batch Swaps failed: %w", err)
		}
	}

	if len(syncEvents) > 0 {
		if err := m.batchHandleSyncs(ctx, tx, syncEvents); err != nil {
			return fmt.Errorf("batch Syncs failed: %w", err)
		}
	}

	if len(mintEvents) > 0 {
		if err := m.batchHandleMints(ctx, tx, mintEvents); err != nil {
			return fmt.Errorf("batch Mints failed: %w", err)
		}
	}

	if len(burnEvents) > 0 {
		if err := m.batchHandleBurns(ctx, tx, burnEvents); err != nil {
			return fmt.Errorf("batch Burns failed: %w", err)
		}
	}

	// Update pair aggregate statistics based on the events we just processed
	// This updates reserves from syncs, volumes from swaps, and transaction counts
	if len(swapEvents) > 0 || len(syncEvents) > 0 {
		if err := m.updatePairStatistics(ctx, tx, swapEvents, syncEvents); err != nil {
			m.logger.Error().Err(err).Msg("Failed to update pair statistics")
			// Don't fail the whole batch for statistics updates
		}
	}

	// Commit the transaction
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	m.logger.Info().
		Int("total_events", len(uniqueEvents)).
		Int("pairs_created", len(pairCreatedEvents)).
		Int("transfers", len(transferEvents)).
		Int("swaps", len(swapEvents)).
		Int("syncs", len(syncEvents)).
		Int("mints", len(mintEvents)).
		Int("burns", len(burnEvents)).
		Dur("duration", time.Since(start)).
		Msg("Batch processed UniswapV2 events")

	return nil
}

// batchHandleSwaps processes multiple Swap events using bulk insert
func (m *UniswapV2Module) batchHandleSwaps(ctx context.Context, tx pgx.Tx, events []*types.Log) error {
	if len(events) == 0 {
		return nil
	}

	// Deduplicate swaps based on tx hash + swap values (Zilliqa duplicate event issue)
	type swapKey struct {
		txHash      string
		pair        string
		sender      string
		recipient   string
		amount0In   string
		amount1In   string
		amount0Out  string
		amount1Out  string
	}

	seenSwaps := make(map[swapKey]bool)
	duplicatesRemoved := 0

	// Prepare arrays for bulk insert using UNNEST
	ids := make([]string, 0, len(events))
	txHashes := make([]string, 0, len(events))
	logIndexes := make([]uint, 0, len(events))
	blockNumbers := make([]uint64, 0, len(events))
	blockHashes := make([]string, 0, len(events))
	timestamps := make([]int64, 0, len(events))
	pairs := make([]string, 0, len(events))
	senders := make([]string, 0, len(events))
	recipients := make([]string, 0, len(events))
	amount0Ins := make([]string, 0, len(events))
	amount1Ins := make([]string, 0, len(events))
	amount0Outs := make([]string, 0, len(events))
	amount1Outs := make([]string, 0, len(events))
	amountUSDs := make([]*string, 0, len(events)) // Nullable field

	for _, event := range events {
		// Parse the event data (simplified - reuse logic from individual handler)
		swapID := fmt.Sprintf("%s-%d", event.TxHash.Hex(), event.Index)
		pairAddr := strings.ToLower(event.Address.Hex())

		// Extract swap data from topics and data
		if len(event.Topics) < 3 || len(event.Data) < 128 {
			m.logger.Warn().Str("tx_hash", event.TxHash.Hex()).Msg("Invalid Swap event data")
			continue
		}

		sender := common.BytesToAddress(event.Topics[1].Bytes())
		to := common.BytesToAddress(event.Topics[2].Bytes())

		// Parse amounts from data
		amount0In := new(big.Int).SetBytes(event.Data[0:32])
		amount1In := new(big.Int).SetBytes(event.Data[32:64])
		amount0Out := new(big.Int).SetBytes(event.Data[64:96])
		amount1Out := new(big.Int).SetBytes(event.Data[96:128])

		// Create deduplication key based on transaction + swap values
		key := swapKey{
			txHash:      event.TxHash.Hex(),
			pair:        pairAddr,
			sender:      strings.ToLower(sender.Hex()),
			recipient:   strings.ToLower(to.Hex()),
			amount0In:   amount0In.String(),
			amount1In:   amount1In.String(),
			amount0Out:  amount0Out.String(),
			amount1Out:  amount1Out.String(),
		}

		// Skip if we've already seen this exact swap in the same transaction
		if seenSwaps[key] {
			duplicatesRemoved++
			m.logger.Debug().
				Str("tx_hash", event.TxHash.Hex()).
				Uint("log_index", event.Index).
				Msg("Skipping duplicate swap event (Zilliqa duplicate event issue)")
			continue
		}
		seenSwaps[key] = true

		// Calculate USD value if price router is available
		var amountUSD *string
		blockTimestamp := m.getBlockTimestamp(ctx, event.BlockNumber)
		if m.priceRouter != nil && blockTimestamp > 0 {
			// Get token addresses for the pair
			var token0, token1 string
			err := tx.QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v2_pairs WHERE address = $1`, pairAddr).Scan(&token0, &token1)
			if err == nil {
				// Get token decimals
				var decimals0, decimals1 int
				_ = tx.QueryRow(ctx, `SELECT COALESCE(decimals, 18) FROM tokens WHERE address = $1`, token0).Scan(&decimals0)
				_ = tx.QueryRow(ctx, `SELECT COALESCE(decimals, 18) FROM tokens WHERE address = $1`, token1).Scan(&decimals1)
				if decimals0 == 0 { decimals0 = 18 }
				if decimals1 == 0 { decimals1 = 18 }

				// Try to get USD prices for both tokens
				ts := time.Unix(blockTimestamp, 0)
				price0, ok0 := m.priceRouter.PriceTokenUSD(ctx, token0, ts)
				price1, ok1 := m.priceRouter.PriceTokenUSD(ctx, token1, ts)

				// Calculate total USD value based on the amounts being swapped
				totalUSD := big.NewFloat(0)

				if ok0 && (amount0In.Sign() > 0 || amount0Out.Sign() > 0) {
					// Convert amount to decimal (divide by 10^decimals)
					divisor0 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals0)), nil))
					priceFloat0, _ := new(big.Float).SetString(price0)

					if amount0In.Sign() > 0 {
						amount0InFloat := new(big.Float).SetInt(amount0In)
						amount0InFloat.Quo(amount0InFloat, divisor0)
						amount0InFloat.Mul(amount0InFloat, priceFloat0)
						totalUSD.Add(totalUSD, amount0InFloat)
					}
					if amount0Out.Sign() > 0 {
						amount0OutFloat := new(big.Float).SetInt(amount0Out)
						amount0OutFloat.Quo(amount0OutFloat, divisor0)
						amount0OutFloat.Mul(amount0OutFloat, priceFloat0)
						totalUSD.Add(totalUSD, amount0OutFloat)
					}
				}

				if ok1 && (amount1In.Sign() > 0 || amount1Out.Sign() > 0) {
					divisor1 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals1)), nil))
					priceFloat1, _ := new(big.Float).SetString(price1)

					if amount1In.Sign() > 0 {
						amount1InFloat := new(big.Float).SetInt(amount1In)
						amount1InFloat.Quo(amount1InFloat, divisor1)
						amount1InFloat.Mul(amount1InFloat, priceFloat1)
						totalUSD.Add(totalUSD, amount1InFloat)
					}
					if amount1Out.Sign() > 0 {
						amount1OutFloat := new(big.Float).SetInt(amount1Out)
						amount1OutFloat.Quo(amount1OutFloat, divisor1)
						amount1OutFloat.Mul(amount1OutFloat, priceFloat1)
						totalUSD.Add(totalUSD, amount1OutFloat)
					}
				}

				// Convert to string if we have a value
				if totalUSD.Sign() > 0 {
					usdStr := totalUSD.Text('f', 18)
					amountUSD = &usdStr
				}
			}
		}

		// Add to batch arrays
		ids = append(ids, swapID)
		txHashes = append(txHashes, event.TxHash.Hex())
		logIndexes = append(logIndexes, event.Index)
		blockNumbers = append(blockNumbers, event.BlockNumber)
		blockHashes = append(blockHashes, event.BlockHash.Hex())
		timestamps = append(timestamps, blockTimestamp)
		pairs = append(pairs, pairAddr)
		senders = append(senders, strings.ToLower(sender.Hex()))
		recipients = append(recipients, strings.ToLower(to.Hex()))
		amount0Ins = append(amount0Ins, amount0In.String())
		amount1Ins = append(amount1Ins, amount1In.String())
		amount0Outs = append(amount0Outs, amount0Out.String())
		amount1Outs = append(amount1Outs, amount1Out.String())
		amountUSDs = append(amountUSDs, amountUSD)
	}

	// Bulk insert using UNNEST - much faster than individual INSERTs
	query := `
		INSERT INTO uniswap_v2_swaps (
			id, transaction_hash, log_index, block_number, block_hash, timestamp,
			pair, sender, recipient, amount0_in, amount1_in, amount0_out, amount1_out, amount_usd
		)
		SELECT * FROM UNNEST(
			$1::TEXT[], $2::TEXT[], $3::INTEGER[], $4::BIGINT[], $5::TEXT[], $6::BIGINT[],
			$7::TEXT[], $8::TEXT[], $9::TEXT[], $10::NUMERIC[], $11::NUMERIC[], $12::NUMERIC[], $13::NUMERIC[], $14::NUMERIC[]
		) AS t(id, transaction_hash, log_index, block_number, block_hash, timestamp,
				pair, sender, recipient, amount0_in, amount1_in, amount0_out, amount1_out, amount_usd)
		ON CONFLICT (id) DO UPDATE SET
			amount0_in = EXCLUDED.amount0_in,
			amount1_in = EXCLUDED.amount1_in,
			amount0_out = EXCLUDED.amount0_out,
			amount1_out = EXCLUDED.amount1_out,
			amount_usd = COALESCE(EXCLUDED.amount_usd, uniswap_v2_swaps.amount_usd)`

	if duplicatesRemoved > 0 {
		m.logger.Info().
			Int("duplicates_removed", duplicatesRemoved).
			Int("unique_swaps", len(ids)).
			Msg("Removed duplicate swap events (Zilliqa issue)")
	}

	_, err := tx.Exec(ctx, query,
		ids, txHashes, logIndexes, blockNumbers, blockHashes, timestamps,
		pairs, senders, recipients, amount0Ins, amount1Ins, amount0Outs, amount1Outs, amountUSDs)

	if err != nil {
		return fmt.Errorf("failed to bulk insert swaps: %w", err)
	}

	m.logger.Debug().Int("swap_count", len(ids)).Msg("Bulk inserted swaps")
	return nil
}

// batchHandleSyncs processes multiple Sync events using bulk insert
func (m *UniswapV2Module) batchHandleSyncs(ctx context.Context, tx pgx.Tx, events []*types.Log) error {
	if len(events) == 0 {
		return nil
	}

	// Deduplicate syncs based on tx hash + sync values (Zilliqa duplicate event issue)
	type syncKey struct {
		txHash   string
		pair     string
		reserve0 string
		reserve1 string
	}

	seenSyncs := make(map[syncKey]bool)
	duplicatesRemoved := 0

	// Prepare arrays for bulk insert
	ids := make([]string, 0, len(events))
	txHashes := make([]string, 0, len(events))
	logIndexes := make([]uint, 0, len(events))
	blockNumbers := make([]uint64, 0, len(events))
	timestamps := make([]int64, 0, len(events))
	pairs := make([]string, 0, len(events))
	reserve0s := make([]string, 0, len(events))
	reserve1s := make([]string, 0, len(events))

	for _, event := range events {
		syncID := fmt.Sprintf("%s-%d", event.TxHash.Hex(), event.Index)
		pairAddr := strings.ToLower(event.Address.Hex())

		// Parse reserves from data
		if len(event.Data) < 64 {
			m.logger.Warn().Str("tx_hash", event.TxHash.Hex()).Msg("Invalid Sync event data")
			continue
		}

		reserve0 := new(big.Int).SetBytes(event.Data[0:32])
		reserve1 := new(big.Int).SetBytes(event.Data[32:64])

		// Create deduplication key
		key := syncKey{
			txHash:   event.TxHash.Hex(),
			pair:     pairAddr,
			reserve0: reserve0.String(),
			reserve1: reserve1.String(),
		}

		// Skip if we've already seen this exact sync in the same transaction
		if seenSyncs[key] {
			duplicatesRemoved++
			m.logger.Debug().
				Str("tx_hash", event.TxHash.Hex()).
				Uint("log_index", event.Index).
				Msg("Skipping duplicate sync event (Zilliqa duplicate event issue)")
			continue
		}
		seenSyncs[key] = true

		// Add to batch arrays
		ids = append(ids, syncID)
		txHashes = append(txHashes, event.TxHash.Hex())
		logIndexes = append(logIndexes, event.Index)
		blockNumbers = append(blockNumbers, event.BlockNumber)
		timestamps = append(timestamps, time.Now().Unix()) // TODO: Get actual block timestamp
		pairs = append(pairs, pairAddr)
		reserve0s = append(reserve0s, reserve0.String())
		reserve1s = append(reserve1s, reserve1.String())
	}

	// Bulk insert using UNNEST
	query := `
		INSERT INTO uniswap_v2_syncs (
			id, transaction_hash, log_index, block_number, timestamp,
			pair, reserve0, reserve1
		)
		SELECT * FROM UNNEST(
			$1::TEXT[], $2::TEXT[], $3::INTEGER[], $4::BIGINT[], $5::BIGINT[],
			$6::TEXT[], $7::NUMERIC[], $8::NUMERIC[]
		) AS t(id, transaction_hash, log_index, block_number, timestamp,
				pair, reserve0, reserve1)
		ON CONFLICT (id) DO UPDATE SET
			reserve0 = EXCLUDED.reserve0,
			reserve1 = EXCLUDED.reserve1`

	if duplicatesRemoved > 0 {
		m.logger.Info().
			Int("duplicates_removed", duplicatesRemoved).
			Int("unique_syncs", len(ids)).
			Msg("Removed duplicate sync events (Zilliqa issue)")
	}

	_, err := tx.Exec(ctx, query,
		ids, txHashes, logIndexes, blockNumbers, timestamps,
		pairs, reserve0s, reserve1s)

	if err != nil {
		return fmt.Errorf("failed to bulk insert syncs: %w", err)
	}

	m.logger.Debug().Int("sync_count", len(ids)).Msg("Bulk inserted syncs")
	return nil
}

// Placeholder implementations for other batch handlers
func (m *UniswapV2Module) batchHandlePairCreated(ctx context.Context, tx pgx.Tx, events []*types.Log) error {
	if len(events) == 0 {
		return nil
	}

	// Process each PairCreated event individually since each needs token metadata fetching
	// and database operations, but do it within the same transaction for atomicity
	for _, event := range events {
		// Parse the event using the same logic as handlePairCreated
		parsedEvent, err := m.parser.ParseEvent(event)
		if err != nil {
			m.logger.Error().Err(err).Msg("Failed to parse PairCreated event in batch")
			continue // Skip malformed events
		}

		// Extract event parameters
		token0, ok := parsedEvent.Args["token0"].(common.Address)
		if !ok {
			m.logger.Error().Interface("args", parsedEvent.Args).Msg("Invalid token0 in PairCreated batch")
			continue
		}

		token1, ok := parsedEvent.Args["token1"].(common.Address)
		if !ok {
			m.logger.Error().Interface("args", parsedEvent.Args).Msg("Invalid token1 in PairCreated batch")
			continue
		}

		pair, ok := parsedEvent.Args["pair"].(common.Address)
		if !ok {
			m.logger.Error().Interface("args", parsedEvent.Args).Msg("Invalid pair in PairCreated batch")
			continue
		}

		// Extract pair index
		var pairIndex *big.Int
		if val, ok := parsedEvent.Args["arg3"]; ok {
			switch v := val.(type) {
			case *big.Int:
				pairIndex = v
			case int64:
				pairIndex = big.NewInt(v)
			case float64:
				pairIndex = big.NewInt(int64(v))
			case int:
				pairIndex = big.NewInt(int64(v))
			default:
				m.logger.Warn().Interface("val", val).Msg("Unexpected pair index type, using 0")
				pairIndex = big.NewInt(0)
			}
		} else {
			pairIndex = big.NewInt(0)
		}

		// Check for duplicate pairs
		var exists bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM uniswap_v2_pairs WHERE address = $1)`,
			strings.ToLower(pair.Hex())).Scan(&exists)
		if err != nil {
			return fmt.Errorf("failed to check pair existence: %w", err)
		}
		if exists {
			m.logger.Debug().Str("pair", pair.Hex()).Msg("Pair already exists, skipping")
			continue
		}

		// Fetch and ensure tokens exist (this may take time due to RPC calls)
		if err := m.ensureToken(ctx, token0, parsedEvent.BlockNumber, m.getBlockTimestamp(ctx, parsedEvent.BlockNumber)); err != nil {
			m.logger.Error().Err(err).Str("token", token0.Hex()).Msg("Failed to ensure token0 exists")
			continue
		}
		if err := m.ensureToken(ctx, token1, parsedEvent.BlockNumber, m.getBlockTimestamp(ctx, parsedEvent.BlockNumber)); err != nil {
			m.logger.Error().Err(err).Str("token", token1.Hex()).Msg("Failed to ensure token1 exists")
			continue
		}

		// Insert the pair
		_, err = tx.Exec(ctx, `
			INSERT INTO uniswap_v2_pairs (
				address, factory, token0, token1,
				reserve0, reserve1, total_supply, reserve_usd,
				volume_token0, volume_token1, volume_usd,
				txn_count, created_at_timestamp, created_at_block
			) VALUES ($1, $2, $3, $4, 0, 0, 0, 0, 0, 0, 0, 0, $5, $6)
			ON CONFLICT (address) DO NOTHING`,
			strings.ToLower(pair.Hex()),
			strings.ToLower(m.factoryAddress.Hex()),
			strings.ToLower(token0.Hex()),
			strings.ToLower(token1.Hex()),
			m.getBlockTimestamp(ctx, parsedEvent.BlockNumber),
			parsedEvent.BlockNumber,
		)
		if err != nil {
			return fmt.Errorf("failed to insert pair: %w", err)
		}

		// Update factory pair count
		_, err = tx.Exec(ctx, `
			UPDATE uniswap_v2_factory
			SET pair_count = pair_count + 1, updated_at = NOW()
			WHERE address = $1`,
			strings.ToLower(m.factoryAddress.Hex()))
		if err != nil {
			m.logger.Warn().Err(err).Msg("Failed to update factory pair count")
		}

		m.logger.Info().
			Str("pair", pair.Hex()).
			Str("token0", token0.Hex()).
			Str("token1", token1.Hex()).
			Str("pair_index", pairIndex.String()).
			Msg("Pair created in batch")
	}

	return nil
}

func (m *UniswapV2Module) batchHandleMints(ctx context.Context, tx pgx.Tx, events []*types.Log) error {
	if len(events) == 0 {
		return nil
	}

	// Process each Mint event - they need to update existing "pending" mint rows
	// created by Transfer events (from == 0x0)
	for _, event := range events {
		// Extract sender - some contracts have it indexed, others don't
		var sender common.Address
		if len(event.Topics) >= 2 {
			// Standard: sender is indexed (second topic)
			sender = common.BytesToAddress(event.Topics[1].Bytes())
		} else if len(event.Data) >= 96 {
			// Non-standard: sender is in data field (first 32 bytes)
			// This happens with some non-standard Uniswap V2 implementations
			sender = common.BytesToAddress(event.Data[0:32])
			m.logger.Debug().
				Str("tx_hash", event.TxHash.Hex()).
				Str("sender", sender.Hex()).
				Msg("Extracted sender from data field (non-standard Mint event)")
		} else {
			// Fallback: use zero address
			m.logger.Warn().
				Int("topic_count", len(event.Topics)).
				Int("data_len", len(event.Data)).
				Str("tx_hash", event.TxHash.Hex()).
				Uint64("block", event.BlockNumber).
				Str("address", event.Address.Hex()).
				Msg("Cannot extract sender from Mint event, using zero address")
			sender = common.Address{}
		}

		// Parse amounts from data field
		// Standard: amount0 and amount1 are at positions 0 and 32
		// Non-standard: amount0 and amount1 are at positions 32 and 64 (after sender)
		var amount0, amount1 *big.Int
		if len(event.Topics) >= 2 {
			// Standard format: amounts start at position 0
			if len(event.Data) < 64 {
				m.logger.Error().Msg("Insufficient data for standard Mint event")
				continue
			}
			amount0 = new(big.Int).SetBytes(event.Data[0:32])
			amount1 = new(big.Int).SetBytes(event.Data[32:64])
		} else {
			// Non-standard format: amounts start at position 32 (after sender)
			if len(event.Data) < 96 {
				m.logger.Error().Msg("Insufficient data for non-standard Mint event")
				continue
			}
			amount0 = new(big.Int).SetBytes(event.Data[32:64])
			amount1 = new(big.Int).SetBytes(event.Data[64:96])
		}

		// Check for duplicates first
		var exists int
		_ = tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM uniswap_v2_mints
			WHERE transaction_hash=$1 AND pair=$2 AND sender=$3 AND amount0=$4 AND amount1=$5`,
			event.TxHash.Hex(), strings.ToLower(event.Address.Hex()),
			strings.ToLower(sender.Hex()), amount0.String(), amount1.String()).Scan(&exists)

		if exists > 0 {
			m.logger.Debug().Str("pair", event.Address.Hex()).Msg("Duplicate Mint event skipped in batch")
			continue
		}

		// Calculate USD value if price router is available
		var amountUSD *string
		blockTimestamp := m.getBlockTimestamp(ctx, event.BlockNumber)
		if m.priceRouter != nil && blockTimestamp > 0 {
			// Get token addresses for the pair
			var token0, token1 string
			err := tx.QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v2_pairs WHERE address = $1`,
				strings.ToLower(event.Address.Hex())).Scan(&token0, &token1)
			if err == nil {
				// Get token decimals
				var decimals0, decimals1 int
				_ = tx.QueryRow(ctx, `SELECT COALESCE(decimals, 18) FROM tokens WHERE address = $1`, token0).Scan(&decimals0)
				_ = tx.QueryRow(ctx, `SELECT COALESCE(decimals, 18) FROM tokens WHERE address = $1`, token1).Scan(&decimals1)
				if decimals0 == 0 { decimals0 = 18 }
				if decimals1 == 0 { decimals1 = 18 }

				// Try to get USD prices for both tokens
				ts := time.Unix(blockTimestamp, 0)
				price0, ok0 := m.priceRouter.PriceTokenUSD(ctx, token0, ts)
				price1, ok1 := m.priceRouter.PriceTokenUSD(ctx, token1, ts)

				// Calculate total USD value
				totalUSD := big.NewFloat(0)

				if ok0 && amount0.Sign() > 0 {
					divisor0 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals0)), nil))
					priceFloat0, _ := new(big.Float).SetString(price0)
					amount0Float := new(big.Float).SetInt(amount0)
					amount0Float.Quo(amount0Float, divisor0)
					amount0Float.Mul(amount0Float, priceFloat0)
					totalUSD.Add(totalUSD, amount0Float)
				}

				if ok1 && amount1.Sign() > 0 {
					divisor1 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals1)), nil))
					priceFloat1, _ := new(big.Float).SetString(price1)
					amount1Float := new(big.Float).SetInt(amount1)
					amount1Float.Quo(amount1Float, divisor1)
					amount1Float.Mul(amount1Float, priceFloat1)
					totalUSD.Add(totalUSD, amount1Float)
				}

				// Convert to string if we have a value
				if totalUSD.Sign() > 0 {
					usdStr := totalUSD.Text('f', 18)
					amountUSD = &usdStr
				}
			}
		}

		// Try to complete a pending mint row (created by Transfer event)
		ct, err := tx.Exec(ctx, `
			UPDATE uniswap_v2_mints m
			SET sender = $3, amount0 = $4, amount1 = $5, amount_usd = COALESCE($7::numeric, amount_usd)
			WHERE m.ctid IN (
				SELECT ctid FROM uniswap_v2_mints
				WHERE transaction_hash = $1 AND pair = $2 AND amount0 = '0' AND amount1 = '0' AND to_address <> $6
				ORDER BY log_index ASC
				LIMIT 1
			)`,
			event.TxHash.Hex(), strings.ToLower(event.Address.Hex()),
			strings.ToLower(sender.Hex()), amount0.String(), amount1.String(),
			strings.ToLower(common.Address{}.Hex()), amountUSD)

		if err != nil {
			m.logger.Error().Err(err).Str("pair", event.Address.Hex()).Msg("Mint update failed in batch")
			continue
		}

		if ct.RowsAffected() == 0 {
			m.logger.Debug().Str("pair", event.Address.Hex()).Msg("No pending mint to complete in batch; skipping")
		} else {
			m.logger.Debug().
				Str("pair", event.Address.Hex()).
				Str("sender", sender.Hex()).
				Str("amount0", amount0.String()).
				Str("amount1", amount1.String()).
				Msg("Mint processed in batch")
		}
	}

	return nil
}

func (m *UniswapV2Module) batchHandleTransfers(ctx context.Context, tx pgx.Tx, events []*types.Log) error {
	if len(events) == 0 {
		return nil
	}

	// Process Transfer events - these create pending mints and handle burns
	for _, event := range events {
		// Extract indexed parameters from topics
		var from, to common.Address
		if len(event.Topics) >= 3 {
			from = common.BytesToAddress(event.Topics[1].Bytes())
			to = common.BytesToAddress(event.Topics[2].Bytes())
		} else {
			continue // Skip if not enough topics
		}

		// Parse value from data field
		if len(event.Data) < 32 {
			continue // Skip if not enough data
		}

		value := new(big.Int).SetBytes(event.Data[0:32])
		zero := common.Address{}

		// Only process transfers from known pair contracts
		var isPair bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM uniswap_v2_pairs WHERE address = $1)`,
			strings.ToLower(event.Address.Hex())).Scan(&isPair); err != nil || !isPair {
			continue
		}

		ts := m.getBlockTimestamp(ctx, event.BlockNumber)

		// Mint start: LP tokens minted (from == zero)
		if from == zero {
			// Ignore the initial minimum liquidity lock (to == zero, value == 1000)
			if to == zero && value.Cmp(big.NewInt(1000)) == 0 {
				// Still update total_supply
				_, _ = tx.Exec(ctx, `
					UPDATE uniswap_v2_pairs
					SET total_supply = COALESCE(total_supply,0) + $2::numeric, updated_at = NOW()
					WHERE address = $1`, strings.ToLower(event.Address.Hex()), value.String())
				continue
			}

			// Check for duplicates
			var dup int
			_ = tx.QueryRow(ctx, `
				SELECT COUNT(*) FROM uniswap_v2_mints
				WHERE transaction_hash=$1 AND pair=$2 AND to_address=$3 AND liquidity=$4 AND block_number=$5`,
				event.TxHash.Hex(), strings.ToLower(event.Address.Hex()),
				strings.ToLower(to.Hex()), value.String(), event.BlockNumber).Scan(&dup)

			if dup == 0 {
				_, _ = tx.Exec(ctx, `
					INSERT INTO uniswap_v2_mints (
						id, transaction_hash, log_index, block_number, timestamp,
						pair, to_address, sender, amount0, amount1, liquidity
					) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
					ON CONFLICT (id) DO NOTHING`,
					fmt.Sprintf("%s-%d", event.TxHash.Hex(), event.Index),
					event.TxHash.Hex(), event.Index, event.BlockNumber, ts,
					strings.ToLower(event.Address.Hex()), strings.ToLower(to.Hex()),
					strings.ToLower(zero.Hex()), "0", "0", value.String())

				// Update pair total_supply
				_, _ = tx.Exec(ctx, `
					UPDATE uniswap_v2_pairs
					SET total_supply = COALESCE(total_supply,0) + $2::numeric, updated_at = NOW()
					WHERE address = $1`, strings.ToLower(event.Address.Hex()), value.String())

				m.logger.Debug().
					Str("pair", event.Address.Hex()).
					Str("to", to.Hex()).
					Str("value", value.String()).
					Msg("LP mint transfer recorded in batch")
			}
		}

		// Burn start: LP tokens burned (to == zero)
		if to == zero && from != zero {
			// Check for duplicates
			var dup int
			_ = tx.QueryRow(ctx, `
				SELECT COUNT(*) FROM uniswap_v2_burns
				WHERE transaction_hash=$1 AND pair=$2 AND sender=$3 AND liquidity=$4`,
				event.TxHash.Hex(), strings.ToLower(event.Address.Hex()),
				strings.ToLower(from.Hex()), value.String()).Scan(&dup)

			if dup == 0 {
				_, _ = tx.Exec(ctx, `
					INSERT INTO uniswap_v2_burns (
						id, transaction_hash, log_index, block_number, timestamp,
						pair, sender, to_address, amount0, amount1, liquidity
					) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
					ON CONFLICT (id) DO NOTHING`,
					fmt.Sprintf("%s-%d-burn", event.TxHash.Hex(), event.Index),
					event.TxHash.Hex(), event.Index, event.BlockNumber, ts,
					strings.ToLower(event.Address.Hex()), strings.ToLower(from.Hex()),
					strings.ToLower(zero.Hex()), "0", "0", value.String())

				// Update pair total_supply
				_, _ = tx.Exec(ctx, `
					UPDATE uniswap_v2_pairs
					SET total_supply = GREATEST(0, COALESCE(total_supply,0) - $2::numeric), updated_at = NOW()
					WHERE address = $1`, strings.ToLower(event.Address.Hex()), value.String())

				m.logger.Debug().
					Str("pair", event.Address.Hex()).
					Str("from", from.Hex()).
					Str("value", value.String()).
					Msg("LP burn transfer recorded in batch")
			}
		}
	}

	return nil
}

func (m *UniswapV2Module) batchHandleBurns(ctx context.Context, tx pgx.Tx, events []*types.Log) error {
	if len(events) == 0 {
		return nil
	}

	// Process each Burn event - similar to Mints but for liquidity removal
	for _, event := range events {
		// Extract indexed sender and to from topics
		var sender, to common.Address
		if len(event.Topics) >= 3 {
			sender = common.BytesToAddress(event.Topics[1].Bytes())
			to = common.BytesToAddress(event.Topics[2].Bytes())
		} else {
			m.logger.Error().Msg("Insufficient topics for Burn event")
			continue
		}

		// Parse amounts from data field
		if len(event.Data) < 64 {
			m.logger.Error().Msg("Insufficient data for Burn event")
			continue
		}

		amount0 := new(big.Int).SetBytes(event.Data[0:32])
		amount1 := new(big.Int).SetBytes(event.Data[32:64])

		// Get block timestamp
		ts := m.getBlockTimestamp(ctx, event.BlockNumber)

		// Check for duplicates first
		var exists int
		_ = tx.QueryRow(ctx, `
			SELECT COUNT(*) FROM uniswap_v2_burns
			WHERE transaction_hash=$1 AND pair=$2 AND sender=$3 AND to_address=$4 AND amount0=$5 AND amount1=$6`,
			event.TxHash.Hex(), strings.ToLower(event.Address.Hex()),
			strings.ToLower(sender.Hex()), strings.ToLower(to.Hex()),
			amount0.String(), amount1.String()).Scan(&exists)

		if exists > 0 {
			m.logger.Debug().Str("pair", event.Address.Hex()).Msg("Duplicate Burn event skipped in batch")
			continue
		}

		// Calculate USD value if price router is available
		var amountUSD string = "0"
		if m.priceRouter != nil && ts > 0 {
			// Get token addresses for the pair
			var token0, token1 string
			err := tx.QueryRow(ctx, `SELECT token0, token1 FROM uniswap_v2_pairs WHERE address = $1`,
				strings.ToLower(event.Address.Hex())).Scan(&token0, &token1)
			if err == nil {
				// Get token decimals
				var decimals0, decimals1 int
				_ = tx.QueryRow(ctx, `SELECT COALESCE(decimals, 18) FROM tokens WHERE address = $1`, token0).Scan(&decimals0)
				_ = tx.QueryRow(ctx, `SELECT COALESCE(decimals, 18) FROM tokens WHERE address = $1`, token1).Scan(&decimals1)
				if decimals0 == 0 { decimals0 = 18 }
				if decimals1 == 0 { decimals1 = 18 }

				// Try to get USD prices for both tokens
				tsTime := time.Unix(ts, 0)
				price0, ok0 := m.priceRouter.PriceTokenUSD(ctx, token0, tsTime)
				price1, ok1 := m.priceRouter.PriceTokenUSD(ctx, token1, tsTime)

				// Calculate total USD value
				totalUSD := big.NewFloat(0)

				if ok0 && amount0.Sign() > 0 {
					divisor0 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals0)), nil))
					priceFloat0, _ := new(big.Float).SetString(price0)
					amount0Float := new(big.Float).SetInt(amount0)
					amount0Float.Quo(amount0Float, divisor0)
					amount0Float.Mul(amount0Float, priceFloat0)
					totalUSD.Add(totalUSD, amount0Float)
				}

				if ok1 && amount1.Sign() > 0 {
					divisor1 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals1)), nil))
					priceFloat1, _ := new(big.Float).SetString(price1)
					amount1Float := new(big.Float).SetInt(amount1)
					amount1Float.Quo(amount1Float, divisor1)
					amount1Float.Mul(amount1Float, priceFloat1)
					totalUSD.Add(totalUSD, amount1Float)
				}

				// Convert to string
				if totalUSD.Sign() > 0 {
					amountUSD = totalUSD.Text('f', 18)
				}
			}
		}

		// Insert burn record directly (Burns don't need pending records like Mints)
		_, err := tx.Exec(ctx, `
			INSERT INTO uniswap_v2_burns (
				id, transaction_hash, log_index, block_number, timestamp,
				pair, sender, to_address, amount0, amount1, liquidity, amount_usd
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, '0', $11)
			ON CONFLICT (id) DO NOTHING`,
			fmt.Sprintf("%s-%d", event.TxHash.Hex(), event.Index),
			event.TxHash.Hex(),
			event.Index,
			event.BlockNumber,
			ts,
			strings.ToLower(event.Address.Hex()),
			strings.ToLower(sender.Hex()),
			strings.ToLower(to.Hex()),
			amount0.String(),
			amount1.String(),
			amountUSD,
		)

		if err != nil {
			m.logger.Error().Err(err).Str("pair", event.Address.Hex()).Msg("Burn insert failed in batch")
			continue
		}

		m.logger.Debug().
			Str("pair", event.Address.Hex()).
			Str("sender", sender.Hex()).
			Str("to", to.Hex()).
			Str("amount0", amount0.String()).
			Str("amount1", amount1.String()).
			Msg("Burn processed in batch")
	}

	return nil
}

