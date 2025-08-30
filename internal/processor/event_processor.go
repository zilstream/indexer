package processor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
)

// EventProcessor handles processing of event logs
type EventProcessor struct {
	rpcClient *rpc.Client
	db        *database.Database
	logger    zerolog.Logger
}

// NewEventProcessor creates a new event processor
func NewEventProcessor(rpcClient *rpc.Client, db *database.Database, logger zerolog.Logger) *EventProcessor {
	return &EventProcessor{
		rpcClient: rpcClient,
		db:        db,
		logger:    logger.With().Str("component", "event_processor").Logger(),
	}
}

// ProcessBlockLogs processes all event logs for a given block
func (p *EventProcessor) ProcessBlockLogs(ctx context.Context, block *types.Block, receipts []*types.Receipt) error {
	p.logger.Debug().
		Uint64("block", block.NumberU64()).
		Int("receipts", len(receipts)).
		Msg("Processing block for event logs")
		
	if len(receipts) == 0 {
		p.logger.Debug().
			Uint64("block", block.NumberU64()).
			Msg("No receipts to process")
		return nil
	}

	// Collect all logs from receipts
	var allLogs []types.Log
	for _, receipt := range receipts {
		if len(receipt.Logs) > 0 {
			p.logger.Debug().
				Str("tx_hash", receipt.TxHash.Hex()).
				Int("logs", len(receipt.Logs)).
				Msg("Found logs in receipt")
		}
		for _, log := range receipt.Logs {
			allLogs = append(allLogs, *log)
		}
	}

	if len(allLogs) == 0 {
		p.logger.Debug().
			Uint64("block", block.NumberU64()).
			Int("receipts", len(receipts)).
			Msg("No event logs found in receipts")
		return nil
	}

	p.logger.Info().
		Uint64("block", block.NumberU64()).
		Int("log_count", len(allLogs)).
		Msg("Processing event logs")

	// Store logs in database
	return p.storeLogs(ctx, block, allLogs)
}

// ProcessBlockLogsBatch processes logs for multiple blocks in a batch
func (p *EventProcessor) ProcessBlockLogsBatch(ctx context.Context, blocks []*types.Block, receiptsMap map[common.Hash][]*types.Receipt) error {
	var allLogs []types.Log
	blockMap := make(map[common.Hash]*types.Block)

	for _, block := range blocks {
		blockMap[block.Hash()] = block
		receipts := receiptsMap[block.Hash()]
		for _, receipt := range receipts {
			for _, log := range receipt.Logs {
				allLogs = append(allLogs, *log)
			}
		}
	}

	if len(allLogs) == 0 {
		return nil
	}

	// Group logs by block for efficient storage
	logsByBlock := make(map[common.Hash][]types.Log)
	for _, log := range allLogs {
		logsByBlock[log.BlockHash] = append(logsByBlock[log.BlockHash], log)
	}

	// Store logs for each block
	for blockHash, logs := range logsByBlock {
		block := blockMap[blockHash]
		if block == nil {
			p.logger.Warn().Str("block_hash", blockHash.Hex()).Msg("Block not found for logs")
			continue
		}
		if err := p.storeLogs(ctx, block, logs); err != nil {
			return fmt.Errorf("failed to store logs for block %d: %w", block.NumberU64(), err)
		}
	}

	return nil
}

// storeLogs stores event logs in the database
func (p *EventProcessor) storeLogs(ctx context.Context, block *types.Block, logs []types.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Prepare batch insert
	batch := &pgx.Batch{}
	query := `
		INSERT INTO event_logs (
			block_number, block_hash, transaction_hash, transaction_index, log_index,
			address, topics, data, removed
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
		ON CONFLICT (block_number, transaction_index, log_index) DO UPDATE
		SET removed = EXCLUDED.removed`

	for _, log := range logs {
		// Convert topics to JSON array
		topicsJSON, err := json.Marshal(hashesToStrings(log.Topics))
		if err != nil {
			return fmt.Errorf("failed to marshal topics: %w", err)
		}

		batch.Queue(query,
			log.BlockNumber,
			log.BlockHash.Hex(),
			log.TxHash.Hex(),
			log.TxIndex,
			log.Index,
			log.Address.Hex(),
			topicsJSON,
			common.Bytes2Hex(log.Data),
			log.Removed,
		)
	}

	// Execute batch
	results := p.db.Pool().SendBatch(ctx, batch)
	defer results.Close()

	// Check for errors
	for j := 0; j < batch.Len(); j++ {
		if _, err := results.Exec(); err != nil {
			return fmt.Errorf("failed to insert log %d: %w", j, err)
		}
	}

	p.logger.Info().
		Uint64("block", block.NumberU64()).
		Int("logs_stored", len(logs)).
		Msg("Successfully stored event logs")

	return nil
}

// GetLogs retrieves logs for a specific block
func (p *EventProcessor) GetLogs(ctx context.Context, blockNumber uint64) ([]types.Log, error) {
	query := `
		SELECT block_hash, transaction_hash, transaction_index, log_index,
		       address, topics, data, removed
		FROM event_logs
		WHERE block_number = $1
		ORDER BY transaction_index, log_index`

	rows, err := p.db.Pool().Query(ctx, query, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to query logs: %w", err)
	}
	defer rows.Close()

	var logs []types.Log
	for rows.Next() {
		var (
			blockHash   string
			txHash      string
			txIndex     uint
			logIndex    uint
			address     string
			topicsJSON  json.RawMessage
			data        string
			removed     bool
		)

		err := rows.Scan(&blockHash, &txHash, &txIndex, &logIndex,
			&address, &topicsJSON, &data, &removed)
		if err != nil {
			return nil, fmt.Errorf("failed to scan log: %w", err)
		}

		// Parse topics
		var topicStrings []string
		if err := json.Unmarshal(topicsJSON, &topicStrings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal topics: %w", err)
		}

		topics := stringsToHashes(topicStrings)

		logs = append(logs, types.Log{
			BlockNumber: blockNumber,
			BlockHash:   common.HexToHash(blockHash),
			TxHash:      common.HexToHash(txHash),
			TxIndex:     txIndex,
			Index:       logIndex,
			Address:     common.HexToAddress(address),
			Topics:      topics,
			Data:        common.Hex2Bytes(data),
			Removed:     removed,
		})
	}

	return logs, rows.Err()
}

// GetLogsByAddress retrieves logs for a specific contract address
func (p *EventProcessor) GetLogsByAddress(ctx context.Context, address common.Address, fromBlock, toBlock uint64) ([]types.Log, error) {
	query := `
		SELECT block_number, block_hash, transaction_hash, transaction_index, log_index,
		       topics, data, removed
		FROM event_logs
		WHERE address = $1 AND block_number >= $2 AND block_number <= $3
		ORDER BY block_number, transaction_index, log_index`

	rows, err := p.db.Pool().Query(ctx, query, strings.ToLower(address.Hex()), fromBlock, toBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to query logs by address: %w", err)
	}
	defer rows.Close()

	var logs []types.Log
	for rows.Next() {
		var (
			blockNumber uint64
			blockHash   string
			txHash      string
			txIndex     uint
			logIndex    uint
			topicsJSON  json.RawMessage
			data        string
			removed     bool
		)

		err := rows.Scan(&blockNumber, &blockHash, &txHash, &txIndex, &logIndex,
			&topicsJSON, &data, &removed)
		if err != nil {
			return nil, fmt.Errorf("failed to scan log: %w", err)
		}

		// Parse topics
		var topicStrings []string
		if err := json.Unmarshal(topicsJSON, &topicStrings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal topics: %w", err)
		}

		topics := stringsToHashes(topicStrings)

		logs = append(logs, types.Log{
			BlockNumber: blockNumber,
			BlockHash:   common.HexToHash(blockHash),
			TxHash:      common.HexToHash(txHash),
			TxIndex:     txIndex,
			Index:       logIndex,
			Address:     address,
			Topics:      topics,
			Data:        common.Hex2Bytes(data),
			Removed:     removed,
		})
	}

	return logs, rows.Err()
}

// GetLogsByTopics retrieves logs matching specific topics
func (p *EventProcessor) GetLogsByTopics(ctx context.Context, topics []common.Hash, fromBlock, toBlock uint64) ([]types.Log, error) {
	// Build topic filter for JSONB
	topicFilters := make([]string, 0, len(topics))
	args := []interface{}{fromBlock, toBlock}
	
	for _, topic := range topics {
		args = append(args, fmt.Sprintf("\"%s\"", topic.Hex()))
		topicFilters = append(topicFilters, fmt.Sprintf("topics @> $%d::jsonb", len(args)))
	}

	query := fmt.Sprintf(`
		SELECT block_number, block_hash, transaction_hash, transaction_index, log_index,
		       address, topics, data, removed
		FROM event_logs
		WHERE block_number >= $1 AND block_number <= $2 AND %s
		ORDER BY block_number, transaction_index, log_index`,
		strings.Join(topicFilters, " AND "))

	rows, err := p.db.Pool().Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query logs by topics: %w", err)
	}
	defer rows.Close()

	var logs []types.Log
	for rows.Next() {
		var (
			blockNumber uint64
			blockHash   string
			txHash      string
			txIndex     uint
			logIndex    uint
			address     string
			topicsJSON  json.RawMessage
			data        string
			removed     bool
		)

		err := rows.Scan(&blockNumber, &blockHash, &txHash, &txIndex, &logIndex,
			&address, &topicsJSON, &data, &removed)
		if err != nil {
			return nil, fmt.Errorf("failed to scan log: %w", err)
		}

		// Parse topics
		var topicStrings []string
		if err := json.Unmarshal(topicsJSON, &topicStrings); err != nil {
			return nil, fmt.Errorf("failed to unmarshal topics: %w", err)
		}

		topicHashes := stringsToHashes(topicStrings)

		logs = append(logs, types.Log{
			BlockNumber: blockNumber,
			BlockHash:   common.HexToHash(blockHash),
			TxHash:      common.HexToHash(txHash),
			TxIndex:     txIndex,
			Index:       logIndex,
			Address:     common.HexToAddress(address),
			Topics:      topicHashes,
			Data:        common.Hex2Bytes(data),
			Removed:     removed,
		})
	}

	return logs, rows.Err()
}

// Helper functions
func hashesToStrings(hashes []common.Hash) []string {
	strings := make([]string, len(hashes))
	for i, hash := range hashes {
		strings[i] = hash.Hex()
	}
	return strings
}

func stringsToHashes(strings []string) []common.Hash {
	hashes := make([]common.Hash, len(strings))
	for i, str := range strings {
		hashes[i] = common.HexToHash(str)
	}
	return hashes
}