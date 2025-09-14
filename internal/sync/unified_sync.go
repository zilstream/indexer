package sync

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"

	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/modules/core"
	"github.com/zilstream/indexer/internal/rpc"
)

// UnifiedSync handles all synchronization with adaptive batching
type UnifiedSync struct {
	db         *database.Database
	rpc        *rpc.Client
	batch      *rpc.BatchClient
	writer     *database.AtomicBlockWriter
	bulkWriter *database.BulkWriter
	modules    *core.ModuleRegistry
	logger     zerolog.Logger

	// Configuration
	maxBatchSize      int
	maxRetries        int
	retryDelay        time.Duration
	requestsPerSecond int
	useEthGetLogs     bool
}

// UnifiedSyncConfig holds configuration for unified sync
type UnifiedSyncConfig struct {
	MaxBatchSize      int
	MaxRetries        int
	RetryDelay        time.Duration
	RequestsPerSecond int
	UseEthGetLogs     bool // Use eth_getLogs instead of fetching individual receipts for events
}

// NewUnifiedSync creates a new unified sync instance
func NewUnifiedSync(
	db *database.Database,
	rpcClient *rpc.Client,
	modules *core.ModuleRegistry,
	config UnifiedSyncConfig,
	logger zerolog.Logger,
) *UnifiedSync {
	// Create batch client for efficient fetching
	batchClient := rpc.NewBatchClient(rpcClient.GetEndpoint(), config.MaxBatchSize, logger)

	// Create atomic writer and bulk writer
	writer := database.NewAtomicBlockWriter(db.Pool(), logger)
	bulkWriter := database.NewBulkWriter(db.Pool(), logger)


	return &UnifiedSync{
		db:                db,
		rpc:               rpcClient,
		batch:             batchClient,
		writer:            writer,
		bulkWriter:        bulkWriter,
		modules:           modules,
		logger:            logger.With().Str("component", "unified_sync").Logger(),
		maxBatchSize:      config.MaxBatchSize,
		maxRetries:        config.MaxRetries,
		retryDelay:        config.RetryDelay,
		requestsPerSecond: config.RequestsPerSecond,
		useEthGetLogs:     config.UseEthGetLogs,
	}
}

// SyncRange syncs a range of blocks
func (s *UnifiedSync) SyncRange(ctx context.Context, startBlock, endBlock uint64) error {
	s.logger.Info().
		Uint64("start", startBlock).
		Uint64("end", endBlock).
		Uint64("total", endBlock-startBlock+1).
		Msg("Starting sync range")


	startTime := time.Now()
	current := startBlock
	totalProcessed := uint64(0)
	
	s.logger.Debug().Uint64("current", current).Uint64("endBlock", endBlock).Msg("Starting sync loop")
	for current <= endBlock {
		// Calculate adaptive batch size based on gap
		gap := endBlock - current + 1
		batchSize := s.calculateBatchSize(gap)
		
		// Don't exceed the remaining blocks
		if current+uint64(batchSize)-1 > endBlock {
			batchSize = int(endBlock - current + 1)
		}
		
		// Fetch and process batch
		batchEnd := current + uint64(batchSize) - 1
		
		s.logger.Debug().
			Uint64("from", current).
			Uint64("to", batchEnd).
			Int("batch_size", batchSize).
			Msg("Processing batch")
		
		err := s.processBatch(ctx, current, batchEnd)
		if err != nil {
			// Retry with exponential backoff
			for retry := 1; retry <= s.maxRetries; retry++ {
				delay := s.retryDelay * time.Duration(1<<(retry-1))
				s.logger.Warn().
					Err(err).
					Uint64("block", current).
					Int("retry", retry).
					Dur("delay", delay).
					Msg("Batch failed, retrying")
				
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
				
				err = s.processBatch(ctx, current, batchEnd)
				if err == nil {
					break
				}
			}
			
			if err != nil {
				return fmt.Errorf("failed to process batch %d-%d after %d retries: %w", 
					current, batchEnd, s.maxRetries, err)
			}
		}
		
		// Update progress
		totalProcessed += uint64(batchSize)
		current = batchEnd + 1
		
		// Log progress periodically
		if totalProcessed%1000 == 0 || current > endBlock {
			elapsed := time.Since(startTime)
			blocksPerSec := float64(totalProcessed) / elapsed.Seconds()
			remaining := endBlock - current + 1
			eta := time.Duration(float64(remaining) / blocksPerSec * float64(time.Second))
			
			s.logger.Info().
				Uint64("processed", totalProcessed).
				Uint64("current", current-1).
				Uint64("remaining", remaining).
				Float64("blocks_per_sec", blocksPerSec).
				Dur("elapsed", elapsed).
				Dur("eta", eta).
				Msg("Sync progress")
		}
	}
	
	elapsed := time.Since(startTime)
	blocksPerSec := float64(totalProcessed) / elapsed.Seconds()
	
	s.logger.Info().
		Uint64("total_processed", totalProcessed).
		Dur("elapsed", elapsed).
		Float64("blocks_per_sec", blocksPerSec).
		Msg("Sync range completed")
	
	return nil
}

// processBatch fetches and stores a batch of blocks atomically
func (s *UnifiedSync) processBatch(ctx context.Context, startBlock, endBlock uint64) error {
	batchStart := time.Now()
	blockCount := endBlock - startBlock + 1
	s.logger.Info().Uint64("start", startBlock).Uint64("end", endBlock).Uint64("count", blockCount).Msg("Starting processBatch")

	// Phase 1: Fetch blocks and events (optimized path)
	fetchStart := time.Now()
	var rawBlocks []*rpc.RawBlock
	var receiptsMap map[uint64][]*types.Receipt
	var directEventLogs map[uint64][]*database.EventLog
	var err error

	if s.useEthGetLogs {
		rawBlocks, directEventLogs, err = s.fetchBlocksWithLogs(ctx, startBlock, endBlock)
	} else {
		rawBlocks, receiptsMap, err = s.fetchBlocksWithReceipts(ctx, startBlock, endBlock)
	}
	if err != nil {
		return fmt.Errorf("failed to fetch blocks: %w", err)
	}
	fetchDuration := time.Since(fetchStart)

	// Phase 2: Process raw blocks into database format
	processStart := time.Now()
	dbBlocks := make([]*database.Block, 0, len(rawBlocks))
	transactionsByBlock := make(map[uint64][]*database.Transaction)
	eventLogsByBlock := make(map[uint64][]*database.EventLog)

	totalTxs := 0
	totalLogs := 0
	
	for _, rawBlock := range rawBlocks {
		if rawBlock == nil {
			continue
		}
		
		blockNum := (*big.Int)(rawBlock.Number).Uint64()
		
		// Convert raw block to database block
		dbBlock := s.convertRawBlock(rawBlock)
		dbBlocks = append(dbBlocks, dbBlock)
		
		// Convert raw transactions to database transactions - preserve original data
		dbTransactions := s.convertRawTransactions(rawBlock, receiptsMap[blockNum])
		if len(dbTransactions) > 0 {
			transactionsByBlock[blockNum] = dbTransactions
			totalTxs += len(dbTransactions)
		}

		// Extract and convert event logs
		if directEventLogs != nil {
			// Use direct event logs from eth_getLogs
			if eventLogs, ok := directEventLogs[blockNum]; ok && len(eventLogs) > 0 {
				eventLogsByBlock[blockNum] = eventLogs
				totalLogs += len(eventLogs)
			}
		} else if receipts, ok := receiptsMap[blockNum]; ok {
			// Extract from receipts (fallback path)
			eventLogs := s.extractEventLogsFromReceipts(blockNum, rawBlock.Hash.Hex(), receipts)
			if len(eventLogs) > 0 {
				eventLogsByBlock[blockNum] = eventLogs
				totalLogs += len(eventLogs)
			}
		}
	}
	processDuration := time.Since(processStart)

	// Phase 3: Write to database
	dbWriteStart := time.Now()
	batchSize := len(dbBlocks)
	var writerType string
	if batchSize >= 50 { // Use bulk writer for 50+ blocks
		writerType = "bulk"
		err = s.bulkWriter.WriteBatchBulk(ctx, dbBlocks, transactionsByBlock, eventLogsByBlock)
		if err != nil {
			return fmt.Errorf("failed to bulk write batch: %w", err)
		}
	} else {
		writerType = "atomic"
		// Use atomic writer for small batches to maintain ACID guarantees near head
		err = s.writer.WriteBatch(ctx, dbBlocks, transactionsByBlock, eventLogsByBlock)
		if err != nil {
			return fmt.Errorf("failed to write batch: %w", err)
		}
	}
	dbWriteDuration := time.Since(dbWriteStart)
	
	// Phase 4: Process events synchronously in the correct order
	var eventProcessDuration time.Duration
	processedEvents := 0
	if s.modules != nil && len(eventLogsByBlock) > 0 {
		eventProcessStart := time.Now()

		// Sort blocks by block number to ensure correct processing order
		var sortedBlockNums []uint64
		for blockNum := range eventLogsByBlock {
			sortedBlockNums = append(sortedBlockNums, blockNum)
		}

		// Sort block numbers
		for i := 0; i < len(sortedBlockNums); i++ {
			for j := i + 1; j < len(sortedBlockNums); j++ {
				if sortedBlockNums[i] > sortedBlockNums[j] {
					sortedBlockNums[i], sortedBlockNums[j] = sortedBlockNums[j], sortedBlockNums[i]
				}
			}
		}

		// Collect all events in proper order for batch processing
		var allEvents []*types.Log
		for _, blockNum := range sortedBlockNums {
			eventLogs := eventLogsByBlock[blockNum]

			// Convert database event logs to ethereum types.Log
			for _, dbLog := range eventLogs {
				ethLog := s.convertToEthLog(dbLog)
				allEvents = append(allEvents, &ethLog)
				processedEvents++
			}
		}

		// Process all events in a single batch for maximum performance
		if len(allEvents) > 0 {
			if err := s.modules.ProcessEventBatch(ctx, allEvents); err != nil {
				s.logger.Error().
					Err(err).
					Int("event_count", len(allEvents)).
					Msg("Failed to process event batch in modules")
				// Note: Individual module errors are handled within ProcessEventBatch
			}
		}
		eventProcessDuration = time.Since(eventProcessStart)
	}

	// Phase 5: Update last block number
	updateStart := time.Now()
	if len(dbBlocks) > 0 {
		lastBlock := dbBlocks[len(dbBlocks)-1]
		if err := s.db.UpdateLastBlockNumber(ctx, lastBlock.Number, lastBlock.Hash); err != nil {
			return fmt.Errorf("failed to update last block: %w", err)
		}
	}
	updateDuration := time.Since(updateStart)

	// Performance summary
	totalDuration := time.Since(batchStart)
	blocksPerSec := float64(blockCount) / totalDuration.Seconds()

	s.logger.Info().
		Uint64("blocks", blockCount).
		Int("transactions", totalTxs).
		Int("event_logs", totalLogs).
		Int("processed_events", processedEvents).
		Str("writer_type", writerType).
		Dur("fetch_time", fetchDuration).
		Dur("process_time", processDuration).
		Dur("db_write_time", dbWriteDuration).
		Dur("event_process_time", eventProcessDuration).
		Dur("update_time", updateDuration).
		Dur("total_time", totalDuration).
		Float64("blocks_per_sec", blocksPerSec).
		Float64("fetch_pct", 100*fetchDuration.Seconds()/totalDuration.Seconds()).
		Float64("db_write_pct", 100*dbWriteDuration.Seconds()/totalDuration.Seconds()).
		Float64("event_process_pct", 100*eventProcessDuration.Seconds()/totalDuration.Seconds()).
		Msg("ProcessBatch performance breakdown")

	return nil
}

// fetchBlocksWithReceipts fetches blocks and their receipts
func (s *UnifiedSync) fetchBlocksWithReceipts(ctx context.Context, startBlock, endBlock uint64) ([]*rpc.RawBlock, map[uint64][]*types.Receipt, error) {
	batchSize := int(endBlock - startBlock + 1)
	
	// For small batches, use regular RPC
	if batchSize <= 10 {
		return s.fetchBlocksNormal(ctx, startBlock, endBlock)
	}
	
	// For large batches, use batch RPC - get raw blocks to preserve transaction metadata
	s.logger.Debug().Uint64("start", startBlock).Uint64("end", endBlock).Msg("Fetching raw blocks")
	rawBlocks, err := s.batch.GetBlockBatchRaw(ctx, s.makeBlockNumbers(startBlock, endBlock))
	if err != nil {
		return nil, nil, err
	}
	s.logger.Debug().Int("count", len(rawBlocks)).Msg("Fetched raw blocks")
	
	// Collect ALL transaction hashes across all blocks
	allHashes := []common.Hash{}
	hashToBlock := make(map[common.Hash]uint64) // Map hash to block number
	blockTxCounts := make(map[uint64]int) // Track tx count per block
	
	for _, rawBlock := range rawBlocks {
		if rawBlock == nil || len(rawBlock.Transactions) == 0 {
			continue
		}
		
		blockNum := (*big.Int)(rawBlock.Number).Uint64()
		blockTxCounts[blockNum] = len(rawBlock.Transactions)
		
		// Collect transaction hashes directly from raw data
		for _, tx := range rawBlock.Transactions {
			allHashes = append(allHashes, tx.Hash)
			hashToBlock[tx.Hash] = blockNum
		}
	}
	
	s.logger.Debug().Int("tx_count", len(allHashes)).Msg("Fetching receipts for all transactions")
	
	// Fetch receipts in parallel chunks for maximum performance
	receiptsMap := make(map[uint64][]*types.Receipt)
	if len(allHashes) > 0 {
		allReceipts, err := s.fetchReceiptsParallel(ctx, allHashes)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to fetch receipts in parallel: %w", err)
		}

		// Organize receipts by block number
		for i, receipt := range allReceipts {
			if receipt != nil && i < len(allHashes) {
				blockNum := hashToBlock[allHashes[i]]
				if receiptsMap[blockNum] == nil {
					receiptsMap[blockNum] = make([]*types.Receipt, 0, blockTxCounts[blockNum])
				}
				receiptsMap[blockNum] = append(receiptsMap[blockNum], receipt)
			}
		}

		s.logger.Debug().Int("receipts_fetched", len(allReceipts)).Msg("Receipts fetched")
	}

	return rawBlocks, receiptsMap, nil
}

// fetchBlocksWithLogs fetches blocks and their event logs using eth_getLogs (MUCH faster!)
func (s *UnifiedSync) fetchBlocksWithLogs(ctx context.Context, startBlock, endBlock uint64) ([]*rpc.RawBlock, map[uint64][]*database.EventLog, error) {
	batchSize := int(endBlock - startBlock + 1)

	// Step 1: Fetch raw blocks (preserve transaction metadata)
	s.logger.Debug().Uint64("start", startBlock).Uint64("end", endBlock).Msg("Fetching raw blocks")
	rawBlocks, err := s.batch.GetBlockBatchRaw(ctx, s.makeBlockNumbers(startBlock, endBlock))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to fetch raw blocks: %w", err)
	}

	// Step 2: Fetch ALL event logs for the block range with a single eth_getLogs call
	logsFetchStart := time.Now()
	allLogs, err := s.batch.GetLogs(ctx, startBlock, endBlock, nil) // nil = all addresses
	if err != nil {
		// Fallback to receipt-based approach on getLogs failure
		s.logger.Warn().
			Err(err).
			Uint64("start", startBlock).
			Uint64("end", endBlock).
			Msg("eth_getLogs failed, falling back to receipt fetching")

		// Fetch with receipts and convert to the expected return format
		rawBlocks, receiptsMap, err := s.fetchBlocksWithReceipts(ctx, startBlock, endBlock)
		if err != nil {
			return nil, nil, err
		}

		// Convert receipts to event logs format for consistency
		eventLogsByBlock := make(map[uint64][]*database.EventLog)
		for blockNum, receipts := range receiptsMap {
			if len(receipts) > 0 {
				// Find the block hash from rawBlocks
				var blockHashStr string
				for _, rawBlock := range rawBlocks {
					if (*big.Int)(rawBlock.Number).Uint64() == blockNum {
						blockHashStr = rawBlock.Hash.Hex()
						break
					}
				}
				eventLogs := s.extractEventLogsFromReceipts(blockNum, blockHashStr, receipts)
				if len(eventLogs) > 0 {
					eventLogsByBlock[blockNum] = eventLogs
				}
			}
		}

		return rawBlocks, eventLogsByBlock, nil
	}

	logsFetchTime := time.Since(logsFetchStart)
	s.logger.Info().
		Uint64("start", startBlock).
		Uint64("end", endBlock).
		Int("blocks", batchSize).
		Int("logs", len(allLogs)).
		Dur("logs_fetch_time", logsFetchTime).
		Msg("Fetched all logs with eth_getLogs - MASSIVE PERFORMANCE WIN!")

	// Step 3: Group logs by block number and convert to database format
	eventLogsByBlock := make(map[uint64][]*database.EventLog)
	for _, log := range allLogs {
		blockNum := log.BlockNumber

		// Convert to database event log format
		topics := make([]string, len(log.Topics))
		for i, topic := range log.Topics {
			topics[i] = topic.Hex()
		}

		dbEventLog := &database.EventLog{
			BlockNumber:      blockNum,
			BlockHash:        log.BlockHash.Hex(),
			TransactionHash:  log.TxHash.Hex(),
			TransactionIndex: int(log.TxIndex),
			LogIndex:         int(log.Index),
			Address:          log.Address.Hex(),
			Topics:           topics,
			Data:             common.Bytes2Hex(log.Data),
			Removed:          log.Removed,
		}

		eventLogsByBlock[blockNum] = append(eventLogsByBlock[blockNum], dbEventLog)
	}

	s.logger.Debug().
		Int("raw_blocks", len(rawBlocks)).
		Int("event_logs_blocks", len(eventLogsByBlock)).
		Msg("Grouped logs by block")

	return rawBlocks, eventLogsByBlock, nil
}

// fetchReceiptsParallel fetches receipts in parallel for maximum performance
func (s *UnifiedSync) fetchReceiptsParallel(ctx context.Context, allHashes []common.Hash) ([]*types.Receipt, error) {
	totalHashes := len(allHashes)
	if totalHashes == 0 {
		return nil, nil
	}

	// Dynamic chunk sizing based on total hashes for optimal performance
	chunkSize := 5000
	if totalHashes > 50000 {
		chunkSize = 10000 // Larger chunks for massive syncs
	} else if totalHashes < 1000 {
		chunkSize = 500 // Smaller chunks for small syncs
	}

	// Calculate number of chunks
	numChunks := (totalHashes + chunkSize - 1) / chunkSize

	// Limit concurrent requests to avoid overwhelming RPC
	maxConcurrency := int64(8)
	if numChunks < 4 {
		maxConcurrency = int64(numChunks)
	}
	sem := semaphore.NewWeighted(maxConcurrency)

	// Prepare results slice
	allReceipts := make([]*types.Receipt, totalHashes)
	var wg sync.WaitGroup
	var errOnce sync.Once
	var fetchErr error

	// Process chunks in parallel
	for i := 0; i < totalHashes; i += chunkSize {
		end := i + chunkSize
		if end > totalHashes {
			end = totalHashes
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()

			// Acquire semaphore
			if err := sem.Acquire(ctx, 1); err != nil {
				errOnce.Do(func() { fetchErr = err })
				return
			}
			defer sem.Release(1)

			chunk := allHashes[start:end]
			startTime := time.Now()

			chunkReceipts, err := s.batch.GetReceiptBatch(ctx, chunk)
			if err != nil {
				s.logger.Warn().
					Err(err).
					Int("chunk_size", len(chunk)).
					Int("chunk_start", start).
					Dur("elapsed", time.Since(startTime)).
					Msg("Failed to fetch receipts chunk, creating empty receipts")

				// Create empty receipts for this chunk
				chunkReceipts = make([]*types.Receipt, len(chunk))
			}

			// Copy results to correct position in main slice
			copy(allReceipts[start:], chunkReceipts)

			s.logger.Debug().
				Int("chunk_size", len(chunk)).
				Int("start", start).
				Int("end", end).
				Dur("elapsed", time.Since(startTime)).
				Msg("Receipt chunk fetched")
		}(i, end)
	}

	// Wait for all goroutines to complete
	wg.Wait()

	if fetchErr != nil {
		return nil, fmt.Errorf("failed to acquire semaphore: %w", fetchErr)
	}

	return allReceipts, nil
}

// fetchBlocksNormal fetches blocks using normal RPC (for small batches)
func (s *UnifiedSync) fetchBlocksNormal(ctx context.Context, startBlock, endBlock uint64) ([]*rpc.RawBlock, map[uint64][]*types.Receipt, error) {
	// Use batch client even for small batches to get raw data
	blockNumbers := s.makeBlockNumbers(startBlock, endBlock)
	rawBlocks, err := s.batch.GetBlockBatchRaw(ctx, blockNumbers)
	if err != nil {
		return nil, nil, err
	}
	
	receiptsMap := make(map[uint64][]*types.Receipt)
	for _, rawBlock := range rawBlocks {
		if rawBlock == nil || len(rawBlock.Transactions) == 0 {
			continue
		}
		
		blockNum := (*big.Int)(rawBlock.Number).Uint64()
		
		// Fetch receipts
		receipts, err := s.rpc.GetBlockReceipts(ctx, blockNum)
		if err != nil {
			s.logger.Warn().
				Err(err).
				Uint64("block", blockNum).
				Msg("Failed to fetch receipts, continuing without")
			receipts = []*types.Receipt{}
		}
		
		if len(receipts) > 0 {
			receiptsMap[blockNum] = receipts
		}
	}
	
	return rawBlocks, receiptsMap, nil
}

// makeBlockNumbers creates a slice of block numbers
func (s *UnifiedSync) makeBlockNumbers(start, end uint64) []uint64 {
	nums := make([]uint64, 0, end-start+1)
	for n := start; n <= end; n++ {
		nums = append(nums, n)
	}
	return nums
}

// calculateBatchSize calculates optimal batch size based on gap
func (s *UnifiedSync) calculateBatchSize(gap uint64) int {
	switch {
	case gap <= 10:
		return 1 // Near head: process one by one
	case gap <= 100:
		return 10 // Small gap: moderate batches
	case gap <= 1000:
		return 50 // Medium gap: larger batches
	case gap <= 10000:
		return 100 // Large gap: big batches
	default:
		return s.maxBatchSize // Huge gap: maximum speed
	}
}

// convertRawBlock converts a raw RPC block to database model
func (s *UnifiedSync) convertRawBlock(rawBlock *rpc.RawBlock) *database.Block {
	return &database.Block{
		Number:           (*big.Int)(rawBlock.Number).Uint64(),
		Hash:             rawBlock.Hash.Hex(),
		ParentHash:       rawBlock.ParentHash.Hex(),
		Timestamp:        (*big.Int)(rawBlock.Timestamp).Int64(),
		GasLimit:         (*big.Int)(rawBlock.GasLimit).Uint64(),
		GasUsed:          (*big.Int)(rawBlock.GasUsed).Uint64(),
		BaseFeePerGas:    (*big.Int)(rawBlock.BaseFeePerGas),
		TransactionCount: len(rawBlock.Transactions),
		CreatedAt:        time.Now(),
	}
}

// convertRawTransactions converts raw RPC transactions to database models - preserves original data
func (s *UnifiedSync) convertRawTransactions(rawBlock *rpc.RawBlock, receipts []*types.Receipt) []*database.Transaction {
	transactions := make([]*database.Transaction, 0, len(rawBlock.Transactions))
	receiptMap := make(map[common.Hash]*types.Receipt)
	
	// Build receipt map for quick lookup
	for _, receipt := range receipts {
		if receipt != nil {
			receiptMap[receipt.TxHash] = receipt
		}
	}
	
	blockNum := (*big.Int)(rawBlock.Number).Uint64()
	
	for i, rawTx := range rawBlock.Transactions {
		// Get transaction type
		var txType int
		if rawTx.Type != nil {
			typeVal := (*big.Int)(rawTx.Type).Uint64()
			if typeVal > 3 {
				// Zilliqa pre-EVM transaction
				txType = 1000 + int(typeVal)
			} else {
				txType = int(typeVal)
			}
		}
		
		dbTx := &database.Transaction{
			Hash:             rawTx.Hash.Hex(),  // Use hash directly from RPC
			BlockNumber:      blockNum,
			TransactionIndex: i,
			FromAddress:      rawTx.From.Hex(),  // Use from address directly from RPC
			Nonce:            uint64(rawTx.Nonce),
			GasLimit:         uint64(rawTx.Gas),
			GasPrice:         (*big.Int)(rawTx.GasPrice),
			Value:            (*big.Int)(rawTx.Value),
			Input:            common.Bytes2Hex(rawTx.Input), // Store as hex string to avoid UTF8 issues
			TransactionType:  txType,
			CreatedAt:        time.Now(),
		}
		
		// Set to address if present
		if rawTx.To != nil {
			toAddr := rawTx.To.Hex()
			dbTx.ToAddress = &toAddr
		}
		
		// Add receipt data if available
		if receipt, ok := receiptMap[rawTx.Hash]; ok {
			dbTx.Status = int(receipt.Status)
			dbTx.GasUsed = receipt.GasUsed
		}
		
		transactions = append(transactions, dbTx)
	}
	
	return transactions
}

// extractEventLogsFromReceipts extracts event logs from receipts
func (s *UnifiedSync) extractEventLogsFromReceipts(blockNum uint64, blockHash string, receipts []*types.Receipt) []*database.EventLog {
	var eventLogs []*database.EventLog
	
	for _, receipt := range receipts {
		if receipt == nil || len(receipt.Logs) == 0 {
			continue
		}
		
		for _, log := range receipt.Logs {
		if log == nil {
				continue
			}
			
			// Convert topics to string array
			topics := make([]string, len(log.Topics))
			for i, topic := range log.Topics {
				topics[i] = topic.Hex()
			}
			
			eventLog := &database.EventLog{
				BlockNumber:      blockNum,
				BlockHash:        blockHash,
				TransactionHash:  log.TxHash.Hex(),
				TransactionIndex: int(log.TxIndex),
				LogIndex:         int(log.Index),
				Address:          log.Address.Hex(),
				Topics:           topics,
				Data:             common.Bytes2Hex(log.Data),
				Removed:          log.Removed,
				CreatedAt:        time.Now(),
			}
			
			eventLogs = append(eventLogs, eventLog)
		}
	}
	
	return eventLogs
}

// convertToEthLog converts a database event log back to types.Log
func (s *UnifiedSync) convertToEthLog(dbLog *database.EventLog) types.Log {
	topics := make([]common.Hash, len(dbLog.Topics))
	for i, topicStr := range dbLog.Topics {
		topics[i] = common.HexToHash(topicStr)
	}
	
	return types.Log{
		Address:     common.HexToAddress(dbLog.Address),
		Topics:      topics,
		Data:        common.Hex2Bytes(dbLog.Data),
		BlockNumber: dbLog.BlockNumber,
		TxHash:      common.HexToHash(dbLog.TransactionHash),
		TxIndex:     uint(dbLog.TransactionIndex),
		BlockHash:   common.HexToHash(dbLog.BlockHash),
		Index:       uint(dbLog.LogIndex),
		Removed:     dbLog.Removed,
	}
}

// Close closes the unified sync
func (s *UnifiedSync) Close() {
	if s.batch != nil {
		s.batch.Close()
	}
}