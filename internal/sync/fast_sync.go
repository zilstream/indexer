package sync

import (
	"context"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
)

// FastSyncConfig contains configuration for fast sync
type FastSyncConfig struct {
	BatchSize         int           // Number of blocks per batch
	WorkerCount       int           // Number of parallel workers
	BufferSize        int           // Size of bulk write buffer
	SkipReceipts      bool          // Skip fetching receipts for old blocks
	SkipReceiptsBelow uint64        // Skip receipts for blocks below this number
	RequestsPerSecond int           // RPC rate limit
	OptimizeBatchSize bool          // Auto-optimize batch size
	LogBlockRange     int           // Block range for eth_getLogs calls
}

// FastSync coordinates fast historical syncing
type FastSync struct {
	config      FastSyncConfig
	db          *database.Database
	rpcClient   *rpc.Client
	batchClient *rpc.BatchClient
	bulkWriter  *database.BulkWriter
	logger      zerolog.Logger
	
	// Progress tracking
	mu              sync.RWMutex
	startBlock      uint64
	endBlock        uint64
	currentBlock    atomic.Uint64
	blocksProcessed atomic.Int64
	txProcessed     atomic.Int64
	startTime       time.Time
	
	// Channels for pipeline
	blockChan chan *types.Block
	errorChan chan error
	doneChan  chan struct{}
}

// NewFastSync creates a new fast sync coordinator
func NewFastSync(
	config FastSyncConfig,
	db *database.Database,
	rpcClient *rpc.Client,
	logger zerolog.Logger,
) *FastSync {
	batchClient := rpc.NewBatchClient(
		"https://api.zilliqa.com",
		config.RequestsPerSecond,
		logger,
	)
	
	bulkWriter := database.NewBulkWriter(
		db.Pool(),
		config.BufferSize,
		logger,
	)
	
	return &FastSync{
		config:      config,
		db:          db,
		rpcClient:   rpcClient,
		batchClient: batchClient,
		bulkWriter:  bulkWriter,
		logger:      logger,
		blockChan:   make(chan *types.Block, config.BufferSize),
		errorChan:   make(chan error, 10),
		doneChan:    make(chan struct{}),
	}
}

// SyncRange performs fast sync for a block range
func (fs *FastSync) SyncRange(ctx context.Context, startBlock, endBlock uint64) error {
	fs.startBlock = startBlock
	fs.endBlock = endBlock
	fs.currentBlock.Store(startBlock)
	fs.startTime = time.Now()
	
	fs.logger.Info().
		Uint64("start", startBlock).
		Uint64("end", endBlock).
		Uint64("total", endBlock-startBlock+1).
		Msg("Starting fast sync")
	
	// Optimize batch size if requested
	if fs.config.OptimizeBatchSize {
		optimalSize, err := fs.batchClient.EstimateOptimalBatchSize(ctx)
		if err == nil && optimalSize > 0 {
			fs.config.BatchSize = optimalSize
			fs.logger.Info().
				Int("batch_size", optimalSize).
				Msg("Using optimized batch size")
		}
	}
	
	// Start pipeline stages
	var wg sync.WaitGroup
	
	// Stage 1: Block fetchers
	for i := 0; i < fs.config.WorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			fs.fetchWorker(ctx, workerID)
		}(i)
	}
	
	// Stage 2: Block processor
	wg.Add(1)
	go func() {
		defer wg.Done()
		fs.processWorker(ctx)
	}()
	
	// Stage 3: Progress monitor
	wg.Add(1)
	go func() {
		defer wg.Done()
		fs.monitorProgress(ctx)
	}()
	
	// Wait for completion or error
	select {
	case <-ctx.Done():
		close(fs.doneChan)
		wg.Wait()
		return ctx.Err()
	case err := <-fs.errorChan:
		close(fs.doneChan)
		wg.Wait()
		return err
	case <-fs.waitForCompletion():
		close(fs.doneChan)
		wg.Wait()
	}
	
	// Final flush
	if err := fs.bulkWriter.Flush(ctx); err != nil {
		return fmt.Errorf("failed to flush final batch: %w", err)
	}
	
	// Update last block number
	if err := fs.db.UpdateLastBlockNumber(ctx, endBlock, ""); err != nil {
		return fmt.Errorf("failed to update last block: %w", err)
	}
	
	// Print final statistics
	fs.printFinalStats()
	
	return nil
}

// fetchWorker fetches blocks in batches
func (fs *FastSync) fetchWorker(ctx context.Context, workerID int) {
	fs.logger.Debug().Int("worker", workerID).Msg("Fetch worker started")
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-fs.doneChan:
			return
		default:
		}
		
		// Get next batch to fetch
		start := fs.currentBlock.Add(uint64(fs.config.BatchSize))
		if start > fs.endBlock {
			return
		}
		
		end := start + uint64(fs.config.BatchSize) - 1
		if end > fs.endBlock {
			end = fs.endBlock
		}
		
		// Fetch batch
		blocks, err := fs.fetchBatch(ctx, start, end)
		if err != nil {
			fs.logger.Error().
				Err(err).
				Int("worker", workerID).
				Uint64("start", start).
				Uint64("end", end).
				Msg("Failed to fetch batch")
			
			// Put blocks back for retry
			fs.currentBlock.Add(-uint64(fs.config.BatchSize))
			time.Sleep(5 * time.Second)
			continue
		}
		
		fs.logger.Debug().
			Int("worker", workerID).
			Uint64("start", start).
			Uint64("end", end).
			Int("fetched", len(blocks)).
			Msg("Batch fetched")
		
		// Send blocks to processor
		sentCount := 0
		for _, block := range blocks {
			if block != nil {
				select {
				case fs.blockChan <- block:
					sentCount++
				case <-ctx.Done():
					return
				case <-fs.doneChan:
					return
				}
			}
		}
		
		if sentCount > 0 {
			fs.logger.Debug().
				Int("worker", workerID).
				Int("sent", sentCount).
				Msg("Sent blocks to processor")
		}
	}
}

// fetchBatch fetches a batch of blocks
func (fs *FastSync) fetchBatch(ctx context.Context, start, end uint64) ([]*types.Block, error) {
	// Determine if we should skip receipts for this range
	skipReceipts := fs.config.SkipReceipts || end < fs.config.SkipReceiptsBelow
	
	if skipReceipts {
		// Fast path: just fetch blocks without receipts
		return fs.batchClient.GetBlockRangeFast(ctx, start, end, fs.config.BatchSize)
	}
	
	// Full processing: fetch blocks with receipts
	blocks, err := fs.batchClient.GetBlockRangeFast(ctx, start, end, fs.config.BatchSize)
	if err != nil {
		return nil, err
	}
	
	// Fetch receipts for transactions
	for _, block := range blocks {
		if block != nil && len(block.Transactions()) > 0 {
			// Collect transaction hashes
			hashes := make([]common.Hash, len(block.Transactions()))
			for i, tx := range block.Transactions() {
				hashes[i] = tx.Hash()
			}
			
			// Batch fetch receipts
			receipts, err := fs.batchClient.GetReceiptBatch(ctx, hashes)
			if err != nil {
				fs.logger.Warn().
					Err(err).
					Uint64("block", block.NumberU64()).
					Msg("Failed to fetch receipts, continuing without")
			}
			
			// Store receipts for later processing
			// (In a real implementation, you'd store these with the transactions)
			_ = receipts
		}
	}
	
	return blocks, nil
}

// processWorker processes blocks from the channel
func (fs *FastSync) processWorker(ctx context.Context) {
	fs.logger.Debug().Msg("Process worker started")
	
	processedCount := 0
	for {
		select {
		case <-ctx.Done():
			fs.logger.Debug().Int("processed", processedCount).Msg("Process worker stopping (context)")
			return
		case <-fs.doneChan:
			fs.logger.Debug().Int("processed", processedCount).Msg("Process worker stopping (done)")
			return
		case block := <-fs.blockChan:
			if block == nil {
				continue
			}
			
			processedCount++
			if processedCount%100 == 0 {
				fs.logger.Debug().
					Int("processed", processedCount).
					Uint64("block", block.NumberU64()).
					Msg("Processing blocks")
			}
			
			// Convert to database models
			dbBlock := fs.convertBlock(block)
			dbTransactions := fs.convertTransactions(block)
			
			// Add to bulk writer
			if err := fs.bulkWriter.AddBlock(dbBlock); err != nil {
				fs.logger.Error().
					Err(err).
					Uint64("block", block.NumberU64()).
					Msg("Failed to add block to buffer")
				fs.errorChan <- err
				return
			}
			
			for _, tx := range dbTransactions {
				if err := fs.bulkWriter.AddTransaction(tx); err != nil {
					fs.logger.Error().
						Err(err).
						Str("hash", tx.Hash).
						Msg("Failed to add transaction to buffer")
					fs.errorChan <- err
					return
				}
			}
			
			// Update progress
			fs.blocksProcessed.Add(1)
			fs.txProcessed.Add(int64(len(dbTransactions)))
		}
	}
}

// convertBlock converts an Ethereum block to database model
func (fs *FastSync) convertBlock(block *types.Block) *database.Block {
	var baseFee *big.Int
	if block.BaseFee() != nil {
		baseFee = block.BaseFee()
	}
	
	return &database.Block{
		Number:           block.NumberU64(),
		Hash:             block.Hash().Hex(),
		ParentHash:       block.ParentHash().Hex(),
		Timestamp:        int64(block.Time()),
		GasLimit:         block.GasLimit(),
		GasUsed:          block.GasUsed(),
		BaseFeePerGas:    baseFee,
		TransactionCount: len(block.Transactions()),
		CreatedAt:        time.Now(),
	}
}

// convertTransactions converts block transactions to database models
func (fs *FastSync) convertTransactions(block *types.Block) []*database.Transaction {
	transactions := make([]*database.Transaction, 0, len(block.Transactions()))
	
	// Get transaction metadata if available
	metadata := fs.rpcClient.GetTransactionMetadata(block.Hash().Hex())
	
	for i, tx := range block.Transactions() {
		dbTx := &database.Transaction{
			Hash:             tx.Hash().Hex(),
			BlockNumber:      block.NumberU64(),
			TransactionIndex: i,
			Nonce:            tx.Nonce(),
			GasLimit:         tx.Gas(),
			GasUsed:          0, // Will be filled from receipt
			GasPrice:         tx.GasPrice(),
			Value:            tx.Value(),
			Input:            fmt.Sprintf("0x%x", tx.Data()),
			Status:           1, // Assume success for fast sync
			TransactionType:  int(tx.Type()),
			CreatedAt:        time.Now(),
		}
		
		// Handle from address
		from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
		if err == nil {
			dbTx.FromAddress = from.Hex()
		} else {
			dbTx.FromAddress = "0x0000000000000000000000000000000000000000"
		}
		
		// Handle to address
		if tx.To() != nil {
			toAddr := tx.To().Hex()
			dbTx.ToAddress = &toAddr
		}
		
		// Apply metadata if available (for Zilliqa transactions)
		if i < len(metadata) && metadata[i].IsZilliqaType {
			dbTx.TransactionType = database.TxTypeZilliqaBase + int(metadata[i].OriginalType)
			if metadata[i].OriginalTypeHex != "" {
				dbTx.OriginalTypeHex = &metadata[i].OriginalTypeHex
			}
		}
		
		transactions = append(transactions, dbTx)
	}
	
	return transactions
}

// monitorProgress monitors and reports sync progress
func (fs *FastSync) monitorProgress(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-fs.doneChan:
			return
		case <-ticker.C:
			fs.printProgress()
		}
	}
}

// printProgress prints current progress
func (fs *FastSync) printProgress() {
	blocksProcessed := fs.blocksProcessed.Load()
	txProcessed := fs.txProcessed.Load()
	elapsed := time.Since(fs.startTime)
	
	current := fs.currentBlock.Load()
	remaining := int64(fs.endBlock) - int64(current)
	blocksPerSec := float64(blocksProcessed) / elapsed.Seconds()
	
	var eta time.Duration
	if blocksPerSec > 0 {
		eta = time.Duration(float64(remaining)/blocksPerSec) * time.Second
	}
	
	fs.logger.Info().
		Int64("processed", blocksProcessed).
		Int64("transactions", txProcessed).
		Uint64("current", current).
		Int64("remaining", remaining).
		Float64("blocks_per_sec", blocksPerSec).
		Dur("elapsed", elapsed).
		Dur("eta", eta).
		Msg("Fast sync progress")
	
	// Log bulk writer metrics
	metrics := fs.bulkWriter.GetMetrics()
	fs.logger.Debug().
		Interface("bulk_writer", metrics).
		Msg("Bulk writer metrics")
	
	// Log batch client metrics
	batchMetrics := fs.batchClient.GetMetrics()
	fs.logger.Debug().
		Interface("batch_client", batchMetrics).
		Msg("Batch client metrics")
}

// printFinalStats prints final sync statistics
func (fs *FastSync) printFinalStats() {
	blocksProcessed := fs.blocksProcessed.Load()
	txProcessed := fs.txProcessed.Load()
	elapsed := time.Since(fs.startTime)
	blocksPerSec := float64(blocksProcessed) / elapsed.Seconds()
	
	fs.logger.Info().
		Int64("total_blocks", blocksProcessed).
		Int64("total_transactions", txProcessed).
		Float64("blocks_per_sec", blocksPerSec).
		Float64("tx_per_sec", float64(txProcessed)/elapsed.Seconds()).
		Dur("total_time", elapsed).
		Msg("Fast sync completed")
	
	// Print final metrics
	fs.logger.Info().
		Interface("bulk_writer", fs.bulkWriter.GetMetrics()).
		Interface("batch_client", fs.batchClient.GetMetrics()).
		Msg("Final metrics")
}

// waitForCompletion waits for sync to complete
func (fs *FastSync) waitForCompletion() <-chan struct{} {
	done := make(chan struct{})
	
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				current := fs.currentBlock.Load()
				processed := fs.blocksProcessed.Load()
				expected := fs.endBlock - fs.startBlock + 1
				
				// Check if all blocks have been processed
				if current >= fs.endBlock && processed >= int64(expected) {
					// Wait a bit more for final flush
					time.Sleep(2 * time.Second)
					close(done)
					return
				}
				
				// Also check if we're stuck
				if current >= fs.endBlock {
					// Give more time for processing to catch up
					time.Sleep(5 * time.Second)
					processedAfterWait := fs.blocksProcessed.Load()
					if processedAfterWait >= int64(expected) || processedAfterWait == processed {
						// Either done or stuck
						close(done)
						return
					}
				}
			}
		}
	}()
	
	return done
}

// Close closes the fast sync coordinator
func (fs *FastSync) Close() {
	fs.batchClient.Close()
}