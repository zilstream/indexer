package sync

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
)

// BlockProcessor interface to avoid circular dependency
type BlockProcessor interface {
	ProcessBlock(ctx context.Context, blockNumber uint64) error
}

type Manager struct {
	db        *database.DB
	rpc       *rpc.Client
	processor BlockProcessor
	logger    zerolog.Logger

	// Sync state
	mu             sync.RWMutex
	isSyncing      bool
	lastSyncedBlock uint64
	latestChainBlock uint64
	
	// Gap tracking
	gaps []BlockRange
	
	// Configuration
	batchSize    int
	maxWorkers   int
	retryDelay   time.Duration
	maxRetries   int
	startBlock   uint64
}

type BlockRange struct {
	Start uint64
	End   uint64
}

type Config struct {
	BatchSize  int
	MaxWorkers int
	RetryDelay time.Duration
	MaxRetries int
	StartBlock uint64 // Minimum block to sync from
}

func NewManager(
	db *database.DB,
	rpc *rpc.Client,
	processor BlockProcessor,
	logger zerolog.Logger,
	config Config,
) *Manager {
	return &Manager{
		db:          db,
		rpc:         rpc,
		processor:   processor,
		logger:      logger,
		batchSize:   config.BatchSize,
		maxWorkers:  config.MaxWorkers,
		retryDelay:  config.RetryDelay,
		maxRetries:  config.MaxRetries,
		startBlock:  config.StartBlock,
		gaps:        make([]BlockRange, 0),
	}
}

// Start begins the sync process
func (m *Manager) Start(ctx context.Context) error {
	m.logger.Info().Msg("Starting sync manager")
	
	// Initialize sync state
	if err := m.initializeSyncState(ctx); err != nil {
		return err
	}
	
	// Start sync workers
	var wg sync.WaitGroup
	
	// Main sync worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.runMainSync(ctx)
	}()
	
	// Gap filler worker
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.runGapFiller(ctx)
	}()
	
	// Health monitor
	wg.Add(1)
	go func() {
		defer wg.Done()
		m.runHealthMonitor(ctx)
	}()
	
	// Wait for all workers to complete
	wg.Wait()
	
	m.logger.Info().Msg("Sync manager stopped")
	return nil
}

// initializeSyncState sets up initial sync state from database
func (m *Manager) initializeSyncState(ctx context.Context) error {
	// Get last synced block from database
	lastBlock, err := m.db.GetLastBlock(ctx)
	if err != nil && !errors.Is(err, database.ErrNotFound) {
		return err
	}
	
	if lastBlock != nil {
		// If we have a start block configured and the last synced block is before it,
		// start from the start block instead
		if m.startBlock > 0 && lastBlock.Number < m.startBlock {
			m.lastSyncedBlock = m.startBlock - 1
			m.logger.Info().
				Uint64("last_db_block", lastBlock.Number).
				Uint64("start_block", m.startBlock).
				Msg("Last synced block is before start block, starting from configured start block")
		} else {
			m.lastSyncedBlock = lastBlock.Number
			m.logger.Info().Uint64("block", m.lastSyncedBlock).Msg("Resuming from last synced block")
		}
	} else {
		// If no blocks synced yet, start from configured start block
		if m.startBlock > 0 {
			m.lastSyncedBlock = m.startBlock - 1 // Set to one before start block
			m.logger.Info().Uint64("block", m.startBlock).Msg("Starting from configured start block")
		} else {
			m.lastSyncedBlock = 0
			m.logger.Info().Msg("Starting fresh sync from genesis")
		}
	}
	
	// Get latest chain block
	latestBlock, err := m.rpc.GetLatestBlockNumber(ctx)
	if err != nil {
		return err
	}
	m.latestChainBlock = latestBlock
	
	// Detect any gaps in our sync
	gaps, err := m.detectGaps(ctx)
	if err != nil {
		return err
	}
	
	if len(gaps) > 0 {
		m.logger.Warn().Int("gap_count", len(gaps)).Msg("Detected gaps in sync")
		m.gaps = gaps
	}
	
	return nil
}

// detectGaps finds missing blocks in our database
func (m *Manager) detectGaps(ctx context.Context) ([]BlockRange, error) {
	if m.lastSyncedBlock == 0 {
		return nil, nil // No gaps if we haven't started syncing
	}
	
	// Only look for gaps from our configured start block onwards
	minBlock := uint64(0)
	if m.startBlock > 0 {
		minBlock = m.startBlock
		// If lastSyncedBlock is before our start block, no gaps to detect
		if m.lastSyncedBlock < m.startBlock {
			return nil, nil
		}
	}
	
	gaps, err := m.db.FindMissingBlocks(ctx, minBlock, m.lastSyncedBlock)
	if err != nil {
		return nil, err
	}
	
	// Convert to block ranges
	var ranges []BlockRange
	if len(gaps) > 0 {
		start := gaps[0]
		end := gaps[0]
		
		for i := 1; i < len(gaps); i++ {
			if gaps[i] == end+1 {
				end = gaps[i]
			} else {
				ranges = append(ranges, BlockRange{Start: start, End: end})
				start = gaps[i]
				end = gaps[i]
			}
		}
		ranges = append(ranges, BlockRange{Start: start, End: end})
	}
	
	return ranges, nil
}

// runMainSync handles syncing from the last synced block to chain tip
func (m *Manager) runMainSync(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second) // Zilliqa has 1-second blocks
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.syncToTip(ctx); err != nil {
				m.logger.Error().Err(err).Msg("Failed to sync to tip")
			}
		}
	}
}

// syncToTip syncs from current position to chain tip
func (m *Manager) syncToTip(ctx context.Context) error {
	// Get latest block number
	latestBlock, err := m.rpc.GetLatestBlockNumber(ctx)
	if err != nil {
		return err
	}
	
	m.mu.Lock()
	m.latestChainBlock = latestBlock
	currentBlock := m.lastSyncedBlock + 1
	m.mu.Unlock()
	
	// Check if we're caught up
	if currentBlock > latestBlock {
		return nil // Already at tip
	}
	
	// Calculate batch end
	batchEnd := currentBlock + uint64(m.batchSize) - 1
	if batchEnd > latestBlock {
		batchEnd = latestBlock
	}
	
	// Process batch
	m.logger.Debug().
		Uint64("from", currentBlock).
		Uint64("to", batchEnd).
		Msg("Processing block batch")
	
	if err := m.processBatch(ctx, currentBlock, batchEnd); err != nil {
		return err
	}
	
	// Update sync state
	m.mu.Lock()
	m.lastSyncedBlock = batchEnd
	m.mu.Unlock()
	
	return nil
}

// processBatch processes a range of blocks
func (m *Manager) processBatch(ctx context.Context, start, end uint64) error {
	// Create worker pool
	jobs := make(chan uint64, end-start+1)
	errors := make(chan error, end-start+1)
	
	// Rate limiter: process blocks with delay to avoid rate limiting
	rateLimiter := time.NewTicker(200 * time.Millisecond) // 5 blocks per second max
	defer rateLimiter.Stop()
	
	var wg sync.WaitGroup
	
	// Start workers
	workerCount := m.maxWorkers
	if int(end-start+1) < workerCount {
		workerCount = int(end - start + 1)
	}
	
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for blockNum := range jobs {
				// Wait for rate limiter
				<-rateLimiter.C
				
				if err := m.processBlockWithRetry(ctx, blockNum); err != nil {
					errors <- err
				}
			}
		}()
	}
	
	// Queue jobs
	for i := start; i <= end; i++ {
		jobs <- i
	}
	close(jobs)
	
	// Wait for completion
	wg.Wait()
	close(errors)
	
	// Check for errors
	var firstErr error
	for err := range errors {
		if firstErr == nil {
			firstErr = err
		}
		m.logger.Error().Err(err).Msg("Block processing error in batch")
	}
	
	return firstErr
}

// processBlockWithRetry processes a block with retry logic
func (m *Manager) processBlockWithRetry(ctx context.Context, blockNum uint64) error {
	var lastErr error
	
	for i := 0; i < m.maxRetries; i++ {
		if err := m.processor.ProcessBlock(ctx, blockNum); err != nil {
			lastErr = err
			m.logger.Warn().
				Err(err).
				Uint64("block", blockNum).
				Int("attempt", i+1).
				Msg("Failed to process block, retrying")
			
			// Exponential backoff
			delay := m.retryDelay * time.Duration(1<<i)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
				continue
			}
		}
		return nil // Success
	}
	
	// Add to gaps if all retries failed
	m.mu.Lock()
	m.gaps = append(m.gaps, BlockRange{Start: blockNum, End: blockNum})
	m.mu.Unlock()
	
	return lastErr
}

// runGapFiller handles filling gaps in sync
func (m *Manager) runGapFiller(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := m.fillGaps(ctx); err != nil {
				m.logger.Error().Err(err).Msg("Failed to fill gaps")
			}
		}
	}
}

// fillGaps attempts to fill detected gaps
func (m *Manager) fillGaps(ctx context.Context) error {
	m.mu.Lock()
	if len(m.gaps) == 0 {
		m.mu.Unlock()
		return nil
	}
	
	// Take first gap to process
	gap := m.gaps[0]
	m.gaps = m.gaps[1:]
	m.mu.Unlock()
	
	// Skip gaps that are entirely before our start block
	if m.startBlock > 0 && gap.End < m.startBlock {
		m.logger.Debug().
			Uint64("start", gap.Start).
			Uint64("end", gap.End).
			Uint64("start_block", m.startBlock).
			Msg("Skipping gap before start block")
		return nil
	}
	
	// Adjust gap start if it's before our start block
	if m.startBlock > 0 && gap.Start < m.startBlock {
		gap.Start = m.startBlock
	}
	
	m.logger.Info().
		Uint64("start", gap.Start).
		Uint64("end", gap.End).
		Msg("Filling gap")
	
	// Process the gap
	for blockNum := gap.Start; blockNum <= gap.End; blockNum++ {
		if err := m.processBlockWithRetry(ctx, blockNum); err != nil {
			// Re-add the remaining gap
			m.mu.Lock()
			m.gaps = append(m.gaps, BlockRange{Start: blockNum, End: gap.End})
			m.mu.Unlock()
			return err
		}
	}
	
	m.logger.Info().
		Uint64("start", gap.Start).
		Uint64("end", gap.End).
		Msg("Gap filled successfully")
	
	return nil
}

// runHealthMonitor monitors sync health
func (m *Manager) runHealthMonitor(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.logSyncStatus()
		}
	}
}

// logSyncStatus logs current sync status
func (m *Manager) logSyncStatus() {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	behindBy := int64(m.latestChainBlock) - int64(m.lastSyncedBlock)
	if behindBy < 0 {
		behindBy = 0
	}
	
	m.logger.Info().
		Uint64("synced", m.lastSyncedBlock).
		Uint64("chain_tip", m.latestChainBlock).
		Int64("behind_by", behindBy).
		Int("gaps", len(m.gaps)).
		Msg("Sync status")
}

// GetStatus returns current sync status
func (m *Manager) GetStatus() map[string]interface{} {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return map[string]interface{}{
		"is_syncing":      m.isSyncing,
		"last_synced":     m.lastSyncedBlock,
		"chain_tip":       m.latestChainBlock,
		"behind_by":       int64(m.latestChainBlock) - int64(m.lastSyncedBlock),
		"gaps_count":      len(m.gaps),
		"gaps":            m.gaps,
	}
}