package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"

	"github.com/zilstream/indexer/internal/database"
)

// ModuleRegistry manages the lifecycle of indexer modules
type ModuleRegistry struct {
	modules map[string]Module
	db      *database.Database
	parser  *EventParser
	logger  zerolog.Logger

	// Event routing
	eventFilters map[string][]string // topic -> module names
	addressFilters map[string][]string // address -> module names

	// Performance optimization - cached module status
	moduleStatus map[string]ModuleStatus
	statusMu     sync.RWMutex

	// Lifecycle management
	mu       sync.RWMutex
	running  bool
	ctx      context.Context
	cancel   context.CancelFunc
}

// NewModuleRegistry creates a new module registry
func NewModuleRegistry(db *database.Database, logger zerolog.Logger) *ModuleRegistry {
	ctx, cancel := context.WithCancel(context.Background())

	return &ModuleRegistry{
		modules:        make(map[string]Module),
		db:             db,
		parser:         NewEventParser(),
		logger:         logger.With().Str("component", "module_registry").Logger(),
		eventFilters:   make(map[string][]string),
		addressFilters: make(map[string][]string),
		moduleStatus:   make(map[string]ModuleStatus),
		ctx:            ctx,
		cancel:         cancel,
	}
}

// RegisterModule registers a new module
func (r *ModuleRegistry) RegisterModule(module Module) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	name := module.Name()
	
	// Check if module is already registered
	if _, exists := r.modules[name]; exists {
		return fmt.Errorf("module %s is already registered", name)
	}

	// Validate module manifest
	manifest := module.Manifest()
	if manifest == nil {
		return fmt.Errorf("module %s has no manifest", name)
	}

	if err := manifest.ValidateManifest(); err != nil {
		return fmt.Errorf("module %s has invalid manifest: %w", name, err)
	}

	// Initialize the module
	if err := module.Initialize(r.ctx, r.db); err != nil {
		return fmt.Errorf("failed to initialize module %s: %w", name, err)
	}

	// Register event filters
	filters := module.GetEventFilters()
	for _, filter := range filters {
		if filter.Topic0 != "" {
			lowerTopic := strings.ToLower(filter.Topic0)
			r.eventFilters[lowerTopic] = append(
				r.eventFilters[lowerTopic], name)
			r.logger.Debug().
				Str("module", name).
				Str("topic0", lowerTopic).
				Msg("Registered topic filter")
		}
		if filter.Address != "" {
			lowerAddr := strings.ToLower(filter.Address)
			r.addressFilters[lowerAddr] = append(
				r.addressFilters[lowerAddr], name)
			r.logger.Debug().
				Str("module", name).
				Str("address", lowerAddr).
				Msg("Registered address filter")
		}
	}

	// Store module
	r.modules[name] = module

	// Initialize or update module state in database
	if err := r.initializeModuleState(name, module.Version()); err != nil {
		r.logger.Error().Err(err).Str("module", name).Msg("Failed to initialize module state")
		return fmt.Errorf("failed to initialize module state for %s: %w", name, err)
	}

	// Cache the module status for performance
	r.cacheModuleStatus(name, StatusActive)

	r.logger.Info().
		Str("module", name).
		Str("version", module.Version()).
		Int("filters", len(filters)).
		Msg("Module registered successfully")

	return nil
}

// UnregisterModule removes a module from the registry
func (r *ModuleRegistry) UnregisterModule(name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.modules[name]; !exists {
		return fmt.Errorf("module %s is not registered", name)
	}

	// Remove from event filters
	for topic, moduleNames := range r.eventFilters {
		r.eventFilters[topic] = removeFromSlice(moduleNames, name)
		if len(r.eventFilters[topic]) == 0 {
			delete(r.eventFilters, topic)
		}
	}

	// Remove from address filters  
	for address, moduleNames := range r.addressFilters {
		r.addressFilters[address] = removeFromSlice(moduleNames, name)
		if len(r.addressFilters[address]) == 0 {
			delete(r.addressFilters, address)
		}
	}

	// Remove module
	delete(r.modules, name)

	r.logger.Info().Str("module", name).Msg("Module unregistered")
	return nil
}

// ProcessEventBatch routes multiple events to interested modules using batch processing for performance
func (r *ModuleRegistry) ProcessEventBatch(ctx context.Context, logs []*types.Log) error {
	if len(logs) == 0 {
		return nil
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.running {
		return nil // Skip processing if registry is not running
	}

	// Group events by module for batch processing
	eventsByModule := make(map[string][]*types.Log)
	totalEventsProcessed := 0

	for _, log := range logs {
		// Find interested modules for this event (optimized - no logging)
		interestedModules := r.findInterestedModules(log)

		if len(interestedModules) > 0 && len(log.Topics) > 0 {
			r.logger.Debug().
				Str("topic0", log.Topics[0].Hex()).
				Str("address", log.Address.Hex()).
				Uint64("block", log.BlockNumber).
				Strs("interested_modules", interestedModules).
				Msg("Event matched modules")
		}

		for _, moduleName := range interestedModules {
			eventsByModule[moduleName] = append(eventsByModule[moduleName], log)
			totalEventsProcessed++
		}
	}

	if totalEventsProcessed > 0 {
		r.logger.Info().
			Int("input_events", len(logs)).
			Int("matched_events", totalEventsProcessed).
			Int("modules_with_events", len(eventsByModule)).
			Msg("Event batch routing summary")
	}

	// Process events for each module in batches
	for moduleName, moduleEvents := range eventsByModule {
		module, exists := r.modules[moduleName]
		if !exists {
			r.logger.Warn().Str("module", moduleName).Msg("Module not found during batch event processing")
			continue
		}

		// Check if module should process events based on cached status
		status := r.getCachedModuleStatus(moduleName)
		if status != StatusActive && status != StatusBackfilling {
			continue // Skip inactive modules silently for performance
		}

		r.logger.Debug().
			Str("module", moduleName).
			Int("event_count", len(moduleEvents)).
			Msg("Processing events for module")

		// Try batch processing first, fall back to individual processing
		if batchModule, ok := module.(BatchModule); ok {
			if err := batchModule.HandleEventBatch(ctx, moduleEvents); err != nil {
				r.logger.Error().
					Err(err).
					Str("module", moduleName).
					Int("event_count", len(moduleEvents)).
					Msg("Module failed to process event batch")
				r.updateModuleStatus(moduleName, StatusError) // Ignore cache update errors for performance
			} else {
				r.logger.Debug().
					Str("module", moduleName).
					Int("event_count", len(moduleEvents)).
					Msg("Successfully processed event batch")
			}
		} else {
			// Fall back to individual event processing (should rarely happen)
			for _, event := range moduleEvents {
				if err := module.HandleEvent(ctx, event); err != nil {
					r.logger.Error().
						Err(err).
						Str("module", moduleName).
						Uint64("block", event.BlockNumber).
						Str("tx_hash", event.TxHash.Hex()).
						Msg("Module failed to process event")
					r.updateModuleStatus(moduleName, StatusError) // Ignore cache update errors for performance
					break // Stop processing this module's events on first error
				}
			}
		}
	}

	return nil
}

// ProcessEvent routes an event to interested modules (legacy method for compatibility)
func (r *ModuleRegistry) ProcessEvent(ctx context.Context, log *types.Log) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if !r.running {
		return nil // Skip processing if registry is not running
	}

	// Log the event being processed
	if len(log.Topics) > 0 {
		r.logger.Debug().
			Str("topic0", log.Topics[0].Hex()).
			Str("address", log.Address.Hex()).
			Uint64("block", log.BlockNumber).
			Int("registered_filters", len(r.eventFilters)).
			Msg("Registry processing event")
	}

	// Find interested modules
	interestedModules := r.findInterestedModules(log)
	if len(interestedModules) == 0 {
		if len(log.Topics) > 0 {
			r.logger.Debug().
				Str("topic0", log.Topics[0].Hex()).
				Msg("No modules interested in event")
		}
		return nil // No modules interested in this event
	}
	
	r.logger.Debug().
		Strs("modules", interestedModules).
		Str("topic0", log.Topics[0].Hex()).
		Msg("Found interested modules")

	// Process event for each interested module
	for _, moduleName := range interestedModules {
		module, exists := r.modules[moduleName]
		if !exists {
			r.logger.Warn().Str("module", moduleName).Msg("Module not found during event processing")
			continue
		}

		// Check if module should process this event based on its status
		status, err := r.getModuleStatus(moduleName)
		if err != nil {
			r.logger.Error().Err(err).Str("module", moduleName).Msg("Failed to get module status")
			continue
		}

		if status != StatusActive && status != StatusBackfilling {
			r.logger.Debug().
				Str("module", moduleName).
				Str("status", string(status)).
				Msg("Skipping event for inactive module")
			continue
		}

		// Process the event
		if err := module.HandleEvent(ctx, log); err != nil {
			r.logger.Error().
				Err(err).
				Str("module", moduleName).
				Uint64("block", log.BlockNumber).
				Str("tx_hash", log.TxHash.Hex()).
				Msg("Module failed to process event")

			// Update module status to error
			if err := r.updateModuleStatus(moduleName, StatusError); err != nil {
				r.logger.Error().Err(err).Str("module", moduleName).Msg("Failed to update module status to error")
			}
		}
	}

	return nil
}

// findInterestedModules finds modules that should process this event
func (r *ModuleRegistry) findInterestedModules(log *types.Log) []string {
	var interested []string
	seen := make(map[string]bool)

	// Check topic filters
	if len(log.Topics) > 0 {
		topic0 := strings.ToLower(log.Topics[0].Hex())
		if moduleNames, exists := r.eventFilters[topic0]; exists {
			for _, name := range moduleNames {
				if !seen[name] {
					interested = append(interested, name)
					seen[name] = true
				}
			}
		}
	}

	// Check address filters
	address := strings.ToLower(log.Address.Hex())
	if moduleNames, exists := r.addressFilters[address]; exists {
		for _, name := range moduleNames {
			if !seen[name] {
				interested = append(interested, name)
				seen[name] = true
			}
		}
	}

	return interested
}

// Start begins the module registry lifecycle
func (r *ModuleRegistry) Start() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.running {
		return fmt.Errorf("module registry is already running")
	}

	r.running = true
	r.logger.Info().Int("modules", len(r.modules)).Msg("Module registry started")

	return nil
}

// Stop gracefully stops the module registry
func (r *ModuleRegistry) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.running {
		return nil
	}

	r.running = false
	r.cancel()

	r.logger.Info().Msg("Module registry stopped")
	return nil
}

// GetModule returns a registered module by name
func (r *ModuleRegistry) GetModule(name string) (Module, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	module, exists := r.modules[name]
	return module, exists
}

// ListModules returns all registered module names
func (r *ModuleRegistry) ListModules() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.modules))
	for name := range r.modules {
		names = append(names, name)
	}

	return names
}

// GetModuleState returns the current state of a module
func (r *ModuleRegistry) GetModuleState(name string) (*ModuleState, error) {
	query := `
		SELECT module_name, version, last_processed_block, status, 
		       backfill_from_block, backfill_to_block, metadata,
		       EXTRACT(EPOCH FROM created_at)::bigint as created_at,
		       EXTRACT(EPOCH FROM updated_at)::bigint as updated_at
		FROM module_state 
		WHERE module_name = $1`

	var state ModuleState
	err := r.db.Pool().QueryRow(r.ctx, query, name).Scan(
		&state.ModuleName,
		&state.Version,
		&state.LastProcessedBlock,
		&state.Status,
		&state.BackfillFromBlock,
		&state.BackfillToBlock,
		&state.Metadata,
		&state.CreatedAt,
		&state.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get module state for %s: %w", name, err)
	}

	return &state, nil
}

// UpdateModuleBlock updates the last processed block for a module
func (r *ModuleRegistry) UpdateModuleBlock(name string, blockNumber uint64) error {
	query := `
		UPDATE module_state 
		SET last_processed_block = $2, updated_at = CURRENT_TIMESTAMP
		WHERE module_name = $1`

	_, err := r.db.Pool().Exec(r.ctx, query, name, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to update module block for %s: %w", name, err)
	}

	return nil
}

// initializeModuleState creates or updates module state in database
func (r *ModuleRegistry) initializeModuleState(name, version string) error {
	query := `
		INSERT INTO module_state (module_name, version, last_processed_block, status)
		VALUES ($1, $2, 0, $3)
		ON CONFLICT (module_name) 
		DO UPDATE SET 
			version = EXCLUDED.version,
			updated_at = CURRENT_TIMESTAMP`

	_, err := r.db.Pool().Exec(r.ctx, query, name, version, StatusActive)
	return err
}

// getModuleStatus returns the current status of a module
func (r *ModuleRegistry) getModuleStatus(name string) (ModuleStatus, error) {
	var status string
	query := `SELECT status FROM module_state WHERE module_name = $1`
	
	err := r.db.Pool().QueryRow(r.ctx, query, name).Scan(&status)
	if err != nil {
		return "", err
	}

	return ModuleStatus(status), nil
}

// updateModuleStatus updates the status of a module
func (r *ModuleRegistry) updateModuleStatus(name string, status ModuleStatus) error {
	query := `
		UPDATE module_state
		SET status = $2, updated_at = CURRENT_TIMESTAMP
		WHERE module_name = $1`

	_, err := r.db.Pool().Exec(r.ctx, query, name, string(status))
	if err == nil {
		// Update cache
		r.cacheModuleStatus(name, status)
	}
	return err
}

// getCachedModuleStatus returns cached module status, defaults to active
func (r *ModuleRegistry) getCachedModuleStatus(name string) ModuleStatus {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()

	if status, exists := r.moduleStatus[name]; exists {
		return status
	}
	return StatusActive // Default to active for performance
}

// cacheModuleStatus caches the module status
func (r *ModuleRegistry) cacheModuleStatus(name string, status ModuleStatus) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	r.moduleStatus[name] = status
}

// RecoverRecentBlocks reprocesses recent blocks for all modules to ensure data consistency after restart
func (r *ModuleRegistry) RecoverRecentBlocks(ctx context.Context, lastCoreBlock uint64, numBlocks uint64) error {
	if numBlocks == 0 {
		return nil
	}

	fromBlock := uint64(1)
	if lastCoreBlock > numBlocks {
		fromBlock = lastCoreBlock - numBlocks + 1
	}

	r.logger.Info().
		Uint64("from_block", fromBlock).
		Uint64("to_block", lastCoreBlock).
		Uint64("blocks_to_recover", numBlocks).
		Msg("Starting module recovery for recent blocks")

	// Fetch event logs for the block range from database
	query := `
		SELECT block_number, block_hash, transaction_hash, transaction_index, 
		       log_index, address, topics, data, removed
		FROM event_logs
		WHERE block_number BETWEEN $1 AND $2
		ORDER BY block_number ASC, transaction_index ASC, log_index ASC`

	rows, err := r.db.Pool().Query(ctx, query, fromBlock, lastCoreBlock)
	if err != nil {
		return fmt.Errorf("failed to query event logs: %w", err)
	}
	defer rows.Close()

	var events []*types.Log
	for rows.Next() {
		var dbLog database.EventLog
		var topics []string

		err := rows.Scan(
			&dbLog.BlockNumber,
			&dbLog.BlockHash,
			&dbLog.TransactionHash,
			&dbLog.TransactionIndex,
			&dbLog.LogIndex,
			&dbLog.Address,
			&topics,
			&dbLog.Data,
			&dbLog.Removed,
		)
		if err != nil {
			return fmt.Errorf("failed to scan event log: %w", err)
		}

		dbLog.Topics = topics
		ethLog := convertDBLogToEthLog(&dbLog)
		events = append(events, &ethLog)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("error iterating event logs: %w", err)
	}

	r.logger.Info().
		Int("events", len(events)).
		Uint64("from_block", fromBlock).
		Uint64("to_block", lastCoreBlock).
		Msg("Loaded events from database for recovery")

	// Process events through modules
	if len(events) > 0 {
		if err := r.ProcessEventBatch(ctx, events); err != nil {
			return fmt.Errorf("failed to process recovery events: %w", err)
		}
	}

	r.logger.Info().
		Int("events_processed", len(events)).
		Msg("Module recovery completed successfully")

	return nil
}

// convertDBLogToEthLog converts database event log to ethereum types.Log
func convertDBLogToEthLog(dbLog *database.EventLog) types.Log {
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

// TriggerBackfill starts backfilling for a module
func (r *ModuleRegistry) TriggerBackfill(name string, fromBlock, toBlock uint64) error {
	r.mu.RLock()
	module, exists := r.modules[name]
	r.mu.RUnlock()

	if !exists {
		return fmt.Errorf("module %s not found", name)
	}

	// Update module state to backfilling
	query := `
		UPDATE module_state 
		SET status = $2, backfill_from_block = $3, backfill_to_block = $4, 
		    updated_at = CURRENT_TIMESTAMP
		WHERE module_name = $1`

	_, err := r.db.Pool().Exec(r.ctx, query, name, StatusBackfilling, fromBlock, toBlock)
	if err != nil {
		return fmt.Errorf("failed to update module state for backfill: %w", err)
	}

	// Start backfill in goroutine
	go func() {
		r.logger.Info().
			Str("module", name).
			Uint64("from", fromBlock).
			Uint64("to", toBlock).
			Msg("Starting module backfill")

		start := time.Now()
		err := module.Backfill(r.ctx, fromBlock, toBlock)
		
		if err != nil {
			r.logger.Error().
				Err(err).
				Str("module", name).
				Dur("duration", time.Since(start)).
				Msg("Module backfill failed")
			
			r.updateModuleStatus(name, StatusError)
		} else {
			r.logger.Info().
				Str("module", name).
				Uint64("blocks", toBlock-fromBlock+1).
				Dur("duration", time.Since(start)).
				Msg("Module backfill completed")

			r.updateModuleStatus(name, StatusActive)

			// Clear backfill range
			clearQuery := `
				UPDATE module_state 
				SET backfill_from_block = NULL, backfill_to_block = NULL,
				    updated_at = CURRENT_TIMESTAMP
				WHERE module_name = $1`
			r.db.Pool().Exec(r.ctx, clearQuery, name)
		}
	}()

	return nil
}

// Helper function to remove an item from a slice
func removeFromSlice(slice []string, item string) []string {
	result := make([]string, 0, len(slice))
	for _, s := range slice {
		if s != item {
			result = append(result, s)
		}
	}
	return result
}