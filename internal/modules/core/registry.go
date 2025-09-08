package core

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

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

// ProcessEvent routes an event to interested modules
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
	return err
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