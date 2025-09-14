package core

import (
	"context"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/zilstream/indexer/internal/database"
)

// Module represents a processing module that handles specific blockchain events
// Inspired by The Graph Protocol's subgraph pattern
type Module interface {
	// Name returns the unique name of the module
	Name() string

	// Version returns the module version
	Version() string

	// Manifest returns the module's manifest configuration
	Manifest() *Manifest

	// Initialize sets up the module with database connection and any required state
	Initialize(ctx context.Context, db *database.Database) error

	// HandleEvent processes a single event log that matches this module's filters
	HandleEvent(ctx context.Context, event *types.Log) error

	// GetEventFilters returns the event filters this module is interested in
	GetEventFilters() []EventFilter

	// GetStartBlock returns the block number from which this module should start processing
	GetStartBlock() uint64

	// Backfill processes historical events from the event_logs table
	Backfill(ctx context.Context, fromBlock, toBlock uint64) error

	// GetSyncState returns the last processed block for this module
	GetSyncState(ctx context.Context) (uint64, error)

	// UpdateSyncState updates the last processed block for this module
	UpdateSyncState(ctx context.Context, blockNumber uint64) error
}

// BatchModule extends Module with batch processing capabilities for performance
type BatchModule interface {
	Module

	// HandleEventBatch processes multiple events in a single database transaction
	// This provides much better performance than HandleEvent for bulk operations
	HandleEventBatch(ctx context.Context, events []*types.Log) error
}

// EventFilter defines what events a module wants to receive
type EventFilter struct {
	// Address is the contract address to watch (optional, empty = all addresses)
	Address string `yaml:"address,omitempty"`
	
	// Topic0 is the event signature hash (optional, empty = all events)
	Topic0 string `yaml:"topic0,omitempty"`
	
	// Topics are additional indexed parameters to filter by
	Topics []string `yaml:"topics,omitempty"`
}

// ModuleState represents the current processing state of a module
type ModuleState struct {
	ModuleName          string `db:"module_name"`
	Version             string `db:"version"`
	LastProcessedBlock  uint64 `db:"last_processed_block"`
	Status              string `db:"status"`
	BackfillFromBlock   *uint64 `db:"backfill_from_block"`
	BackfillToBlock     *uint64 `db:"backfill_to_block"`
	Metadata            []byte  `db:"metadata"` // JSON
	CreatedAt           int64   `db:"created_at"`
	UpdatedAt           int64   `db:"updated_at"`
}

// ModuleStatus represents the possible states of a module
type ModuleStatus string

const (
	StatusActive     ModuleStatus = "active"
	StatusBackfilling ModuleStatus = "backfilling"
	StatusPaused     ModuleStatus = "paused"
	StatusError      ModuleStatus = "error"
)