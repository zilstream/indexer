package core

// Manifest defines the structure of a module manifest (inspired by subgraph manifests)
type Manifest struct {
	Name        string                 `yaml:"name"`
	Version     string                 `yaml:"version"`
	Description string                 `yaml:"description,omitempty"`
	Repository  string                 `yaml:"repository,omitempty"`
	DataSources []DataSource           `yaml:"dataSources"`
	Context     map[string]interface{} `yaml:"context,omitempty"` // Module-specific context
}

// DataSource defines a contract or set of contracts to watch
type DataSource struct {
	Kind    string                 `yaml:"kind"`    // "ethereum/contract"
	Name    string                 `yaml:"name"`    // Friendly name
	Network string                 `yaml:"network"` // "zilliqa"
	Source  DataSourceSource       `yaml:"source"`
	Mapping DataSourceMapping      `yaml:"mapping"`
	Context map[string]interface{} `yaml:"context,omitempty"` // Additional context
}

// DataSourceSource defines the contract source information
type DataSourceSource struct {
	Address    *string `yaml:"address,omitempty"`   // Contract address (optional for templates)
	ABI        string  `yaml:"abi"`                 // ABI name
	StartBlock *uint64 `yaml:"startBlock,omitempty"` // Block to start indexing from
}

// DataSourceMapping defines how to handle events from this data source
type DataSourceMapping struct {
	Kind          string         `yaml:"kind"`          // "ethereum/events"
	APIVersion    string         `yaml:"apiVersion,omitempty"` // "0.0.1"
	Language      string         `yaml:"language,omitempty"`   // "wasm/assemblyscript"
	Entities      []string       `yaml:"entities"`      // List of entities this mapping creates
	EventHandlers []EventHandler `yaml:"eventHandlers"`
	BlockHandlers []BlockHandler `yaml:"blockHandlers,omitempty"`
	CallHandlers  []CallHandler  `yaml:"callHandlers,omitempty"`
}

// EventHandler defines how to handle a specific event
type EventHandler struct {
	Event   string `yaml:"event"`   // Event signature (e.g., "Transfer(indexed address,indexed address,uint256)")
	Handler string `yaml:"handler"` // Handler function name
	Filter  *EventHandlerFilter `yaml:"filter,omitempty"` // Optional event filter
}

// EventHandlerFilter provides additional filtering for events
type EventHandlerFilter struct {
	// Add filters like min block, topics, etc.
}

// BlockHandler defines how to handle blocks (optional)
type BlockHandler struct {
	Handler string                 `yaml:"handler"`
	Filter  map[string]interface{} `yaml:"filter,omitempty"`
}

// CallHandler defines how to handle contract calls (optional) 
type CallHandler struct {
	Function string                 `yaml:"function"`
	Handler  string                 `yaml:"handler"`
	Filter   map[string]interface{} `yaml:"filter,omitempty"`
}

// ValidateManifest validates a manifest structure
func (m *Manifest) ValidateManifest() error {
	if m.Name == "" {
		return ErrInvalidManifest{Field: "name", Reason: "name is required"}
	}
	
	if m.Version == "" {
		return ErrInvalidManifest{Field: "version", Reason: "version is required"}
	}
	
	if len(m.DataSources) == 0 {
		return ErrInvalidManifest{Field: "dataSources", Reason: "at least one data source is required"}
	}
	
	for i, ds := range m.DataSources {
		if err := ds.validate(); err != nil {
			return ErrInvalidManifest{Field: "dataSources[" + string(rune(i)) + "]", Reason: err.Error()}
		}
	}
	
	return nil
}

func (ds *DataSource) validate() error {
	if ds.Kind == "" {
		return ErrInvalidManifest{Field: "kind", Reason: "kind is required"}
	}
	
	if ds.Name == "" {
		return ErrInvalidManifest{Field: "name", Reason: "name is required"}
	}
	
	if ds.Source.ABI == "" {
		return ErrInvalidManifest{Field: "source.abi", Reason: "ABI is required"}
	}
	
	if len(ds.Mapping.EventHandlers) == 0 {
		return ErrInvalidManifest{Field: "mapping.eventHandlers", Reason: "at least one event handler is required"}
	}
	
	return nil
}

// ErrInvalidManifest is returned when a manifest is invalid
type ErrInvalidManifest struct {
	Field  string
	Reason string
}

func (e ErrInvalidManifest) Error() string {
	return "invalid manifest field " + e.Field + ": " + e.Reason
}