package loader

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/rs/zerolog"
	"gopkg.in/yaml.v3"

	"github.com/zilstream/indexer/internal/modules/core"
)

// ManifestLoader handles loading and parsing module manifests
type ManifestLoader struct {
	logger zerolog.Logger
}

// NewManifestLoader creates a new manifest loader
func NewManifestLoader(logger zerolog.Logger) *ManifestLoader {
	return &ManifestLoader{
		logger: logger.With().Str("component", "manifest_loader").Logger(),
	}
}

// LoadFromFile loads a single manifest from a file
func (l *ManifestLoader) LoadFromFile(path string) (*core.Manifest, error) {
	l.logger.Debug().Str("path", path).Msg("Loading manifest from file")

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read manifest file %s: %w", path, err)
	}

	return l.ParseManifest(data)
}

// LoadFromDirectory loads all manifests from a directory
func (l *ManifestLoader) LoadFromDirectory(dir string) ([]*core.Manifest, error) {
	l.logger.Debug().Str("dir", dir).Msg("Loading manifests from directory")

	var manifests []*core.Manifest

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories and non-YAML files
		if d.IsDir() {
			return nil
		}

		if !isManifestFile(d.Name()) {
			l.logger.Debug().Str("file", d.Name()).Msg("Skipping non-manifest file")
			return nil
		}

		manifest, err := l.LoadFromFile(path)
		if err != nil {
			l.logger.Warn().Err(err).Str("path", path).Msg("Failed to load manifest, skipping")
			return nil // Continue processing other files
		}

		manifests = append(manifests, manifest)
		l.logger.Info().
			Str("name", manifest.Name).
			Str("version", manifest.Version).
			Str("path", path).
			Msg("Loaded manifest")

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to walk directory %s: %w", dir, err)
	}

	return manifests, nil
}

// ParseManifest parses a YAML manifest from bytes
func (l *ManifestLoader) ParseManifest(data []byte) (*core.Manifest, error) {
	var manifest core.Manifest

	if err := yaml.Unmarshal(data, &manifest); err != nil {
		return nil, fmt.Errorf("failed to parse YAML manifest: %w", err)
	}

	// Validate the manifest
	if err := manifest.ValidateManifest(); err != nil {
		return nil, fmt.Errorf("invalid manifest: %w", err)
	}

	// Set defaults if not specified
	l.setDefaults(&manifest)

	return &manifest, nil
}

// setDefaults sets default values for manifest fields
func (l *ManifestLoader) setDefaults(manifest *core.Manifest) {
	// Set default data source values
	for i := range manifest.DataSources {
		ds := &manifest.DataSources[i]

		// Default kind
		if ds.Kind == "" {
			ds.Kind = "ethereum/contract"
		}

		// Default network
		if ds.Network == "" {
			ds.Network = "zilliqa"
		}

		// Default mapping values
		if ds.Mapping.Kind == "" {
			ds.Mapping.Kind = "ethereum/events"
		}

		if ds.Mapping.APIVersion == "" {
			ds.Mapping.APIVersion = "0.0.1"
		}

		if ds.Mapping.Language == "" {
			ds.Mapping.Language = "wasm/assemblyscript"
		}

		// Ensure start block is set (default to 0)
		if ds.Source.StartBlock == nil {
			startBlock := uint64(0)
			ds.Source.StartBlock = &startBlock
		}
	}
}

// ValidateManifestDirectory checks if a directory contains valid manifests
func (l *ManifestLoader) ValidateManifestDirectory(dir string) error {
	// Check if directory exists
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		return fmt.Errorf("manifest directory does not exist: %s", dir)
	}

	// Try to load manifests
	manifests, err := l.LoadFromDirectory(dir)
	if err != nil {
		return fmt.Errorf("failed to validate manifest directory: %w", err)
	}

	// Check for duplicate module names
	seen := make(map[string]string)
	for _, manifest := range manifests {
		if existing, exists := seen[manifest.Name]; exists {
			return fmt.Errorf("duplicate module name '%s' found in directory (conflicts with %s)", 
				manifest.Name, existing)
		}
		seen[manifest.Name] = manifest.Name
	}

	l.logger.Info().
		Int("count", len(manifests)).
		Str("dir", dir).
		Msg("Successfully validated manifest directory")

	return nil
}

// GetManifestTemplate returns a template manifest for creating new modules
func (l *ManifestLoader) GetManifestTemplate(moduleName string) *core.Manifest {
	return &core.Manifest{
		Name:        moduleName,
		Version:     "1.0.0",
		Description: "Description for " + moduleName + " module",
		Repository:  "",
		DataSources: []core.DataSource{
			{
				Kind:    "ethereum/contract",
				Name:    moduleName + "Contract",
				Network: "zilliqa",
				Source: core.DataSourceSource{
					Address:    stringPtr("0x0000000000000000000000000000000000000000"),
					ABI:        moduleName + "ABI",
					StartBlock: uint64Ptr(0),
				},
				Mapping: core.DataSourceMapping{
					Kind:       "ethereum/events",
					APIVersion: "0.0.1",
					Language:   "wasm/assemblyscript",
					Entities:   []string{moduleName + "Entity"},
					EventHandlers: []core.EventHandler{
						{
							Event:   "ExampleEvent(indexed address,uint256)",
							Handler: "handleExampleEvent",
						},
					},
				},
			},
		},
	}
}

// SerializeManifest converts a manifest to YAML bytes
func (l *ManifestLoader) SerializeManifest(manifest *core.Manifest) ([]byte, error) {
	data, err := yaml.Marshal(manifest)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize manifest: %w", err)
	}

	return data, nil
}

// isManifestFile checks if a filename appears to be a manifest file
func isManifestFile(filename string) bool {
	// Accept .yaml and .yml extensions
	ext := strings.ToLower(filepath.Ext(filename))
	return ext == ".yaml" || ext == ".yml"
}

// stringPtr returns a pointer to a string (helper for setting optional fields)
func stringPtr(s string) *string {
	return &s
}

// uint64Ptr returns a pointer to a uint64 (helper for setting optional fields)
func uint64Ptr(i uint64) *uint64 {
	return &i
}

// ManifestConfig represents configuration for the manifest loader
type ManifestConfig struct {
	// Directory to scan for manifests
	ManifestsDir string `yaml:"manifests_dir"`
	
	// Whether to watch for manifest changes
	WatchForChanges bool `yaml:"watch_for_changes"`
	
	// File pattern to match manifests
	FilePattern string `yaml:"file_pattern"`
}

// DefaultManifestConfig returns default configuration for manifest loading
func DefaultManifestConfig() ManifestConfig {
	return ManifestConfig{
		ManifestsDir:    "manifests",
		WatchForChanges: false,
		FilePattern:     "*.yaml",
	}
}