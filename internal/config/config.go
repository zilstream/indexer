package config

import (
	"fmt"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server    ServerConfig    `mapstructure:"server"`
	Chain     ChainConfig     `mapstructure:"chain"`
	Database  DatabaseConfig  `mapstructure:"database"`
	Processor ProcessorConfig `mapstructure:"processor"`
	Logging   LoggingConfig   `mapstructure:"logging"`
}

type ServerConfig struct {
	Port        int `mapstructure:"port"`
	MetricsPort int `mapstructure:"metrics_port"`
}

type ChainConfig struct {
	Name        string        `mapstructure:"name"`
	ChainID     int64         `mapstructure:"chain_id"`
	RPCEndpoint string        `mapstructure:"rpc_endpoint"`
	BlockTime   time.Duration `mapstructure:"block_time"`
	StartBlock  uint64        `mapstructure:"start_block"`
}

type DatabaseConfig struct {
	Host           string `mapstructure:"host"`
	Port           int    `mapstructure:"port"`
	Name           string `mapstructure:"name"`
	User           string `mapstructure:"user"`
	Password       string `mapstructure:"password"`
	SSLMode        string `mapstructure:"ssl_mode"`
	MaxConnections int32  `mapstructure:"max_connections"`
}

type ProcessorConfig struct {
	BatchSize int            `mapstructure:"batch_size"`
	Workers   int            `mapstructure:"workers"`
	FastSync  FastSyncConfig `mapstructure:"fast_sync"`
}

type FastSyncConfig struct {
	Enabled            bool `mapstructure:"enabled"`
	Threshold          int  `mapstructure:"threshold"`
	BatchSize          int  `mapstructure:"batch_size"`
	Workers            int  `mapstructure:"workers"`
	BufferSize         int  `mapstructure:"buffer_size"`
	SkipReceipts       bool `mapstructure:"skip_receipts"`
	SkipReceiptsBelow  int  `mapstructure:"skip_receipts_below"`
	RequestsPerSecond  int  `mapstructure:"requests_per_second"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetEnvPrefix("INDEXER")
	viper.AutomaticEnv()

	// Set defaults
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.metrics_port", 9090)
	viper.SetDefault("chain.block_time", "1s")
	viper.SetDefault("database.ssl_mode", "disable")
	viper.SetDefault("database.max_connections", 10)
	viper.SetDefault("processor.batch_size", 100)
	viper.SetDefault("processor.workers", 5)
	viper.SetDefault("processor.fast_sync.enabled", true)
	viper.SetDefault("processor.fast_sync.threshold", 10000)
	viper.SetDefault("processor.fast_sync.batch_size", 50)
	viper.SetDefault("processor.fast_sync.workers", 20)
	viper.SetDefault("processor.fast_sync.buffer_size", 5000)
	viper.SetDefault("processor.fast_sync.skip_receipts", true)
	viper.SetDefault("processor.fast_sync.skip_receipts_below", 10000)
	viper.SetDefault("processor.fast_sync.requests_per_second", 50)
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config: %w", err)
	}

	var config Config
	if err := viper.Unmarshal(&config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return &config, nil
}

func (c *DatabaseConfig) ConnectionString() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		c.User, c.Password, c.Host, c.Port, c.Name, c.SSLMode)
}