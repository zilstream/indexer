package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Server      ServerConfig      `mapstructure:"server"`
	Chain       ChainConfig       `mapstructure:"chain"`
	Database    DatabaseConfig    `mapstructure:"database"`
	Processor   ProcessorConfig   `mapstructure:"processor"`
	Logging     LoggingConfig     `mapstructure:"logging"`
	Bootstrap   BootstrapConfig   `mapstructure:"bootstrap"`
	Centrifugo  CentrifugoConfig  `mapstructure:"centrifugo"`
}

type ServerConfig struct {
	Port        int  `mapstructure:"port"`
	MetricsPort int  `mapstructure:"metrics_port"`
	RunIndexer  bool `mapstructure:"run_indexer"`
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
	BatchSize         int           `mapstructure:"batch_size"`
	Workers           int           `mapstructure:"workers"`
	RequestsPerSecond int           `mapstructure:"requests_per_second"`
	MaxRetries        int           `mapstructure:"max_retries"`
	RetryDelay        time.Duration `mapstructure:"retry_delay"`
	PricePollInterval time.Duration `mapstructure:"price_poll_interval"`
}

type LoggingConfig struct {
	Level  string `mapstructure:"level"`
	Format string `mapstructure:"format"`
}

type BootstrapConfig struct {
	AutoMigrate bool               `mapstructure:"auto_migrate"`
	ZILPrices   ZILPricesBootstrap `mapstructure:"zil_prices"`
}

type ZILPricesBootstrap struct {
	AutoLoad  bool   `mapstructure:"auto_load"`
	CSVPath   string `mapstructure:"csv_path"`
	Source    string `mapstructure:"source"`
	BatchSize int    `mapstructure:"batch_size"`
}

type CentrifugoConfig struct {
	APIURL    string `mapstructure:"api_url"`
	APIKey    string `mapstructure:"api_key"`
	WsURL     string `mapstructure:"ws_url"`
	JWTSecret string `mapstructure:"jwt_secret"`
	Enabled   bool   `mapstructure:"enabled"`
}

func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetEnvPrefix("INDEXER")
	// Allow env vars like INDEXER_DATABASE_HOST to override database.host
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()

	// Set defaults
	viper.SetDefault("server.port", 8080)
	viper.SetDefault("server.metrics_port", 9090)
	viper.SetDefault("server.run_indexer", true)
	viper.SetDefault("chain.block_time", "1s")
	viper.SetDefault("database.ssl_mode", "disable")
	viper.SetDefault("database.max_connections", 10)
	viper.SetDefault("processor.batch_size", 100)
	viper.SetDefault("processor.workers", 5)
	viper.SetDefault("processor.requests_per_second", 50)
	viper.SetDefault("processor.max_retries", 3)
	viper.SetDefault("processor.retry_delay", "1s")
	viper.SetDefault("processor.price_poll_interval", "5m")
	viper.SetDefault("logging.level", "info")
	viper.SetDefault("logging.format", "json")
	viper.SetDefault("bootstrap.auto_migrate", true)
	viper.SetDefault("bootstrap.zil_prices.auto_load", true)
	viper.SetDefault("bootstrap.zil_prices.csv_path", "data/zilliqa_historical_prices.csv")
	viper.SetDefault("bootstrap.zil_prices.source", "bootstrap_csv")
	viper.SetDefault("bootstrap.zil_prices.batch_size", 50_000)
	viper.SetDefault("centrifugo.enabled", false)
	viper.SetDefault("centrifugo.api_url", "http://localhost:8001/api")
	viper.SetDefault("centrifugo.ws_url", "ws://localhost:8001/connection/websocket")

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
