package rpc

import (
	"context"
	"fmt"
	"math/big"
	"net/http"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/rs/zerolog"
)

// Client wraps an Ethereum client for Zilliqa EVM RPC interactions
type Client struct {
	client   *ethclient.Client
	endpoint string
	chainID  *big.Int
	logger   zerolog.Logger
	
	// Store transaction metadata for blocks with Zilliqa transactions
	txMetadata map[string][]TransactionMeta // blockHash -> transaction metadata
}

// NewClient creates a new RPC client
func NewClient(endpoint string, chainID int64, logger zerolog.Logger) (*Client, error) {
	// Create HTTP client with custom timeout
	httpClient := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			MaxIdleConnsPerHost: 10,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	// Create RPC client with custom HTTP client
	rpcClient, err := rpc.DialHTTPWithClient(endpoint, httpClient)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RPC endpoint: %w", err)
	}

	// Create eth client from RPC client
	client := ethclient.NewClient(rpcClient)

	// Verify chain ID with longer timeout
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	networkID, err := client.ChainID(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("Failed to verify chain ID, continuing anyway")
		networkID = big.NewInt(chainID)
	} else if networkID.Int64() != chainID {
		logger.Warn().
			Int64("expected", chainID).
			Int64("got", networkID.Int64()).
			Msg("Chain ID mismatch, continuing anyway")
	}

	logger.Info().
		Str("endpoint", endpoint).
		Int64("chain_id", chainID).
		Msg("Connected to RPC endpoint")

	return &Client{
		client:     client,
		endpoint:   endpoint,
		chainID:    big.NewInt(chainID),
		logger:     logger,
		txMetadata: make(map[string][]TransactionMeta),
	}, nil
}

// Close closes the RPC client connection
func (c *Client) Close() {
	c.client.Close()
	c.logger.Info().Msg("RPC client connection closed")
}

// GetLatestBlockNumber returns the latest block number
func (c *Client) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	// Create a timeout context if one isn't already set
	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	blockNumber, err := c.client.BlockNumber(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get latest block number: %w", err)
	}
	return blockNumber, nil
}

// GetBlock fetches a block by number
func (c *Client) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	// Add timeout if not present
	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	block, err := c.client.BlockByNumber(ctx, big.NewInt(int64(number)))
	if err != nil {
		return nil, fmt.Errorf("failed to get block %d: %w", number, err)
	}
	return block, nil
}

// GetBlockWithTransactions fetches a block with full transaction data
func (c *Client) GetBlockWithTransactions(ctx context.Context, number uint64) (*types.Block, error) {
	// Add timeout if not present
	_, hasDeadline := ctx.Deadline()
	if !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
	}

	// First try standard method
	block, err := c.client.BlockByNumber(ctx, big.NewInt(int64(number)))
	if err != nil {
		// If we get transaction-related errors, use raw RPC to handle Zilliqa transactions
		errStr := err.Error()
		if strings.Contains(errStr, "transaction type not supported") ||
		   strings.Contains(errStr, "invalid transaction v, r, s values") ||
		   strings.Contains(errStr, "invalid signature values") {
			c.logger.Debug().
				Uint64("block", number).
				Str("error", errStr).
				Msg("Block contains problematic transaction, using raw RPC")
			
			return c.GetBlockWithRawRPC(ctx, number)
		}
		return nil, fmt.Errorf("failed to get block with transactions %d: %w", number, err)
	}
	return block, nil
}

// GetBlockWithRawRPC fetches a block using raw RPC to handle Zilliqa-specific transactions
func (c *Client) GetBlockWithRawRPC(ctx context.Context, number uint64) (*types.Block, error) {
	// Create raw RPC client
	rpcClient, err := rpc.DialContext(ctx, c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to dial raw RPC: %w", err)
	}
	defer rpcClient.Close()

	var rawBlock RawBlock
	err = rpcClient.CallContext(ctx, &rawBlock, "eth_getBlockByNumber", hexutil.EncodeUint64(number), true)
	if err != nil {
		return nil, fmt.Errorf("failed to get raw block %d: %w", number, err)
	}

	// Convert to standard block with metadata
	block, metadata := rawBlock.ToEthBlockWithMeta()
	
	// Store metadata if there are Zilliqa transactions
	hasZilliqaTx := false
	for _, meta := range metadata {
		if meta.IsZilliqaType {
			hasZilliqaTx = true
			break
		}
	}
	
	if hasZilliqaTx {
		c.txMetadata[block.Hash().Hex()] = metadata
		c.logger.Info().
			Uint64("block", number).
			Int("transactions", len(block.Transactions())).
			Int("zilliqa_txs", countZilliqaTxs(metadata)).
			Msg("Block contains pre-EVM Zilliqa transactions")
	}
	
	c.logger.Debug().
		Uint64("block", number).
		Int("transactions", len(block.Transactions())).
		Msg("Successfully parsed block with raw RPC")
	
	return block, nil
}

// GetTransactionMetadata returns metadata for transactions in a block
func (c *Client) GetTransactionMetadata(blockHash string) []TransactionMeta {
	return c.txMetadata[blockHash]
}

// Helper function to count Zilliqa transactions
func countZilliqaTxs(metadata []TransactionMeta) int {
	count := 0
	for _, meta := range metadata {
		if meta.IsZilliqaType {
			count++
		}
	}
	return count
}

// GetTransactionReceipt fetches a transaction receipt
func (c *Client) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	receipt, err := c.client.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get transaction receipt %s: %w", txHash.Hex(), err)
	}
	return receipt, nil
}

// GetBlockReceipts fetches all transaction receipts for a block
func (c *Client) GetBlockReceipts(ctx context.Context, blockNumber uint64) ([]*types.Receipt, error) {
	// First try to get the block to see if it has transactions
	block, err := c.GetBlockWithTransactions(ctx, blockNumber)
	if err != nil {
		// If the block fetch failed completely, return error
		return nil, fmt.Errorf("failed to get block %d: %w", blockNumber, err)
	}

	receipts := make([]*types.Receipt, 0, len(block.Transactions()))
	for _, tx := range block.Transactions() {
		receipt, err := c.GetTransactionReceipt(ctx, tx.Hash())
		if err != nil {
			c.logger.Warn().
				Str("tx_hash", tx.Hash().Hex()).
				Uint64("block", blockNumber).
				Err(err).
				Msg("Failed to get transaction receipt")
			// For Zilliqa pre-EVM transactions, create a minimal receipt
			// This allows processing to continue
			receipt = &types.Receipt{
				TxHash:      tx.Hash(),
				GasUsed:     0, // Pre-EVM transactions may not have gas info
				Status:      types.ReceiptStatusSuccessful,
				BlockNumber: big.NewInt(int64(blockNumber)),
			}
		}
		receipts = append(receipts, receipt)
	}

	return receipts, nil
}

// GetLogs fetches logs matching the given filter query
func (c *Client) GetLogs(ctx context.Context, query ethereum.FilterQuery) ([]types.Log, error) {
	logs, err := c.client.FilterLogs(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to get logs: %w", err)
	}
	return logs, nil
}

// GetEndpoint returns the RPC endpoint URL
func (c *Client) GetEndpoint() string {
	return c.endpoint
}

// IsConnected checks if the client is connected to the RPC endpoint
func (c *Client) IsConnected(ctx context.Context) bool {
	_, err := c.client.BlockNumber(ctx)
	return err == nil
}

// Retry wraps a function with retry logic
func (c *Client) Retry(ctx context.Context, fn func() error, maxRetries int) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		if err = fn(); err == nil {
			return nil
		}

		if i < maxRetries-1 {
			waitTime := time.Duration(i+1) * time.Second
			c.logger.Warn().
				Err(err).
				Int("attempt", i+1).
				Dur("wait", waitTime).
				Msg("Retrying RPC call")

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(waitTime):
				continue
			}
		}
	}
	return fmt.Errorf("max retries exceeded: %w", err)
}