package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
)

// BatchClient provides batch RPC operations for fast syncing
type BatchClient struct {
	endpoint   string
	httpClient *http.Client
	logger     zerolog.Logger
	
	// Rate limiting
	requestsPerSecond int
	rateLimiter       *time.Ticker
	
	// Metrics
	mu              sync.RWMutex
	totalRequests   int64
	totalBatchCalls int64
	avgBatchSize    float64
}

// BatchRequest represents a single request in a batch
type BatchRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      int             `json:"id"`
}

// BatchResponse represents a single response in a batch
type BatchResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  json.RawMessage `json:"result"`
	Error   *RPCError       `json:"error"`
	ID      int             `json:"id"`
}

// RPCError represents an RPC error
type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// BlockRange represents a range of blocks to fetch
type BlockRange struct {
	Start uint64
	End   uint64
}

// NewBatchClient creates a new batch RPC client
func NewBatchClient(endpoint string, requestsPerSecond int, logger zerolog.Logger) *BatchClient {
	return &BatchClient{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 100,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		logger:            logger,
		requestsPerSecond: requestsPerSecond,
		rateLimiter:       time.NewTicker(time.Second / time.Duration(requestsPerSecond)),
	}
}

// Close closes the batch client
func (c *BatchClient) Close() {
	c.rateLimiter.Stop()
}

// GetBlockBatch fetches multiple blocks in a single batch request
func (c *BatchClient) GetBlockBatch(ctx context.Context, blockNumbers []uint64) ([]*types.Block, error) {
	if len(blockNumbers) == 0 {
		return nil, nil
	}
	
	// Wait for rate limiter
	<-c.rateLimiter.C
	
	// Build batch request
	requests := make([]BatchRequest, len(blockNumbers))
	for i, num := range blockNumbers {
		params, _ := json.Marshal([]interface{}{
			hexutil.EncodeUint64(num),
			true, // Include transactions
		})
		requests[i] = BatchRequest{
			JSONRPC: "2.0",
			Method:  "eth_getBlockByNumber",
			Params:  params,
			ID:      i,
		}
	}
	
	// Execute batch request
	responses, err := c.executeBatch(ctx, requests)
	if err != nil {
		return nil, err
	}
	
	// Parse responses
	blocks := make([]*types.Block, 0, len(responses))
	for i, resp := range responses {
		if resp.Error != nil {
			c.logger.Warn().
				Uint64("block", blockNumbers[i]).
				Str("error", resp.Error.Message).
				Msg("Failed to fetch block in batch")
			continue
		}
		
		// Try to parse as raw block first (for Zilliqa transactions)
		var rawBlock RawBlock
		if err := json.Unmarshal(resp.Result, &rawBlock); err != nil {
			c.logger.Error().
				Uint64("block", blockNumbers[i]).
				Err(err).
				Msg("Failed to parse block")
			continue
		}
		
		block := rawBlock.ToEthBlock()
		if block != nil {
			blocks = append(blocks, block)
		}
	}
	
	// Update metrics
	c.updateMetrics(len(blockNumbers))
	
	return blocks, nil
}

// GetBlockRangeFast fetches a range of blocks using optimized batching
func (c *BatchClient) GetBlockRangeFast(ctx context.Context, start, end uint64, batchSize int) ([]*types.Block, error) {
	if start > end {
		return nil, fmt.Errorf("invalid range: start %d > end %d", start, end)
	}
	
	// Create block numbers for this range
	nums := make([]uint64, 0, end-start+1)
	for n := start; n <= end; n++ {
		nums = append(nums, n)
	}
	
	// Fetch all blocks in this range
	return c.GetBlockBatch(ctx, nums)
}

// GetReceiptBatch fetches multiple receipts in a single batch request
func (c *BatchClient) GetReceiptBatch(ctx context.Context, txHashes []common.Hash) ([]*types.Receipt, error) {
	if len(txHashes) == 0 {
		return nil, nil
	}
	
	// Wait for rate limiter
	<-c.rateLimiter.C
	
	// Build batch request
	requests := make([]BatchRequest, len(txHashes))
	for i, hash := range txHashes {
		params, _ := json.Marshal([]interface{}{hash.Hex()})
		requests[i] = BatchRequest{
			JSONRPC: "2.0",
			Method:  "eth_getTransactionReceipt",
			Params:  params,
			ID:      i,
		}
	}
	
	// Execute batch request
	responses, err := c.executeBatch(ctx, requests)
	if err != nil {
		return nil, err
	}
	
	// Parse responses
	receipts := make([]*types.Receipt, len(responses))
	for i, resp := range responses {
		if resp.Error != nil {
			// Create minimal receipt for failed requests
			receipts[i] = &types.Receipt{
				TxHash: txHashes[i],
				Status: types.ReceiptStatusSuccessful,
			}
			continue
		}
		
		var receipt types.Receipt
		if err := json.Unmarshal(resp.Result, &receipt); err != nil {
			c.logger.Warn().
				Err(err).
				Str("hash", txHashes[i].Hex()).
				Msg("Failed to parse receipt")
			// Create minimal receipt
			receipts[i] = &types.Receipt{
				TxHash: txHashes[i],
				Status: types.ReceiptStatusSuccessful,
			}
		} else {
			receipts[i] = &receipt
		}
	}
	
	return receipts, nil
}

// GetLogs fetches logs for a block range using eth_getLogs
func (c *BatchClient) GetLogs(ctx context.Context, fromBlock, toBlock uint64, addresses []common.Address) ([]types.Log, error) {
	// Build filter
	filter := map[string]interface{}{
		"fromBlock": hexutil.EncodeUint64(fromBlock),
		"toBlock":   hexutil.EncodeUint64(toBlock),
	}
	
	if len(addresses) > 0 {
		addrStrs := make([]string, len(addresses))
		for i, addr := range addresses {
			addrStrs[i] = addr.Hex()
		}
		filter["address"] = addrStrs
	}
	
	params, _ := json.Marshal([]interface{}{filter})
	
	// Wait for rate limiter
	<-c.rateLimiter.C
	
	// Execute single request (eth_getLogs doesn't support batch well)
	request := BatchRequest{
		JSONRPC: "2.0",
		Method:  "eth_getLogs",
		Params:  params,
		ID:      1,
	}
	
	responses, err := c.executeBatch(ctx, []BatchRequest{request})
	if err != nil {
		return nil, err
	}
	
	if len(responses) == 0 || responses[0].Error != nil {
		if responses[0].Error != nil {
			return nil, fmt.Errorf("eth_getLogs error: %s", responses[0].Error.Message)
		}
		return nil, fmt.Errorf("no response from eth_getLogs")
	}
	
	var logs []types.Log
	if err := json.Unmarshal(responses[0].Result, &logs); err != nil {
		return nil, fmt.Errorf("failed to parse logs: %w", err)
	}
	
	return logs, nil
}

// executeBatch executes a batch RPC request
func (c *BatchClient) executeBatch(ctx context.Context, requests []BatchRequest) ([]BatchResponse, error) {
	// Marshal requests
	body, err := json.Marshal(requests)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal batch request: %w", err)
	}
	
	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	// Execute request
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute batch request: %w", err)
	}
	defer resp.Body.Close()
	
	// Read response
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}
	
	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("batch request failed with status %d: %s", resp.StatusCode, string(respBody))
	}
	
	// Parse response
	var responses []BatchResponse
	if err := json.Unmarshal(respBody, &responses); err != nil {
		return nil, fmt.Errorf("failed to parse batch response: %w", err)
	}
	
	return responses, nil
}

// updateMetrics updates client metrics
func (c *BatchClient) updateMetrics(batchSize int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	c.totalRequests++
	c.totalBatchCalls += int64(batchSize)
	
	// Update moving average
	if c.avgBatchSize == 0 {
		c.avgBatchSize = float64(batchSize)
	} else {
		c.avgBatchSize = (c.avgBatchSize*0.9) + (float64(batchSize)*0.1)
	}
}

// GetMetrics returns client metrics
func (c *BatchClient) GetMetrics() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	return map[string]interface{}{
		"total_requests":    c.totalRequests,
		"total_batch_calls": c.totalBatchCalls,
		"avg_batch_size":    c.avgBatchSize,
		"efficiency":        float64(c.totalBatchCalls) / float64(c.totalRequests),
	}
}

// EstimateOptimalBatchSize estimates the optimal batch size based on response times
func (c *BatchClient) EstimateOptimalBatchSize(ctx context.Context) (int, error) {
	testSizes := []int{1, 5, 10, 20, 50, 100}
	bestSize := 10
	bestRate := 0.0
	
	// Test latest block to get a baseline
	latestBlock, err := c.getLatestBlockNumber(ctx)
	if err != nil {
		return bestSize, err
	}
	
	for _, size := range testSizes {
		start := time.Now()
		
		// Test fetching 'size' blocks
		nums := make([]uint64, size)
		for i := 0; i < size; i++ {
			nums[i] = latestBlock - uint64(i)
		}
		
		_, err := c.GetBlockBatch(ctx, nums)
		if err != nil {
			c.logger.Warn().
				Int("size", size).
				Err(err).
				Msg("Batch size test failed")
			continue
		}
		
		elapsed := time.Since(start)
		rate := float64(size) / elapsed.Seconds()
		
		c.logger.Info().
			Int("batch_size", size).
			Dur("elapsed", elapsed).
			Float64("blocks_per_sec", rate).
			Msg("Batch size test")
		
		if rate > bestRate {
			bestRate = rate
			bestSize = size
		}
		
		// If we're getting errors or slow responses, don't test larger sizes
		if elapsed > 10*time.Second {
			break
		}
	}
	
	c.logger.Info().
		Int("optimal_size", bestSize).
		Float64("blocks_per_sec", bestRate).
		Msg("Optimal batch size determined")
	
	return bestSize, nil
}

// getLatestBlockNumber gets the latest block number
func (c *BatchClient) getLatestBlockNumber(ctx context.Context) (uint64, error) {
	params, _ := json.Marshal([]interface{}{})
	request := BatchRequest{
		JSONRPC: "2.0",
		Method:  "eth_blockNumber",
		Params:  params,
		ID:      1,
	}
	
	responses, err := c.executeBatch(ctx, []BatchRequest{request})
	if err != nil {
		return 0, err
	}
	
	if len(responses) == 0 || responses[0].Error != nil {
		return 0, fmt.Errorf("failed to get latest block number")
	}
	
	var blockNumHex string
	if err := json.Unmarshal(responses[0].Result, &blockNumHex); err != nil {
		return 0, err
	}
	
	blockNum, err := hexutil.DecodeUint64(blockNumHex)
	if err != nil {
		return 0, err
	}
	
	return blockNum, nil
}