package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
	"golang.org/x/sync/semaphore"
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

// ZilliqaReceipt represents a Zilliqa transaction receipt with optional fields
type ZilliqaReceipt struct {
	TxHash            common.Hash     `json:"transactionHash"`
	TxIndex           hexutil.Uint64  `json:"transactionIndex"`
	BlockHash         common.Hash     `json:"blockHash"`
	BlockNumber       *hexutil.Big    `json:"blockNumber"`
	From              common.Address  `json:"from"`
	To                *common.Address `json:"to"`
	CumulativeGasUsed *hexutil.Big    `json:"cumulativeGasUsed,omitempty"` // Optional for Zilliqa
	GasUsed           *hexutil.Big    `json:"gasUsed"`
	ContractAddress   *common.Address `json:"contractAddress"`
	Logs              []*types.Log    `json:"logs"`
	LogsBloom         types.Bloom     `json:"logsBloom"`
	Status            hexutil.Uint64  `json:"status"`
}

// ToStandardReceipt converts a ZilliqaReceipt to a standard types.Receipt
func (zr *ZilliqaReceipt) ToStandardReceipt() *types.Receipt {
	receipt := &types.Receipt{
		Type:             types.LegacyTxType,
		TxHash:           zr.TxHash,
		GasUsed:          uint64(0),
		BlockHash:        zr.BlockHash,
		TransactionIndex: uint(zr.TxIndex),
		Logs:             zr.Logs,
		Bloom:            zr.LogsBloom,
		Status:           uint64(zr.Status),
	}
	
	// Set contract address if it exists
	if zr.ContractAddress != nil {
		receipt.ContractAddress = *zr.ContractAddress
	}
	
	// Set block number
	if zr.BlockNumber != nil {
		receipt.BlockNumber = (*big.Int)(zr.BlockNumber)
	}
	
	// Set gas used
	if zr.GasUsed != nil {
		receipt.GasUsed = (*big.Int)(zr.GasUsed).Uint64()
	}
	
	// Set cumulative gas used (use gas used if cumulative not available)
	if zr.CumulativeGasUsed != nil {
		receipt.CumulativeGasUsed = (*big.Int)(zr.CumulativeGasUsed).Uint64()
	} else {
		receipt.CumulativeGasUsed = receipt.GasUsed // Fallback for Zilliqa
	}
	
	return receipt
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

// GetBlockBatchRaw fetches multiple blocks and returns raw RPC data
func (c *BatchClient) GetBlockBatchRaw(ctx context.Context, blockNumbers []uint64) ([]*RawBlock, error) {
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
	blocks := make([]*RawBlock, 0, len(responses))
	for i, resp := range responses {
		if resp.Error != nil {
			c.logger.Warn().
				Uint64("block", blockNumbers[i]).
				Str("error", resp.Error.Message).
				Msg("Failed to fetch block in batch")
			continue
		}
		
		// Parse as raw block to preserve all RPC fields
		var rawBlock RawBlock
		if err := json.Unmarshal(resp.Result, &rawBlock); err != nil {
			c.logger.Error().
				Uint64("block", blockNumbers[i]).
				Err(err).
				Msg("Failed to parse block")
			continue
		}
		
		blocks = append(blocks, &rawBlock)
	}
	
	// Update metrics
	c.updateMetrics(len(blockNumbers))
	
	return blocks, nil
}

// GetBlockRangeFast fetches a range of blocks using optimized batching
func (c *BatchClient) GetBlockRangeFast(ctx context.Context, start, end uint64, batchSize int) ([]*RawBlock, error) {
	if start > end {
		return nil, fmt.Errorf("invalid range: start %d > end %d", start, end)
	}
	
	// Create block numbers for this range
	nums := make([]uint64, 0, end-start+1)
	for n := start; n <= end; n++ {
		nums = append(nums, n)
	}
	
	// Fetch all blocks in this range
	return c.GetBlockBatchRaw(ctx, nums)
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
		
		// Try parsing as Zilliqa receipt first
		var zilliqaReceipt ZilliqaReceipt
		if err := json.Unmarshal(resp.Result, &zilliqaReceipt); err != nil {
			c.logger.Warn().
				Err(err).
				Str("hash", txHashes[i].Hex()).
				Msg("Failed to parse Zilliqa receipt")
			// Create minimal receipt
			receipts[i] = &types.Receipt{
				TxHash: txHashes[i],
				Status: types.ReceiptStatusSuccessful,
			}
		} else {
			// Convert Zilliqa receipt to standard receipt
			receipts[i] = zilliqaReceipt.ToStandardReceipt()
		}
	}
	
	return receipts, nil
}

// GetLogs fetches logs for a block range using eth_getLogs with chunking for large ranges
func (c *BatchClient) GetLogs(ctx context.Context, fromBlock, toBlock uint64, addresses []common.Address) ([]types.Log, error) {
	blockRange := toBlock - fromBlock + 1

	// For large ranges, split into chunks to avoid RPC limits
	maxRangePerRequest := uint64(2000) // Conservative limit for most RPCs
	if blockRange <= maxRangePerRequest {
		return c.getLogsRange(ctx, fromBlock, toBlock, addresses)
	}

	// Split into chunks and process in parallel
	var allLogs []types.Log
	numChunks := (blockRange + maxRangePerRequest - 1) / maxRangePerRequest

	// Use semaphore to limit concurrent requests
	maxConcurrency := int64(4) // Conservative concurrency for getLogs
	if numChunks < 4 {
		maxConcurrency = int64(numChunks)
	}
	sem := semaphore.NewWeighted(maxConcurrency)

	var mu sync.Mutex
	var wg sync.WaitGroup
	var fetchErr error

	for start := fromBlock; start <= toBlock; start += maxRangePerRequest {
		end := start + maxRangePerRequest - 1
		if end > toBlock {
			end = toBlock
		}

		wg.Add(1)
		go func(chunkStart, chunkEnd uint64) {
			defer wg.Done()

			// Acquire semaphore
			if err := sem.Acquire(ctx, 1); err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = err
				}
				mu.Unlock()
				return
			}
			defer sem.Release(1)

			chunkLogs, err := c.getLogsRange(ctx, chunkStart, chunkEnd, addresses)
			if err != nil {
				mu.Lock()
				if fetchErr == nil {
					fetchErr = fmt.Errorf("failed to get logs for range %d-%d: %w", chunkStart, chunkEnd, err)
				}
				mu.Unlock()
				return
			}

			mu.Lock()
			allLogs = append(allLogs, chunkLogs...)
			mu.Unlock()
		}(start, end)
	}

	wg.Wait()
	if fetchErr != nil {
		return nil, fetchErr
	}

	c.logger.Debug().
		Uint64("from", fromBlock).
		Uint64("to", toBlock).
		Int("total_logs", len(allLogs)).
		Int("chunks", int(numChunks)).
		Msg("Fetched logs in chunks")

	return allLogs, nil
}

// getLogsRange fetches logs for a single block range
func (c *BatchClient) getLogsRange(ctx context.Context, fromBlock, toBlock uint64, addresses []common.Address) ([]types.Log, error) {
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

	startTime := time.Now()

	// Execute single request
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
		if len(responses) > 0 && responses[0].Error != nil {
			return nil, fmt.Errorf("eth_getLogs error: %s", responses[0].Error.Message)
		}
		return nil, fmt.Errorf("no response from eth_getLogs")
	}

	var logs []types.Log
	if err := json.Unmarshal(responses[0].Result, &logs); err != nil {
		return nil, fmt.Errorf("failed to parse logs: %w", err)
	}

	c.logger.Debug().
		Uint64("from", fromBlock).
		Uint64("to", toBlock).
		Int("logs", len(logs)).
		Dur("elapsed", time.Since(startTime)).
		Msg("Fetched logs range")

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
		
		_, err := c.GetBlockBatchRaw(ctx, nums)
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