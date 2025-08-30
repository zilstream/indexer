package rpc

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/rpc"
)

// ZilliqaBlock represents a Zilliqa-specific block structure
type ZilliqaBlock struct {
	Number           *hexutil.Big    `json:"number"`
	Hash             common.Hash     `json:"hash"`
	ParentHash       common.Hash     `json:"parentHash"`
	Timestamp        *hexutil.Big    `json:"timestamp"`
	GasLimit         *hexutil.Big    `json:"gasLimit"`
	GasUsed          *hexutil.Big    `json:"gasUsed"`
	Transactions     json.RawMessage `json:"transactions"`
	TransactionCount int             `json:"-"`
}

// GetZilliqaBlock fetches a block using raw RPC to handle Zilliqa-specific fields
func (c *Client) GetZilliqaBlock(ctx context.Context, number uint64) (*ZilliqaBlock, error) {
	// Create raw RPC client
	rpcClient, err := rpc.DialContext(ctx, c.endpoint)
	if err != nil {
		return nil, fmt.Errorf("failed to dial RPC: %w", err)
	}
	defer rpcClient.Close()

	var result ZilliqaBlock
	err = rpcClient.CallContext(ctx, &result, "eth_getBlockByNumber", hexutil.EncodeUint64(number), false)
	if err != nil {
		return nil, fmt.Errorf("failed to get Zilliqa block %d: %w", number, err)
	}

	// Count transactions if they exist
	if len(result.Transactions) > 0 && string(result.Transactions) != "[]" {
		var txHashes []string
		if err := json.Unmarshal(result.Transactions, &txHashes); err == nil {
			result.TransactionCount = len(txHashes)
		}
	}

	return &result, nil
}

// HasUnsupportedTransactions checks if a block might have Zilliqa-specific transactions
func (c *Client) HasUnsupportedTransactions(ctx context.Context, number uint64) (bool, error) {
	block, err := c.GetZilliqaBlock(ctx, number)
	if err != nil {
		return false, err
	}
	return block.TransactionCount > 0, nil
}

// Helper to convert hexutil.Big to uint64
func hexBigToUint64(hb *hexutil.Big) uint64 {
	if hb == nil {
		return 0
	}
	return (*big.Int)(hb).Uint64()
}