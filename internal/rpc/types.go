package rpc

import (
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
)

// RawTransaction represents a raw transaction from RPC that might have non-standard types
type RawTransaction struct {
	Type             *hexutil.Big    `json:"type"`
	Hash             common.Hash     `json:"hash"`
	Nonce            hexutil.Uint64  `json:"nonce"`
	BlockHash        *common.Hash    `json:"blockHash"`
	BlockNumber      *hexutil.Big    `json:"blockNumber"`
	TransactionIndex *hexutil.Uint64 `json:"transactionIndex"`
	From             common.Address  `json:"from"`
	To               *common.Address `json:"to"`
	Value            *hexutil.Big    `json:"value"`
	Gas              hexutil.Uint64  `json:"gas"`
	GasPrice         *hexutil.Big    `json:"gasPrice"`
	Input            hexutil.Bytes   `json:"input"`
	V                *hexutil.Big    `json:"v"`
	R                *hexutil.Big    `json:"r"`
	S                *hexutil.Big    `json:"s"`
	
	// Zilliqa-specific fields
	OriginalType     uint64          `json:"-"` // Store the original type value
	IsZilliqaType    bool            `json:"-"` // Flag for Zilliqa-specific transaction
}

// RawBlock represents a block with raw transactions
type RawBlock struct {
	Number           *hexutil.Big     `json:"number"`
	Hash             common.Hash      `json:"hash"`
	ParentHash       common.Hash      `json:"parentHash"`
	Timestamp        *hexutil.Big     `json:"timestamp"`
	GasLimit         *hexutil.Big     `json:"gasLimit"`
	GasUsed          *hexutil.Big     `json:"gasUsed"`
	Transactions     []RawTransaction `json:"transactions"`
	TransactionsRoot common.Hash      `json:"transactionsRoot"`
	StateRoot        common.Hash      `json:"stateRoot"`
	ReceiptsRoot     common.Hash      `json:"receiptsRoot"`
	Miner            common.Address   `json:"miner"`
	Difficulty       *hexutil.Big     `json:"difficulty"`
	TotalDifficulty  *hexutil.Big     `json:"totalDifficulty"`
	Size             *hexutil.Big     `json:"size"`
	ExtraData        hexutil.Bytes    `json:"extraData"`
	Nonce            types.BlockNonce `json:"nonce"`
	MixHash          common.Hash      `json:"mixHash"`
	BaseFeePerGas    *hexutil.Big     `json:"baseFeePerGas"`
}

// ToEthBlock converts RawBlock to types.Block, handling Zilliqa-specific transactions
func (rb *RawBlock) ToEthBlock() *types.Block {
	// Create header
	header := &types.Header{
		ParentHash:  rb.ParentHash,
		Root:        rb.StateRoot,
		TxHash:      rb.TransactionsRoot,
		ReceiptHash: rb.ReceiptsRoot,
		Number:      (*big.Int)(rb.Number),
		GasLimit:    uint64((*big.Int)(rb.GasLimit).Int64()),
		GasUsed:     uint64((*big.Int)(rb.GasUsed).Int64()),
		Time:        uint64((*big.Int)(rb.Timestamp).Int64()),
		Extra:       rb.ExtraData,
		MixDigest:   rb.MixHash,
		Nonce:       rb.Nonce,
		Difficulty:  (*big.Int)(rb.Difficulty),
	}

	if rb.BaseFeePerGas != nil {
		header.BaseFee = (*big.Int)(rb.BaseFeePerGas)
	}

	// Convert transactions
	transactions := make([]*types.Transaction, 0)
	for _, rawTx := range rb.Transactions {
		if tx := rawTx.ToTransaction(); tx != nil {
			transactions = append(transactions, tx)
		}
	}

	// Use NewBlockWithHeader when we have no uncles (which Zilliqa doesn't have)
	block := types.NewBlockWithHeader(header).WithBody(transactions, nil)
	
	return block
}

// ToEthBlockWithMeta converts RawBlock to types.Block with transaction metadata
func (rb *RawBlock) ToEthBlockWithMeta() (*types.Block, []TransactionMeta) {
	// Create header
	header := &types.Header{
		ParentHash:  rb.ParentHash,
		Root:        rb.StateRoot,
		TxHash:      rb.TransactionsRoot,
		ReceiptHash: rb.ReceiptsRoot,
		Number:      (*big.Int)(rb.Number),
		GasLimit:    uint64((*big.Int)(rb.GasLimit).Int64()),
		GasUsed:     uint64((*big.Int)(rb.GasUsed).Int64()),
		Time:        uint64((*big.Int)(rb.Timestamp).Int64()),
		Extra:       rb.ExtraData,
		MixDigest:   rb.MixHash,
		Nonce:       rb.Nonce,
		Difficulty:  (*big.Int)(rb.Difficulty),
	}

	if rb.BaseFeePerGas != nil {
		header.BaseFee = (*big.Int)(rb.BaseFeePerGas)
	}

	// Convert transactions and collect metadata
	transactions := make([]*types.Transaction, 0)
	metadata := make([]TransactionMeta, 0)
	
	for _, rawTx := range rb.Transactions {
		if tx := rawTx.ToTransaction(); tx != nil {
			transactions = append(transactions, tx)
			
			// Create metadata for this transaction
			meta := TransactionMeta{
				Hash:          rawTx.Hash,
				From:          rawTx.From,
				IsZilliqaType: rawTx.IsZilliqaType,
				OriginalType:  rawTx.OriginalType,
			}
			if rawTx.IsZilliqaType && rawTx.Type != nil {
				meta.OriginalTypeHex = (*hexutil.Big)(rawTx.Type).String()
			}
			metadata = append(metadata, meta)
		}
	}

	// Use NewBlockWithHeader when we have no uncles (which Zilliqa doesn't have)
	block := types.NewBlockWithHeader(header).WithBody(transactions, nil)
	
	return block, metadata
}

// TransactionMeta contains metadata about a transaction
type TransactionMeta struct {
	Hash            common.Hash     // Original transaction hash from RPC
	From            common.Address  // Original from address from RPC  
	IsZilliqaType   bool
	OriginalType    uint64
	OriginalTypeHex string
}

// BlockWithMeta contains a block with transaction metadata
type BlockWithMeta struct {
	Block    *types.Block
	TxMeta   []TransactionMeta
}

// ToTransaction converts RawTransaction to types.Transaction
func (rt *RawTransaction) ToTransaction() *types.Transaction {
	// Check transaction type
	var txType uint64
	if rt.Type != nil {
		txType = (*big.Int)(rt.Type).Uint64()
		rt.OriginalType = txType
	}

	// For Zilliqa pre-EVM types (> 3), mark as Zilliqa transaction but treat as legacy for processing
	if txType > 3 {
		rt.IsZilliqaType = true
		// These are pre-EVM Zilliqa transactions, we'll process them as legacy EVM transactions
		// but keep track of their original type for historical accuracy
		txType = 0 // Process as legacy transaction
	}

	var value *big.Int
	if rt.Value != nil {
		value = (*big.Int)(rt.Value)
	} else {
		value = big.NewInt(0)
	}

	var gasPrice *big.Int
	if rt.GasPrice != nil {
		gasPrice = (*big.Int)(rt.GasPrice)
	} else {
		gasPrice = big.NewInt(0)
	}

	// For Zilliqa pre-EVM transactions, V, R, S might be nil or invalid
	// Set default values if they're missing
	var v, r, s *big.Int
	if rt.V != nil {
		v = (*big.Int)(rt.V)
	} else {
		v = big.NewInt(0)
	}
	if rt.R != nil {
		r = (*big.Int)(rt.R)
	} else {
		r = big.NewInt(0)
	}
	if rt.S != nil {
		s = (*big.Int)(rt.S)
	} else {
		s = big.NewInt(1) // Set to 1 instead of 0 to avoid invalid signature error
	}

	// Create legacy transaction structure
	// Pre-EVM Zilliqa transactions can be processed as legacy EVM transactions
	return types.NewTx(&types.LegacyTx{
		Nonce:    uint64(rt.Nonce),
		To:       rt.To,
		Value:    value,
		Gas:      uint64(rt.Gas),
		GasPrice: gasPrice,
		Data:     rt.Input,
		V:        v,
		R:        r,
		S:        s,
	})
}

// UnmarshalJSON implements json.Unmarshaler for RawBlock
func (rb *RawBlock) UnmarshalJSON(data []byte) error {
	type rawBlock RawBlock
	var temp struct {
		rawBlock
		Transactions json.RawMessage `json:"transactions"`
	}

	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	*rb = RawBlock(temp.rawBlock)

	// Try to unmarshal transactions
	// If it's an array of hashes, ignore
	// If it's an array of objects, parse them
	var txHashes []string
	if err := json.Unmarshal(temp.Transactions, &txHashes); err == nil {
		// Just transaction hashes, not full transactions
		return nil
	}

	// Try to unmarshal as full transactions
	return json.Unmarshal(temp.Transactions, &rb.Transactions)
}