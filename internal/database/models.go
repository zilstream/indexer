package database

import (
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Block represents a blockchain block in the database
type Block struct {
	Number           uint64    `db:"number"`
	Hash             string    `db:"hash"`
	ParentHash       string    `db:"parent_hash"`
	Timestamp        int64     `db:"timestamp"`
	GasLimit         uint64    `db:"gas_limit"`
	GasUsed          uint64    `db:"gas_used"`
	BaseFeePerGas    *big.Int  `db:"base_fee_per_gas"`
	TransactionCount int       `db:"transaction_count"`
	CreatedAt        time.Time `db:"created_at"`
}

// Transaction represents a blockchain transaction in the database
type Transaction struct {
	Hash             string     `db:"hash"`
	BlockNumber      uint64     `db:"block_number"`
	TransactionIndex int        `db:"transaction_index"`
	FromAddress      string     `db:"from_address"`
	ToAddress        *string    `db:"to_address"`
	Value            *big.Int   `db:"value"`
	GasPrice         *big.Int   `db:"gas_price"`
	GasLimit         uint64     `db:"gas_limit"`
	GasUsed          uint64     `db:"gas_used"`
	Nonce            uint64     `db:"nonce"`
	Input            string     `db:"input"`
	Status           int        `db:"status"`
	TransactionType  int        `db:"transaction_type"`  // 0=Legacy, 1=EIP-2930, 2=EIP-1559, 3=EIP-4844, >1000=Zilliqa
	OriginalTypeHex  *string    `db:"original_type_hex"` // Original hex type from Zilliqa (e.g., "0xdd870")
	CreatedAt        time.Time  `db:"created_at"`
}

// Transaction type constants
const (
	TxTypeLegacy    = 0
	TxTypeEIP2930   = 1
	TxTypeEIP1559   = 2
	TxTypeEIP4844   = 3
	TxTypeZilliqaBase = 1000 // Zilliqa-specific types start from 1000
)

// IndexerState represents the current state of the indexer
type IndexerState struct {
	ID              int       `db:"id"`
	ChainID         int64     `db:"chain_id"`
	LastBlockNumber uint64    `db:"last_block_number"`
	LastBlockHash   *string   `db:"last_block_hash"`
	Syncing         bool      `db:"syncing"`
	CreatedAt       time.Time `db:"created_at"`
	UpdatedAt       time.Time `db:"updated_at"`
}

// Helper functions for conversions

func HashToString(hash common.Hash) string {
	return hash.Hex()
}

func AddressToString(addr common.Address) string {
	return addr.Hex()
}

func BigIntToNumeric(value *big.Int) *string {
	if value == nil {
		return nil
	}
	str := value.String()
	return &str
}