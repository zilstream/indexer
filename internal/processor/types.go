package processor

import (
	"github.com/ethereum/go-ethereum/core/types"
)

// TransactionWithMeta wraps a transaction with additional metadata
type TransactionWithMeta struct {
	Transaction     *types.Transaction
	IsZilliqaType   bool   // True if this is a pre-EVM Zilliqa transaction
	OriginalType    uint64 // Original transaction type value
	OriginalTypeHex string // Original type in hex format (e.g., "0xdd870")
}

// BlockWithMeta wraps a block with transaction metadata
type BlockWithMeta struct {
	Block        *types.Block
	Transactions []TransactionWithMeta
}