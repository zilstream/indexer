package processor

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
)

// TransactionProcessor handles the processing of transactions
type TransactionProcessor struct {
	rpcClient *rpc.Client
	db        *database.Database
	txRepo    *database.TransactionRepository
	logger    zerolog.Logger
}

// NewTransactionProcessor creates a new transaction processor
func NewTransactionProcessor(rpcClient *rpc.Client, db *database.Database, logger zerolog.Logger) *TransactionProcessor {
	return &TransactionProcessor{
		rpcClient: rpcClient,
		db:        db,
		txRepo:    database.NewTransactionRepository(db),
		logger:    logger,
	}
}

// ProcessTransactions processes all transactions in a block
func (p *TransactionProcessor) ProcessTransactions(ctx context.Context, block *types.Block) error {
	if len(block.Transactions()) == 0 {
		return nil
	}

	// Fetch receipts for all transactions
	receipts, err := p.rpcClient.GetBlockReceipts(ctx, block.NumberU64())
	if err != nil {
		return fmt.Errorf("failed to get block receipts: %w", err)
	}

	// Convert transactions to database models
	transactions := p.convertTransactions(block, receipts)

	// Insert transactions in batch
	if err := p.txRepo.InsertBatch(ctx, transactions); err != nil {
		return fmt.Errorf("failed to insert transactions: %w", err)
	}

	p.logger.Debug().
		Uint64("block", block.NumberU64()).
		Int("count", len(transactions)).
		Msg("Transactions processed")

	return nil
}

// convertTransactions converts Ethereum transactions to database models
func (p *TransactionProcessor) convertTransactions(block *types.Block, receipts []*types.Receipt) []*database.Transaction {
	transactions := make([]*database.Transaction, 0, len(block.Transactions()))
	receiptMap := make(map[common.Hash]*types.Receipt)

	// Create receipt map for quick lookup
	for _, receipt := range receipts {
		receiptMap[receipt.TxHash] = receipt
	}

	// Process each transaction
	for i, tx := range block.Transactions() {
		dbTx := p.convertTransaction(tx, block.NumberU64(), i)

		// Add receipt data if available
		if receipt, ok := receiptMap[tx.Hash()]; ok {
			dbTx.GasUsed = receipt.GasUsed
			dbTx.Status = int(receipt.Status)
		}

		transactions = append(transactions, dbTx)
	}

	return transactions
}

// convertTransactionsWithMeta converts transactions with Zilliqa metadata
func (p *TransactionProcessor) convertTransactionsWithMeta(block *types.Block, receipts []*types.Receipt, metadata []rpc.TransactionMeta) []*database.Transaction {
	transactions := make([]*database.Transaction, 0, len(block.Transactions()))
	receiptMap := make(map[common.Hash]*types.Receipt)

	// Create receipt map for quick lookup
	for _, receipt := range receipts {
		receiptMap[receipt.TxHash] = receipt
	}

	// Process each transaction
	for i, tx := range block.Transactions() {
		var meta *rpc.TransactionMeta
		if i < len(metadata) {
			meta = &metadata[i]
		}
		
		dbTx := p.convertTransactionWithMeta(tx, block.NumberU64(), i, meta)

		// Add receipt data if available
		if receipt, ok := receiptMap[tx.Hash()]; ok {
			dbTx.GasUsed = receipt.GasUsed
			dbTx.Status = int(receipt.Status)
		}

		transactions = append(transactions, dbTx)
	}

	return transactions
}

// convertTransaction converts a single Ethereum transaction to database model
func (p *TransactionProcessor) convertTransaction(tx *types.Transaction, blockNumber uint64, index int) *database.Transaction {
	var toAddress *string
	if tx.To() != nil {
		addr := tx.To().Hex()
		toAddress = &addr
	}

	// Get the sender address
	from, err := types.Sender(types.LatestSignerForChainID(tx.ChainId()), tx)
	if err != nil {
		p.logger.Error().
			Err(err).
			Str("tx_hash", tx.Hash().Hex()).
			Msg("Failed to get transaction sender")
		from = common.Address{}
	}

	// Default to legacy transaction type
	txType := database.TxTypeLegacy
	
	// Check actual transaction type
	if tx.Type() == types.AccessListTxType {
		txType = database.TxTypeEIP2930
	} else if tx.Type() == types.DynamicFeeTxType {
		txType = database.TxTypeEIP1559
	} else if tx.Type() == types.BlobTxType {
		txType = database.TxTypeEIP4844
	}

	return &database.Transaction{
		Hash:             tx.Hash().Hex(),
		BlockNumber:      blockNumber,
		TransactionIndex: index,
		FromAddress:      from.Hex(),
		ToAddress:        toAddress,
		Value:            tx.Value(),
		GasPrice:         tx.GasPrice(),
		GasLimit:         tx.Gas(),
		Nonce:            tx.Nonce(),
		Input:            common.Bytes2Hex(tx.Data()),
		TransactionType:  txType,
	}
}

// convertTransactionWithMeta converts a transaction with Zilliqa metadata
func (p *TransactionProcessor) convertTransactionWithMeta(tx *types.Transaction, blockNumber uint64, index int, meta *rpc.TransactionMeta) *database.Transaction {
	dbTx := p.convertTransaction(tx, blockNumber, index)
	
	// If this is a Zilliqa pre-EVM transaction, mark it appropriately
	if meta != nil && meta.IsZilliqaType {
		dbTx.TransactionType = database.TxTypeZilliqaBase + int(meta.OriginalType)
		if meta.OriginalTypeHex != "" {
			dbTx.OriginalTypeHex = &meta.OriginalTypeHex
		}
		
		p.logger.Debug().
			Str("tx_hash", tx.Hash().Hex()).
			Str("original_type", meta.OriginalTypeHex).
			Msg("Processing pre-EVM Zilliqa transaction")
	}
	
	return dbTx
}

// insertBatchTx inserts transactions within a database transaction
func (p *TransactionProcessor) insertBatchTx(ctx context.Context, tx pgx.Tx, transactions []*database.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	query := `
		INSERT INTO transactions (
			hash, block_number, transaction_index, from_address, to_address,
			value, gas_price, gas_limit, gas_used, nonce, input, status,
			transaction_type, original_type_hex
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
		ON CONFLICT (hash) DO NOTHING`

	for _, dbTx := range transactions {
		_, err := tx.Exec(ctx, query,
			dbTx.Hash,
			dbTx.BlockNumber,
			dbTx.TransactionIndex,
			dbTx.FromAddress,
			dbTx.ToAddress,
			database.BigIntToNumeric(dbTx.Value),
			database.BigIntToNumeric(dbTx.GasPrice),
			dbTx.GasLimit,
			dbTx.GasUsed,
			dbTx.Nonce,
			dbTx.Input,
			dbTx.Status,
			dbTx.TransactionType,
			dbTx.OriginalTypeHex,
		)
		if err != nil {
			return fmt.Errorf("failed to insert transaction %s: %w", dbTx.Hash, err)
		}
	}

	return nil
}

// GetTransactionCount returns the number of transactions for a given address
func (p *TransactionProcessor) GetTransactionCount(ctx context.Context, address string) (uint64, error) {
	var count uint64
	query := `
		SELECT COUNT(*) 
		FROM transactions 
		WHERE from_address = $1 OR to_address = $1`

	err := p.db.Pool().QueryRow(ctx, query, address).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get transaction count: %w", err)
	}

	return count, nil
}

// GetTransactionsByBlock returns all transactions for a given block
func (p *TransactionProcessor) GetTransactionsByBlock(ctx context.Context, blockNumber uint64) ([]*database.Transaction, error) {
	query := `
		SELECT hash, block_number, transaction_index, from_address, to_address,
		       value, gas_price, gas_limit, gas_used, nonce, input, status
		FROM transactions
		WHERE block_number = $1
		ORDER BY transaction_index`

	rows, err := p.db.Pool().Query(ctx, query, blockNumber)
	if err != nil {
		return nil, fmt.Errorf("failed to query transactions: %w", err)
	}
	defer rows.Close()

	var transactions []*database.Transaction
	for rows.Next() {
		var tx database.Transaction
		var valueStr, gasPriceStr *string

		err := rows.Scan(
			&tx.Hash,
			&tx.BlockNumber,
			&tx.TransactionIndex,
			&tx.FromAddress,
			&tx.ToAddress,
			&valueStr,
			&gasPriceStr,
			&tx.GasLimit,
			&tx.GasUsed,
			&tx.Nonce,
			&tx.Input,
			&tx.Status,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan transaction: %w", err)
		}

		// Convert string values back to big.Int
		if valueStr != nil {
			tx.Value = new(big.Int)
			tx.Value.SetString(*valueStr, 10)
		}
		if gasPriceStr != nil {
			tx.GasPrice = new(big.Int)
			tx.GasPrice.SetString(*gasPriceStr, 10)
		}

		transactions = append(transactions, &tx)
	}

	return transactions, nil
}