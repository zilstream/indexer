package main

import (
	"context"
	"fmt"
	"log"
	"math/big"

	"github.com/ethereum/go-ethereum/ethclient"
)

func main() {
	// Connect to Zilliqa RPC
	client, err := ethclient.Dial("https://api.zilliqa.com")
	if err != nil {
		log.Fatal("Failed to connect:", err)
	}
	defer client.Close()

	ctx := context.Background()

	// Get block 8433115
	blockNum := uint64(8433115)
	block, err := client.BlockByNumber(ctx, big.NewInt(int64(blockNum)))
	if err != nil {
		log.Fatal("Failed to get block:", err)
	}

	fmt.Printf("Block %d:\n", blockNum)
	fmt.Printf("  Hash: %s\n", block.Hash().Hex())
	fmt.Printf("  Transactions: %d\n", len(block.Transactions()))

	// Get receipts for this block
	for i, tx := range block.Transactions() {
		fmt.Printf("\nTransaction %d:\n", i)
		fmt.Printf("  Hash: %s\n", tx.Hash().Hex())
		fmt.Printf("  Type: %d\n", tx.Type())
		fmt.Printf("  To: %v\n", tx.To())
		
		// Get receipt
		receipt, err := client.TransactionReceipt(ctx, tx.Hash())
		if err != nil {
			fmt.Printf("  Receipt error: %v\n", err)
			continue
		}
		
		fmt.Printf("  Status: %d\n", receipt.Status)
		fmt.Printf("  Gas Used: %d\n", receipt.GasUsed)
		fmt.Printf("  Logs: %d\n", len(receipt.Logs))
		
		// Print event logs
		for j, log := range receipt.Logs {
			fmt.Printf("\n  Log %d:\n", j)
			fmt.Printf("    Address: %s\n", log.Address.Hex())
			fmt.Printf("    Topics: %d\n", len(log.Topics))
			if len(log.Topics) > 0 {
				fmt.Printf("    Topic[0]: %s\n", log.Topics[0].Hex())
			}
			fmt.Printf("    Data length: %d bytes\n", len(log.Data))
		}
	}
}