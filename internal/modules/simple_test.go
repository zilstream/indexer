package modules

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRealBlockData(t *testing.T) {
	t.Run("validate block 3251552 structure", func(t *testing.T) {
		// Load block data
		blockData, err := os.ReadFile("../../test_data_block.json")
		require.NoError(t, err)
		
		var blockJSON map[string]interface{}
		err = json.Unmarshal(blockData, &blockJSON)
		require.NoError(t, err)
		
		result := blockJSON["result"].(map[string]interface{})
		
		// Validate block structure
		assert.Equal(t, "0x319d60", result["number"])
		assert.Equal(t, "0x85673be95d799659eba5aced882786a447c1b6ffb778aa63cc5e871e97c78da4", result["hash"])
		assert.Equal(t, "0x6526b669", result["timestamp"])
		
		// Check transactions
		txs := result["transactions"].([]interface{})
		assert.Len(t, txs, 2, "Block should have 2 transactions")
		
		// First transaction - Standard EVM
		tx1 := txs[0].(map[string]interface{})
		assert.Equal(t, "0x0", tx1["type"])
		assert.Equal(t, "0xf60e0fe0dd5b2824e8d07c09dc7b0682d2478e93f0bb1cb3077f0590a087b397", tx1["hash"])
		assert.Equal(t, "0x33c6a20d2a605da9fd1f506dded449355f0564fe", tx1["to"])
		
		// Second transaction - Zilliqa pre-EVM
		tx2 := txs[1].(map[string]interface{})
		assert.Equal(t, "0xdd870", tx2["type"])
		assert.Equal(t, "0x2476d2892a8061942df3577d41bec053774be79f611f31ab86104827b0c4223e", tx2["hash"])
		
		t.Log("✓ Block structure validated")
	})
	
	t.Run("validate receipt data", func(t *testing.T) {
		// Load first receipt
		receipt1Data, err := os.ReadFile("../../test_data_receipt1.json")
		require.NoError(t, err)
		
		var receipt1JSON map[string]interface{}
		err = json.Unmarshal(receipt1Data, &receipt1JSON)
		require.NoError(t, err)
		
		result := receipt1JSON["result"].(map[string]interface{})
		
		// Validate receipt structure
		assert.Equal(t, "0x1", result["status"])
		assert.Equal(t, "0xf60e0fe0dd5b2824e8d07c09dc7b0682d2478e93f0bb1cb3077f0590a087b397", result["transactionHash"])
		assert.Equal(t, "0x319d60", result["blockNumber"])
		
		// Check logs
		logs := result["logs"].([]interface{})
		assert.Greater(t, len(logs), 0, "Receipt should have logs")
		
		// Check for Uniswap events
		eventSignatures := map[string]string{
			"0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822": "Swap",
			"0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1": "Sync",
			"0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f": "Mint",
		}
		
		foundEvents := []string{}
		for _, logData := range logs {
			log := logData.(map[string]interface{})
			topics := log["topics"].([]interface{})
			if len(topics) > 0 {
				topic0 := topics[0].(string)
				if eventName, ok := eventSignatures[topic0]; ok {
					foundEvents = append(foundEvents, eventName)
					t.Logf("Found %s event at address %s", eventName, log["address"])
				}
			}
		}
		
		assert.Greater(t, len(foundEvents), 0, "Should find Uniswap events")
		t.Logf("✓ Found %d Uniswap events in receipt", len(foundEvents))
	})
	
	t.Run("validate Zilliqa transaction format", func(t *testing.T) {
		// Load block data
		blockData, err := os.ReadFile("../../test_data_block.json")
		require.NoError(t, err)
		
		var blockJSON map[string]interface{}
		err = json.Unmarshal(blockData, &blockJSON)
		require.NoError(t, err)
		
		result := blockJSON["result"].(map[string]interface{})
		txs := result["transactions"].([]interface{})
		
		// Find Zilliqa transaction
		var zilliqaTx map[string]interface{}
		for _, tx := range txs {
			txMap := tx.(map[string]interface{})
			if txMap["type"] == "0xdd870" {
				zilliqaTx = txMap
				break
			}
		}
		
		require.NotNil(t, zilliqaTx, "Should find Zilliqa transaction")
		
		// Parse the input JSON
		inputHex := zilliqaTx["input"].(string)
		inputBytes := common.FromHex(inputHex)
		
		var inputJSON map[string]interface{}
		err = json.Unmarshal(inputBytes, &inputJSON)
		require.NoError(t, err, "Input should be valid JSON")
		
		// Validate JSON structure
		assert.Equal(t, "WithdrawStakeRewards", inputJSON["_tag"])
		params := inputJSON["params"].([]interface{})
		assert.Len(t, params, 1)
		
		param := params[0].(map[string]interface{})
		assert.Equal(t, "ssnaddr", param["vname"])
		assert.Equal(t, "ByStr20", param["type"])
		
		t.Logf("✓ Zilliqa transaction validated: %s", inputJSON["_tag"])
	})
}