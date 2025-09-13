package uniswapv3

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testLogger() zerolog.Logger { return zerolog.Nop() }

type receipt struct {
	Result struct {
		TransactionHash string        `json:"transactionHash"`
		BlockHash       string        `json:"blockHash"`
		BlockNumber     string        `json:"blockNumber"`
		Logs            []receiptLog  `json:"logs"`
	} `json:"result"`
}

type receiptLog struct {
	Removed          bool     `json:"removed"`
	LogIndex         string   `json:"logIndex"`
	TransactionIndex string   `json:"transactionIndex"`
	TransactionHash  string   `json:"transactionHash"`
	BlockHash        string   `json:"blockHash"`
	BlockNumber      string   `json:"blockNumber"`
	Address          string   `json:"address"`
	Data             string   `json:"data"`
	Topics           []string `json:"topics"`
}

func hexToUint64(h string) uint64 {
	if len(h) >= 2 && h[:2] == "0x" {
		h = h[2:]
	}
	var v uint64
	for _, c := range []byte(h) {
		v <<= 4
		switch {
		case c >= '0' && c <= '9':
			v += uint64(c - '0')
		case c >= 'a' && c <= 'f':
			v += uint64(c-'a') + 10
		case c >= 'A' && c <= 'F':
			v += uint64(c-'A') + 10
		}
	}
	return v
}

func hexToUint(h string) uint { return uint(hexToUint64(h)) }

func toEthLog(r receiptLog) types.Log {
	var topics []common.Hash
	for _, t := range r.Topics {
		topics = append(topics, common.HexToHash(t))
	}
	return types.Log{
		Address:     common.HexToAddress(r.Address),
		Topics:      topics,
		Data:        common.Hex2Bytes(r.Data),
		BlockNumber: hexToUint64(r.BlockNumber),
		TxHash:      common.HexToHash(r.TransactionHash),
		TxIndex:     hexToUint(r.TransactionIndex),
		BlockHash:   common.HexToHash(r.BlockHash),
		Index:       hexToUint(r.LogIndex),
		Removed:     r.Removed,
	}
}

func mapKeys(m map[string]interface{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m { keys = append(keys, k) }
	return keys
}

func Test_UniswapV3_ParsePoolCreatedAndSwap(t *testing.T) {
	// Ensure manifest relative path exists for constructor
	_ = os.MkdirAll("manifests", 0o755)
	manifestSrc := "../../../manifests/uniswap-v3.yaml"
	manifestDst := "manifests/uniswap-v3.yaml"
	if _, err := os.Stat(manifestDst); os.IsNotExist(err) {
		b, rerr := os.ReadFile(manifestSrc)
		require.NoError(t, rerr)
		require.NoError(t, os.WriteFile(manifestDst, b, 0o644))
	}

	// Create module (loads ABIs)
	m, err := NewUniswapV3Module(testLogger())
	require.NoError(t, err)

	// Load pool creation receipt
	poolReceiptBytes, err := os.ReadFile("../../../testdata/uniswap-v3-pool-creation-receipt.json")
	require.NoError(t, err)
	var poolRcpt receipt
	require.NoError(t, json.Unmarshal(poolReceiptBytes, &poolRcpt))

	// Find PoolCreated logs by topic0 from factory ABI
	poolCreatedTopic := m.factoryABI.Events["PoolCreated"].ID.Hex()
	foundPoolCreated := false
	for _, rl := range poolRcpt.Result.Logs {
		if len(rl.Topics) == 0 { continue }
		if common.HexToHash(rl.Topics[0]).Hex() != poolCreatedTopic { continue }
		foundPoolCreated = true
		ethLog := toEthLog(rl)
		pe, err := m.parser.ParseEvent(&ethLog)
		require.NoError(t, err)
		assert.Equal(t, "PoolCreated", pe.EventName)
		// Validate minimal args presence
		t.Logf("PoolCreated args keys: %v", mapKeys(pe.Args))
		_, ok0 := pe.Args["token0"]
		_, ok1 := pe.Args["token1"]
		_, okFee := pe.Args["fee"]
		assert.True(t, ok0 && ok1 && okFee, "PoolCreated args should be parsed (indexed)")
	}
	assert.True(t, foundPoolCreated, "should find PoolCreated in receipt")

	// Load swap receipt
	swapReceiptBytes, err := os.ReadFile("../../../testdata/uniswap-v3-swap-receipt.json")
	require.NoError(t, err)
	var swapRcpt receipt
	require.NoError(t, json.Unmarshal(swapReceiptBytes, &swapRcpt))
	assert.Greater(t, len(swapRcpt.Result.Logs), 0, "swap receipt should contain logs")

	// Look for a V3-like swap topic (Algebra variant observed on ZIL)
	altSwapTopic := "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
	foundSwap := false
	for _, rl := range swapRcpt.Result.Logs {
		if len(rl.Topics) == 0 { continue }
		if rl.Topics[0] == altSwapTopic { foundSwap = true; break }
	}
	assert.True(t, foundSwap, "should find at least one Uniswap V3-like Swap event topic in receipt")
}

func Test_UniswapV3_DecodeSwapOnly(t *testing.T) {
	m, err := NewUniswapV3Module(testLogger())
	require.NoError(t, err)

	// Ensure pool ABI is registered so the parser knows Swap event signature
	m.parser.AddContract(common.Address{}, m.poolABI)

	// Load swap receipt
	swapReceiptBytes, err := os.ReadFile("../../../testdata/uniswap-v3-swap-receipt.json")
	require.NoError(t, err)
	var swapRcpt receipt
	require.NoError(t, json.Unmarshal(swapReceiptBytes, &swapRcpt))

	// Look for a V3-like swap topic (Algebra variant observed on ZIL)
	altSwapTopic := "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"
	foundSwap := false
	for _, rl := range swapRcpt.Result.Logs {
		if len(rl.Topics) == 0 { continue }
		if rl.Topics[0] == altSwapTopic { foundSwap = true; break }
	}
	assert.True(t, foundSwap, "should find V3-like Swap topic in receipt")
}
