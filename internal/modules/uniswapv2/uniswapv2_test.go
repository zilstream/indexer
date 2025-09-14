package uniswapv2

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
		TransactionHash string       `json:"transactionHash"`
		BlockHash       string       `json:"blockHash"`
		BlockNumber     string       `json:"blockNumber"`
		Logs            []receiptLog `json:"logs"`
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
	if len(h) >= 2 && h[:2] == "0x" { h = h[2:] }
	var v uint64
	for _, c := range []byte(h) {
		v <<= 4
		switch {
		case c >= '0' && c <= '9': v += uint64(c-'0')
		case c >= 'a' && c <= 'f': v += uint64(c-'a') + 10
		case c >= 'A' && c <= 'F': v += uint64(c-'A') + 10
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

func Test_UniswapV2_ParsePairCreatedAndMintFlow(t *testing.T) {
	// Ensure manifest relative path exists for constructor
	_ = os.MkdirAll("manifests", 0o755)
	manifestSrc := "../../../manifests/uniswap-v2.yaml"
	manifestDst := "manifests/uniswap-v2.yaml"
	if _, err := os.Stat(manifestDst); os.IsNotExist(err) {
		b, rerr := os.ReadFile(manifestSrc)
		require.NoError(t, rerr)
		require.NoError(t, os.WriteFile(manifestDst, b, 0o644))
	}

	m, err := NewUniswapV2Module(testLogger())
	require.NoError(t, err)

	// Load receipt
	b, err := os.ReadFile("../../../testdata/uniswap-v2-pair-creation-receipt.json")
	require.NoError(t, err)
	var rcpt receipt
	require.NoError(t, json.Unmarshal(b, &rcpt))
	assert.Greater(t, len(rcpt.Result.Logs), 0)

	pairCreatedTopic := m.factoryABI.Events["PairCreated"].ID.Hex()
	mintTopic := common.HexToHash("0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f").Hex()
	syncTopic := common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1").Hex()
	transferTopic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").Hex()

	// Register pair ABI to parse Mint/Sync if needed
	m.parser.AddContract(common.Address{}, m.pairABI)

	foundPairCreated := 0
	foundMint := 0
	foundSync := 0
	foundTransfer := 0

	for _, rl := range rcpt.Result.Logs {
		if len(rl.Topics) == 0 { continue }
		if rl.Topics[0] == pairCreatedTopic {
			foundPairCreated++
			ethLog := toEthLog(rl)
			pe, err := m.parser.ParseEvent(&ethLog)
			require.NoError(t, err)
			assert.Equal(t, "PairCreated", pe.EventName)
			_, ok0 := pe.Args["token0"]; _, ok1 := pe.Args["token1"]
			assert.True(t, ok0 && ok1)
		}
		if rl.Topics[0] == mintTopic { foundMint++ }
		if rl.Topics[0] == syncTopic { foundSync++ }
		if rl.Topics[0] == transferTopic { foundTransfer++ }
	}

	assert.GreaterOrEqual(t, foundPairCreated, 1)
	assert.GreaterOrEqual(t, foundMint, 1)
	assert.GreaterOrEqual(t, foundSync, 1)
	assert.GreaterOrEqual(t, foundTransfer, 1)
}

func Test_UniswapV2_ParseMintOnly(t *testing.T) {
	m, err := NewUniswapV2Module(testLogger())
	require.NoError(t, err)
	m.parser.AddContract(common.Address{}, m.pairABI)

	b, err := os.ReadFile("../../../testdata/uniswap-v2-mint-receipt.json")
	require.NoError(t, err)
	var rcpt receipt
	require.NoError(t, json.Unmarshal(b, &rcpt))

	mintTopic := common.HexToHash("0x4c209b5fc8ad50758f13e2e1088ba56a560dff690a1c6fef26394f4c03821c4f").Hex()
	syncTopic := common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1").Hex()
	transferTopic := common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef").Hex()

	mints := 0
	syncs := 0
	transfers := 0
	for _, rl := range rcpt.Result.Logs {
		if len(rl.Topics) == 0 { continue }
		switch rl.Topics[0] {
		case mintTopic:
			mints++
			// Decode amounts
			log := toEthLog(rl)
			pe, err := m.parser.ParseEvent(&log)
			require.NoError(t, err)
			assert.Equal(t, "Mint", pe.EventName)
		case syncTopic:
			syncs++
		case transferTopic:
			transfers++
		}
	}
	assert.GreaterOrEqual(t, mints, 1)
	assert.GreaterOrEqual(t, syncs, 1)
	assert.GreaterOrEqual(t, transfers, 1)
}

func Test_UniswapV2_ParseSwapReceipt(t *testing.T) {
	m, err := NewUniswapV2Module(testLogger())
	require.NoError(t, err)
	m.parser.AddContract(common.Address{}, m.pairABI)

	b, err := os.ReadFile("../../../testdata/uniswap-v2-swap-receipt.json")
	require.NoError(t, err)
	var rcpt receipt
	require.NoError(t, json.Unmarshal(b, &rcpt))

	swapTopic := common.HexToHash("0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822").Hex()
	syncTopic := common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1").Hex()

	foundSwap := 0
	foundSync := 0
	for _, rl := range rcpt.Result.Logs {
		if len(rl.Topics) == 0 { continue }
		if rl.Topics[0] == swapTopic { foundSwap++ }
		if rl.Topics[0] == syncTopic { foundSync++ }
	}
	assert.GreaterOrEqual(t, foundSwap, 1)
	assert.GreaterOrEqual(t, foundSync, 1)
}
