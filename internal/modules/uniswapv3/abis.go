package uniswapv3

import (
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

// initializeABIs sets up the contract ABIs for parsing events
func (m *UniswapV3Module) initializeABIs() error {
	// Initialize Factory ABI
	factoryABI, err := abi.JSON(strings.NewReader(UniswapV3FactoryABI))
	if err != nil {
		return fmt.Errorf("failed to parse v3 factory ABI: %w", err)
	}
	m.factoryABI = &factoryABI

	// Initialize Pool ABI
	poolABI, err := abi.JSON(strings.NewReader(UniswapV3PoolABI))
	if err != nil {
		return fmt.Errorf("failed to parse v3 pool ABI: %w", err)
	}
	m.poolABI = &poolABI

	// Initialize alternative Algebra-style V3 pool ABI (observed on Zilliqa)
	altPoolABI, err := abi.JSON(strings.NewReader(AlgebraV3PoolABI))
	if err != nil {
		return fmt.Errorf("failed to parse alt v3 pool ABI: %w", err)
	}

	// Add ABIs to event parser
	m.parser.AddContract(m.factoryAddress, &factoryABI)
	// Add both pool ABIs so topic->event resolution works regardless of implementation
	m.parser.AddContract(m.factoryAddress, &poolABI)
	m.parser.AddContract(m.factoryAddress, &altPoolABI)
	// Pool contracts will be added dynamically upon PoolCreated

	return nil
}

// Minimal Uniswap V3 ABIs with only events needed by the module

const UniswapV3FactoryABI = `[
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true,  "internalType": "address", "name": "token0",      "type": "address"},
      {"indexed": true,  "internalType": "address", "name": "token1",      "type": "address"},
      {"indexed": true,  "internalType": "uint24",  "name": "fee",         "type": "uint24"},
      {"indexed": false, "internalType": "int24",   "name": "tickSpacing", "type": "int24"},
      {"indexed": false, "internalType": "address", "name": "pool",        "type": "address"}
    ],
    "name": "PoolCreated",
    "type": "event"
  }
]`

const UniswapV3PoolABI = `[
  {
    "anonymous": false,
    "inputs": [
      {"indexed": false, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
      {"indexed": false, "internalType": "int24",  "name": "tick",          "type": "int24"}
    ],
    "name": "Initialize",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true,  "internalType": "address", "name": "sender",      "type": "address"},
      {"indexed": true,  "internalType": "address", "name": "recipient",   "type": "address"},
      {"indexed": false, "internalType": "int256",  "name": "amount0",     "type": "int256"},
      {"indexed": false, "internalType": "int256",  "name": "amount1",     "type": "int256"},
      {"indexed": false, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
      {"indexed": false, "internalType": "uint128", "name": "liquidity",    "type": "uint128"},
      {"indexed": false, "internalType": "int24",   "name": "tick",         "type": "int24"}
    ],
    "name": "Swap",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": false, "internalType": "address", "name": "sender",     "type": "address"},
      {"indexed": true,  "internalType": "address", "name": "owner",      "type": "address"},
      {"indexed": true,  "internalType": "int24",   "name": "tickLower",  "type": "int24"},
      {"indexed": true,  "internalType": "int24",   "name": "tickUpper",  "type": "int24"},
      {"indexed": false, "internalType": "uint128", "name": "amount",     "type": "uint128"},
      {"indexed": false, "internalType": "uint256", "name": "amount0",    "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "amount1",    "type": "uint256"}
    ],
    "name": "Mint",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true,  "internalType": "address", "name": "owner",      "type": "address"},
      {"indexed": true,  "internalType": "int24",   "name": "tickLower",  "type": "int24"},
      {"indexed": true,  "internalType": "int24",   "name": "tickUpper",  "type": "int24"},
      {"indexed": false, "internalType": "uint128", "name": "amount",     "type": "uint128"},
      {"indexed": false, "internalType": "uint256", "name": "amount0",    "type": "uint256"},
      {"indexed": false, "internalType": "uint256", "name": "amount1",    "type": "uint256"}
    ],
    "name": "Burn",
    "type": "event"
  },
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true,  "internalType": "address", "name": "owner",      "type": "address"},
      {"indexed": false, "internalType": "address", "name": "recipient",  "type": "address"},
      {"indexed": true,  "internalType": "int24",   "name": "tickLower",  "type": "int24"},
      {"indexed": true,  "internalType": "int24",   "name": "tickUpper",  "type": "int24"},
      {"indexed": false, "internalType": "uint128", "name": "amount0",    "type": "uint128"},
      {"indexed": false, "internalType": "uint128", "name": "amount1",    "type": "uint128"}
    ],
    "name": "Collect",
    "type": "event"
  }
]`

// Alternative Algebra-style V3 pool ABI (observed differences: liquidity may be uint32)
const AlgebraV3PoolABI = `[
  {
    "anonymous": false,
    "inputs": [
      {"indexed": true,  "internalType": "address", "name": "sender",      "type": "address"},
      {"indexed": true,  "internalType": "address", "name": "recipient",   "type": "address"},
      {"indexed": false, "internalType": "int256",  "name": "amount0",     "type": "int256"},
      {"indexed": false, "internalType": "int256",  "name": "amount1",     "type": "int256"},
      {"indexed": false, "internalType": "uint160", "name": "sqrtPriceX96", "type": "uint160"},
      {"indexed": false, "internalType": "uint32",  "name": "liquidity",    "type": "uint32"},
      {"indexed": false, "internalType": "int24",   "name": "tick",         "type": "int24"}
    ],
    "name": "Swap",
    "type": "event"
  }
]`
