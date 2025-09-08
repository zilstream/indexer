# Test Data

This directory contains real blockchain data from Zilliqa EVM for testing purposes.

## Files

### `test_data_block.json`
- **Block Number**: 3251552 (0x319d60)
- **Block Hash**: `0x85673be95d799659eba5aced882786a447c1b6ffb778aa63cc5e871e97c78da4`
- **Timestamp**: 1697036905 (0x6526b669)
- **Transactions**: 2 transactions
  - Standard EVM transaction (addLiquidity call to Uniswap V2 router)
  - Zilliqa pre-EVM transaction (WithdrawStakeRewards, type 0xdd870)

### `test_data_receipt1.json`
- **Transaction Hash**: `0xf60e0fe0dd5b2824e8d07c09dc7b0682d2478e93f0bb1cb3077f0590a087b397`
- **Type**: EVM transaction receipt (addLiquidity)
- **Events**: Contains Uniswap V2 events:
  - 2x PairCreated events
  - 2x Mint events  
  - 2x Sync events
  - 4x Transfer events
  - Zilliqa-specific TransferFromSuccess events

### `test_data_receipt2.json`
- **Transaction Hash**: `0x2476d2892a8061942df3577d41bec053774be79f611f31ab86104827b0c4223e`
- **Type**: Zilliqa pre-EVM transaction receipt (WithdrawStakeRewards)
- **Status**: Success (0x1)

## Usage

These files are used by test suites to validate:
- Block and transaction processing
- Event log extraction and parsing
- Zilliqa-specific transaction handling (pre-EVM types)
- Uniswap V2 event detection and decoding
- Complete liquidity provision flow analysis

## Data Source

This is real data from Zilliqa EVM mainnet, captured for testing the indexer's ability to handle:
- Mixed transaction types (EVM + Zilliqa pre-EVM)
- Complex DeFi operations (Uniswap V2 liquidity provision)
- Event-rich transactions with multiple smart contract interactions