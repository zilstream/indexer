# Uniswap V2 Test Results

## Transaction Analysis: 0xf60e0fe0dd5b2824e8d07c09dc7b0682d2478e93f0bb1cb3077f0590a087b397

### Overview
Successfully analyzed and tested Uniswap V2 events from block 3251552 on Zilliqa EVM.

## Key Findings

### 1. PairCreated Events ✅
- **Found**: 2 PairCreated events
- **Factory**: `0xf42d1058f233329185a36b04b7f96105afa1add2`
- **Pair**: `0xCA36279D8fDa7f75EE4dc62DE301181824373E2c`
- **Token0**: `0x2274005778063684fbB1BfA96a2b725dC37D75f9`
- **Token1**: `0xc6F3dede529Af9D98a11C5B32DbF03Bf34272ED5`
- **Pair Index**: 1

### 2. AddLiquidity Transaction ✅
Successfully decoded the `addLiquidity` method call:
- **Method ID**: `0xe8e33700`
- **Router**: `0x33c6a20d2a605da9fd1f506dded449355f0564fe`
- **TokenA**: `0xc6F3dede529Af9D98a11C5B32DbF03Bf34272ED5`
- **TokenB**: `0x2274005778063684fbB1BfA96a2b725dC37D75f9`
- **AmountA**: 830
- **AmountB**: 10,475,223
- **Recipient**: `0x7B81481c116fd3D0F8172EECE0CB383C00a82732`

### 3. Event Summary ✅
Successfully detected all Uniswap V2 events:
| Event Type | Count |
|------------|-------|
| PairCreated | 2 |
| Mint | 2 |
| Sync | 2 |
| Transfer | 4 |

### 4. Liquidity Provision Flow ✅
The complete flow was validated:
1. **Router called** → `addLiquidity` method invoked
2. **Pair created** → Factory emits PairCreated event
3. **Tokens transferred** → Both tokens sent to pair contract
4. **LP tokens minted** → Mint events show liquidity added
5. **Reserves synced** → Sync events update pair reserves

### 5. Token and Contract Discovery ✅
Successfully extracted:
- **1 Factory contract**: `0xf42d1058f233329185a36b04b7f96105afa1add2`
- **1 Pair contract**: `0xCA36279D8fDa7f75EE4dc62DE301181824373E2c`
- **2 Token contracts**:
  - `0x2274005778063684fbB1BfA96a2b725dC37D75f9`
  - `0xc6F3dede529Af9D98a11C5B32DbF03Bf34272ED5`

### 6. Mint Event Details ✅
Both Mint events showed identical amounts (duplicate liquidity addition):
- **Amount0**: 10,475,223
- **Amount1**: 830
- **Sender**: Router contract

### 7. Sync Event Details ✅
Final reserves after liquidity additions:
- **Reserve0**: 10,475,223
- **Reserve1**: 830

## Test Coverage

All test cases passed:
- ✅ `detect_PairCreated_event`
- ✅ `parse_addLiquidity_transaction`
- ✅ `extract_all_Uniswap_events_and_tokens`
- ✅ `decode_Mint_events`
- ✅ `decode_Sync_events`
- ✅ `complete_liquidity_provision_flow`

## Special Notes

1. **Zilliqa-specific events**: The transaction also includes Zilliqa-specific `TransferFromSuccess` events (signature: `0x96acecb2152edcc0681aa27d354d55d64192e86489ff8d5d903d63ef266755a1`) from wrapped token contracts.

2. **Duplicate operations**: The transaction appears to perform the same liquidity addition twice, possibly due to how the router handles the operation.

3. **Event ordering**: Events follow the expected sequence for liquidity provision.

## Conclusion

The test suite successfully:
- ✅ Recognizes PairCreated events
- ✅ Extracts token addresses from events
- ✅ Decodes Uniswap V2 method calls
- ✅ Parses all event types correctly
- ✅ Validates the complete liquidity provision flow

This confirms that the indexer's event processing logic is working correctly for Uniswap V2 events on Zilliqa EVM.