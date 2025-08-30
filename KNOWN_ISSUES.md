# Known Issues

## 1. Zilliqa-Specific Transaction Types (Stage 1)

### Issue
Some blocks on Zilliqa contain transactions with custom transaction types that are not supported by the standard go-ethereum library. When attempting to fetch these blocks with transactions, the error "transaction type not supported" is returned.

### Affected Blocks
- Block 8491997 and others with Zilliqa-specific transactions
- These blocks appear to have transactions when queried, but the go-ethereum client cannot parse them

### Current Workaround (Stage 1)
When we encounter this error, we:
1. Log a warning message
2. Fetch the block without transactions
3. Continue processing (the block is stored but transactions are skipped)

This means:
- The block itself is indexed
- The transactions in that block are NOT indexed
- The indexer continues running without crashing

### Proper Solution (Stage 3+)
In Stage 3 (Event Log Processing), we will:
1. Implement raw RPC parsing for Zilliqa-specific blocks
2. Create custom transaction decoders for Zilliqa transaction types
3. Store these transactions with a special type flag
4. Process logs from these transactions properly

### Investigation Notes
- Zilliqa blocks include a `quorumCertificate` field not present in standard Ethereum
- Some transaction types appear to be related to Zilliqa's consensus mechanism
- The RPC endpoint returns these blocks but go-ethereum cannot deserialize them

### Impact
- Low: Most blocks process correctly
- Affected blocks still get indexed (without their transactions)
- No data corruption or sync issues
- Will be fully resolved in Stage 3

## 2. Empty Blocks

Many blocks on Zilliqa have no transactions. This is normal behavior and not an error.