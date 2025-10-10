# ZilStream Indexer API Documentation

## Base URL
All endpoints are served on the configured API port (default: `:8080`)

## Response Format
All responses follow a consistent envelope structure:

```json
{
  "data": { ... },
  "pagination": { ... },  // Only for list endpoints
  "error": "..."         // Only on errors
}
```

### Pagination
List endpoints support pagination with the following query parameters:
- `page`: Page number (default: 1)
- `per_page`: Items per page (default: 25, max: 100)

Pagination response:
```json
{
  "pagination": {
    "page": 1,
    "per_page": 25,
    "has_next": true
  }
}
```

---

## Global Stats

### `GET /stats`
Returns aggregate statistics across all DEX pairs and tokens.

**Response:**
```json
{
  "data": {
    "total_pairs": 1234,
    "total_tokens": 567,
    "total_liquidity_usd": "12345678.90",
    "total_volume_usd_24h": "234567.89",
    "total_volume_usd_all": "98765432.10"
  }
}
```

**Fields:**
- `total_pairs`: Total number of trading pairs across all DEXs
- `total_tokens`: Total number of unique tokens tracked
- `total_liquidity_usd`: Total liquidity in USD across all pairs
- `total_volume_usd_24h`: Total trading volume in USD (last 24 hours)
- `total_volume_usd_all`: Total trading volume in USD (all-time)

---

## Blocks

### `GET /blocks`
Returns a paginated list of blocks, ordered by block number descending (newest first).

**Query Parameters:**
- `page`: Page number
- `per_page`: Items per page

**Response:**
```json
{
  "data": [
    {
      "number": 8470100,
      "hash": "0xabc123...",
      "parent_hash": "0xdef456...",
      "timestamp": 1704067200,
      "gas_limit": 80000000,
      "gas_used": 12345678,
      "base_fee_per_gas": "1000000000",
      "transaction_count": 42
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 25,
    "has_next": true
  }
}
```

**Fields:**
- `number`: Block number
- `hash`: Block hash (with 0x prefix)
- `parent_hash`: Previous block hash
- `timestamp`: Unix timestamp (seconds)
- `gas_limit`: Gas limit for the block
- `gas_used`: Total gas used by all transactions
- `base_fee_per_gas`: Base fee (EIP-1559), as string
- `transaction_count`: Number of transactions in block

### `GET /blocks/{number}`
Returns details for a specific block by block number.

**Response:**
```json
{
  "data": {
    "number": 8470100,
    "hash": "0xabc123...",
    "parent_hash": "0xdef456...",
    "timestamp": 1704067200,
    "gas_limit": 80000000,
    "gas_used": 12345678,
    "base_fee_per_gas": "1000000000",
    "transaction_count": 42
  }
}
```

**Error Response (404):**
```json
{
  "error": "block not found"
}
```

---

## Transactions

### `GET /transactions`
Returns a paginated list of transactions, ordered by block number and transaction index descending (newest first).

**Query Parameters:**
- `page`: Page number
- `per_page`: Items per page

**Response:**
```json
{
  "data": [
    {
      "hash": "0x789abc...",
      "block_number": 8470100,
      "transaction_index": 5,
      "from_address": "0x123...",
      "to_address": "0x456...",
      "value": "1000000000000000000",
      "gas_price": "1000000000",
      "gas_limit": 21000,
      "gas_used": 21000,
      "nonce": 42,
      "status": 1,
      "transaction_type": 2,
      "original_type_hex": null,
      "max_fee_per_gas": "2000000000",
      "max_priority_fee_per_gas": "1000000000",
      "effective_gas_price": "1500000000",
      "contract_address": null,
      "cumulative_gas_used": 123456
    }
  ],
  "pagination": {
    "page": 1,
    "per_page": 25,
    "has_next": true
  }
}
```

**Fields:**
- `hash`: Transaction hash (with 0x prefix)
- `block_number`: Block number containing this transaction
- `transaction_index`: Position in block
- `from_address`: Sender address
- `to_address`: Recipient address (null for contract creation)
- `value`: Amount transferred in wei (as string)
- `gas_price`: Gas price in wei (as string)
- `gas_limit`: Gas limit set for transaction
- `gas_used`: Actual gas consumed
- `nonce`: Sender nonce
- `status`: Transaction status (1 = success, 0 = failed)
- `transaction_type`: 
  - `0-3`: Standard EVM transaction types
  - `1000+`: Zilliqa pre-EVM transaction (1000 + original type)
- `original_type_hex`: Hex value of original Zilliqa transaction type (for pre-EVM txs)
- `max_fee_per_gas`: EIP-1559 max fee (as string)
- `max_priority_fee_per_gas`: EIP-1559 priority fee (as string)
- `effective_gas_price`: Actual gas price paid (as string)
- `contract_address`: Address of deployed contract (for contract creation)
- `cumulative_gas_used`: Cumulative gas used in block up to this transaction

### `GET /transactions/{hash}`
Returns details for a specific transaction by hash.

**Response:**
Same structure as single transaction item above.

**Error Response (404):**
```json
{
  "error": "transaction not found"
}
```

---

## Health & Status

### `GET /health`
Simple health check endpoint.

**Response:**
```json
{
  "data": {
    "status": "healthy",
    "timestamp": "2024-01-01T12:00:00Z"
  }
}
```

### `GET /status`
Returns indexer status with last processed block information.

**Response:**
```json
{
  "data": {
    "last_block": {
      "number": 8470100,
      "timestamp": 1704067200
    },
    "time": "2024-01-01T12:00:00Z"
  }
}
```

---

## Additional Endpoints

### Tokens
- `GET /tokens` - List all tokens with optional search
- Query params: `search` (fuzzy search on symbol/name)

### Pairs
- `GET /pairs` - List all trading pairs
- Query params: `sort_by` (volume_24h, liquidity, created, volume), `sort_order` (asc, desc)

### Pair Events
- `GET /pairs/{address}/events` - List events for specific pair
- Query params: `type` (event type filter), `protocol` (protocol filter)

---

## Notes

### Zilliqa-Specific Transaction Types
Zilliqa has pre-EVM transaction types that are encoded as `1000 + original_type`. Check the `original_type_hex` field for the raw Zilliqa transaction type.

### Large Numbers
All large numeric values (prices, amounts, gas values) are returned as strings to prevent precision loss in JSON parsing.

### Timestamps
All timestamps are Unix timestamps in seconds (not milliseconds).
