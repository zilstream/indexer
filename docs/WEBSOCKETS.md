# WebSocket Real-Time Updates

ZilStream Indexer provides real-time updates for trading pair data via WebSockets using [Centrifugo](https://centrifugal.dev/).

## Overview

Clients can subscribe to real-time updates for:
- **Pair List Updates** - Batched updates for multiple pairs (for overview pages)
- **Individual Pair Updates** - Dedicated channel for specific pair updates (for detail pages)

## Connection Setup

### 1. Get WebSocket Connection Info

**Endpoint:** `GET /ws/info`

**Response:**
```json
{
  "url": "ws://localhost:8001/connection/websocket",
  "ping_ms": 25000,
  "recover": true
}
```

### 2. Get Connection Token

**Endpoint:** `POST /ws/token`

**Response:**
```json
{
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
```

The token is valid for 12 hours and allows anonymous connections.

### 3. Connect to Centrifugo

Use a Centrifugo client library to connect:

**JavaScript Example:**
```javascript
import { Centrifuge } from 'centrifuge';

// Get connection info and token
const infoRes = await fetch('/ws/info');
const { url } = await infoRes.json();

const tokenRes = await fetch('/ws/token', { method: 'POST' });
const { token } = await tokenRes.json();

// Connect
const centrifuge = new Centrifuge(url, {
  token: token
});

centrifuge.connect();
```

## Channels

### Pair List Channel: `dex.pairs`

Subscribe to receive batched updates for multiple pairs that changed.

**JavaScript Example:**
```javascript
const subscription = centrifuge.newSubscription('dex.pairs');

subscription.on('publication', (ctx) => {
  const update = ctx.data;
  console.log('Batch update:', update);
  // update.type === "pair.batch"
  // update.block_number
  // update.ts (timestamp)
  // update.items (array of pair summaries)
});

subscription.subscribe();
```

**Message Format:**
```json
{
  "type": "pair.batch",
  "block_number": 8470100,
  "ts": 1731698201,
  "items": [
    {
      "protocol": "uniswap_v2",
      "address": "0xabc123...",
      "token0": "0xdef456...",
      "token1": "0x789abc...",
      "token0_symbol": "ZIL",
      "token0_name": "Zilliqa",
      "token1_symbol": "USDT",
      "token1_name": "Tether USD",
      "reserve0": "1000000000000000000",
      "reserve1": "50000000",
      "liquidity_usd": "125000.50",
      "volume_usd_24h": "12345.67",
      "price_change_24h": "2.5",
      "price_change_7d": "-1.2"
    }
  ]
}
```

### Individual Pair Channel: `dex.pair.<address>`

Subscribe to updates for a specific pair by its address (lowercase).

**JavaScript Example:**
```javascript
const pairAddress = "0xabc123...".toLowerCase();
const subscription = centrifuge.newSubscription(`dex.pair.${pairAddress}`);

subscription.on('publication', (ctx) => {
  const update = ctx.data;
  console.log('Pair update:', update);
  // update.type === "pair.update"
  // update.pair (pair summary object)
});

subscription.subscribe();
```

**Message Format:**
```json
{
  "type": "pair.update",
  "block_number": 8470101,
  "ts": 1731698202,
  "pair": {
    "protocol": "uniswap_v2",
    "address": "0xabc123...",
    "token0": "0xdef456...",
    "token1": "0x789abc...",
    "token0_symbol": "ZIL",
    "token0_name": "Zilliqa",
    "token1_symbol": "USDT",
    "token1_name": "Tether USD",
    "reserve0": "1000000000000000000",
    "reserve1": "50000000",
    "liquidity_usd": "125000.50",
    "volume_usd_24h": "12345.67",
    "price_change_24h": "2.5",
    "price_change_7d": "-1.2"
  }
}
```

## Client Libraries

Centrifugo provides official client libraries for multiple platforms:

- **JavaScript/TypeScript**: [centrifuge-js](https://www.npmjs.com/package/centrifuge)
- **Go**: [centrifuge-go](https://github.com/centrifugal/centrifuge-go)
- **Swift**: [centrifuge-swift](https://github.com/centrifugal/centrifuge-swift)
- **Dart**: [centrifuge-dart](https://github.com/centrifugal/centrifuge-dart)

## Update Frequency

- Updates are batched every **250ms** to optimize performance
- Updates are also flushed at block boundaries (~1 second on Zilliqa)
- Pair data is updated when Swap, Mint, Burn, or Sync events occur

## History & Recovery

Centrifugo is configured with:
- **History Size**: 100 messages per channel
- **History TTL**: 15 seconds for `dex.pairs`, 120 seconds for individual pair channels
- **Recovery**: Enabled - clients can recover missed messages during brief disconnections

To enable recovery in your client:
```javascript
const subscription = centrifuge.newSubscription('dex.pairs', {
  recoverable: true
});
```

## Best Practices

### Initial State
Always fetch initial state via REST API before subscribing to WebSocket updates:

```javascript
// 1. Fetch initial data
const pairs = await fetch('/pairs').then(r => r.json());

// 2. Subscribe to updates
const subscription = centrifuge.newSubscription('dex.pairs');
subscription.on('publication', (ctx) => {
  // Merge update into local state
  updateLocalState(ctx.data);
});
subscription.subscribe();
```

### Handling Stale Updates
Use `block_number` to discard stale updates:

```javascript
let lastBlockNumber = 0;

subscription.on('publication', (ctx) => {
  const { block_number, items } = ctx.data;
  
  if (block_number < lastBlockNumber) {
    console.log('Ignoring stale update');
    return;
  }
  
  lastBlockNumber = block_number;
  updateUI(items);
});
```

### Reconnection
Centrifuge client handles reconnection automatically with exponential backoff. On reconnection failure or long disconnection, refetch via REST:

```javascript
centrifuge.on('disconnected', (ctx) => {
  if (ctx.code === 3500) { // Unauthorized
    refreshToken();
  }
});

centrifuge.on('connected', (ctx) => {
  console.log('Connected:', ctx);
});
```

## Environment Variables

Configure WebSocket URLs via environment variables:

- `CENTRIFUGO_API_URL`: Internal API URL for publishing (e.g., `http://centrifugo:8001/api`)
- `CENTRIFUGO_WS_URL`: Public WebSocket URL for clients (e.g., `ws://localhost:8001/connection/websocket`)
- `CENTRIFUGO_JWT_SECRET`: Secret for signing JWT tokens
- `CENTRIFUGO_API_KEY`: API key for publishing to Centrifugo

## Configuration

### config.yaml
```yaml
centrifugo:
  api_url: "http://localhost:8001/api"
  api_key: "your-api-key-here"
  ws_url: "ws://localhost:8001/connection/websocket"
  jwt_secret: "your-jwt-secret-here"
  enabled: true
```

### docker-compose.yml
The indexer includes a Centrifugo service:

```yaml
centrifugo:
  image: centrifugo/centrifugo:v5
  container_name: zilstream-centrifugo
  command: centrifugo --config=/centrifugo/config.json
  volumes:
    - ./config/centrifugo.json:/centrifugo/config.json:ro
  ports:
    - "8001:8001"
```

## Monitoring

Check Centrifugo health:
```bash
curl http://localhost:8001/health
```

Monitor active channels and clients via Centrifugo admin panel (if enabled) or API.
