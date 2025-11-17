# Real-Time WebSocket Updates - Setup Guide

This guide explains how to set up and run the real-time WebSocket updates using Centrifugo.

## Quick Start

### 1. Configure Secrets

Edit `config/centrifugo.json` and replace placeholders:

```json
{
  "token_hmac_secret_key": "CHANGE_ME_IN_PRODUCTION",
  "api_key": "CHANGE_ME_IN_PRODUCTION",
  ...
}
```

Generate secure secrets:
```bash
# Generate random secrets
openssl rand -hex 32  # For token_hmac_secret_key
openssl rand -hex 32  # For api_key
```

Edit `config.yaml`:
```yaml
centrifugo:
  api_url: "http://localhost:8001/api"
  api_key: "YOUR_API_KEY_HERE"
  ws_url: "ws://localhost:8001/connection/websocket"
  jwt_secret: "YOUR_JWT_SECRET_HERE"
  enabled: true
```

### 2. Start Services

Using Docker Compose:
```bash
docker-compose up -d
```

Or run separately:

**Terminal 1 - Centrifugo:**
```bash
docker run -p 8001:8001 \
  -v $(pwd)/config/centrifugo.json:/centrifugo/config.json \
  centrifugo/centrifugo:v5 centrifugo --config=/centrifugo/config.json
```

**Terminal 2 - Indexer:**
```bash
go run cmd/server/main.go -config config.yaml
```

### 3. Test WebSocket Connection

```bash
# Get connection info
curl http://localhost:8080/ws/info

# Get connection token
curl -X POST http://localhost:8080/ws/token
```

## Architecture

```
┌─────────────────┐
│   Blockchain    │
│   (Zilliqa)     │
└────────┬────────┘
         │
         │ Events (Swap, Mint, Burn, Sync)
         ▼
┌─────────────────┐
│    Indexer      │
│  - Uniswap V2   │◄─── Processes events
│    Module       │     Updates database
└────────┬────────┘     Enqueues changes
         │
         │ EnqueuePairChanged()
         ▼
┌─────────────────┐
│   Publisher     │
│  - 250ms batch  │◄─── Batches updates
│  - Block flush  │     Fetches summaries
└────────┬────────┘     Publishes to Centrifugo
         │
         │ Publish (HTTP API)
         ▼
┌─────────────────┐
│  Centrifugo     │
│   - Channels    │◄─── Broadcasts to subscribers
│   - History     │     Handles reconnection
└────────┬────────┘
         │
         │ WebSocket
         ▼
┌─────────────────┐
│   Clients       │
│  (Web/Mobile)   │
└─────────────────┘
```

## Data Flow

1. **Event Processing**: Uniswap V2 module processes Swap/Mint/Burn/Sync events
2. **Pair Tracking**: Changed pairs are collected during batch processing
3. **Publisher Enqueueing**: `publisher.EnqueuePairChanged(address)` queues updates
4. **Batching**: Publisher batches for 250ms or until block boundary
5. **Database Query**: Fetches current pair summaries from `dex_pools` view
6. **Publishing**:
   - Individual channel: `dex.pair.<address>`
   - Batch channel: `dex.pairs`
7. **Client Updates**: Connected clients receive JSON messages

## Channels

### `dex.pairs` - Batch Updates
All pair changes in batched format (recommended for overview pages).

**Message:**
```json
{
  "type": "pair.batch",
  "block_number": 8470100,
  "ts": 1731698201,
  "items": [/* array of pair summaries */]
}
```

### `dex.pair.<address>` - Individual Pair
Dedicated channel for specific pair (recommended for detail pages).

**Message:**
```json
{
  "type": "pair.update",
  "block_number": 8470101,
  "ts": 1731698202,
  "pair": {/* pair summary object */}
}
```

## Configuration Options

### Centrifugo (`config/centrifugo.json`)

- **`token_hmac_secret_key`**: Secret for JWT token signing
- **`api_key`**: API key for server-side publishing
- **`allow_subscribe_for_anonymous`**: Allow unauthenticated subscriptions (true)
- **`allow_publish_for_client`**: Prevent client publishing (false)
- **`history_size`**: Number of messages to keep (100)
- **`history_ttl`**: How long to keep history (15s for batch, 120s for individual)

### Indexer (`config.yaml`)

```yaml
centrifugo:
  api_url: "http://localhost:8001/api"      # Internal publish endpoint
  api_key: "your-api-key"                    # Must match Centrifugo api_key
  ws_url: "ws://localhost:8001/connection/websocket"  # Public WebSocket URL
  jwt_secret: "your-jwt-secret"              # Must match token_hmac_secret_key
  enabled: true                               # Enable/disable real-time updates
```

## Environment Variables

Override config via environment variables:

```bash
export CENTRIFUGO_API_URL="http://centrifugo:8001/api"
export CENTRIFUGO_API_KEY="your-api-key"
export CENTRIFUGO_WS_URL="wss://your-domain.com/connection/websocket"
export CENTRIFUGO_JWT_SECRET="your-jwt-secret"
```

## Production Deployment

### 1. Secure Configuration

- Use strong random secrets (32+ characters)
- Store secrets in environment variables or secret management
- Use TLS/WSS for WebSocket connections
- Enable CORS with specific origins

### 2. Update Centrifugo Config

```json
{
  "token_hmac_secret_key": "ENV:CENTRIFUGO_JWT_SECRET",
  "api_key": "ENV:CENTRIFUGO_API_KEY",
  "allowed_origins": ["https://your-frontend-domain.com"],
  "namespaces": [...]
}
```

### 3. Update Docker Compose

```yaml
centrifugo:
  image: centrifugo/centrifugo:v5
  environment:
    - CENTRIFUGO_JWT_SECRET=${CENTRIFUGO_JWT_SECRET}
    - CENTRIFUGO_API_KEY=${CENTRIFUGO_API_KEY}
  ports:
    - "8001:8001"
  restart: always
```

### 4. Nginx/Load Balancer

```nginx
# WebSocket proxy
location /connection/websocket {
    proxy_pass http://centrifugo:8001;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_read_timeout 86400;
}
```

## Monitoring

### Health Checks

```bash
# Centrifugo health
curl http://localhost:8001/health

# Indexer API health
curl http://localhost:8080/health
```

### Metrics

Centrifugo exposes Prometheus metrics on `/metrics`:

```bash
curl http://localhost:8001/metrics
```

Key metrics:
- `centrifugo_node_num_clients` - Connected clients
- `centrifugo_node_num_channels` - Active channels
- `centrifugo_node_num_msg_published` - Messages published
- `centrifugo_node_num_msg_queued` - Queued messages

### Logs

Enable debug logging in Centrifugo:
```json
{
  "log_level": "debug"
}
```

## Troubleshooting

### Clients Can't Connect

1. Check Centrifugo is running: `curl http://localhost:8001/health`
2. Verify token generation: `curl -X POST http://localhost:8080/ws/token`
3. Check CORS configuration in `config/centrifugo.json`
4. Inspect browser console for WebSocket errors

### No Updates Received

1. Check `centrifugo.enabled` is `true` in config.yaml
2. Verify API key matches between config.yaml and centrifugo.json
3. Check indexer logs for "Realtime publisher enabled"
4. Verify events are being indexed (check database)
5. Monitor publisher flush logs: `"Enqueued pairs for realtime updates"`

### High Memory Usage

1. Reduce `history_size` in centrifugo.json (default: 100)
2. Reduce `history_ttl` (default: 15s for batch, 120s for individual)
3. Increase flush interval (modify 250ms in `internal/realtime/publisher.go`)

### Stale Data

1. Ensure block numbers are increasing
2. Check `last_updated_at` timestamps in database
3. Verify publisher is flushing at block boundaries
4. Client should refetch via REST API on long disconnection

## Client Examples

See [WEBSOCKETS.md](WEBSOCKETS.md) for detailed client examples in JavaScript/TypeScript.

Basic example:
```javascript
import { Centrifuge } from 'centrifuge';

const { url } = await fetch('/ws/info').then(r => r.json());
const { token } = await fetch('/ws/token', { method: 'POST' }).then(r => r.json());

const centrifuge = new Centrifuge(url, { token });

const sub = centrifuge.newSubscription('dex.pairs');
sub.on('publication', (ctx) => {
  console.log('Update:', ctx.data);
});
sub.subscribe();

centrifuge.connect();
```

## Reference

- [Centrifugo Documentation](https://centrifugal.dev/)
- [WebSocket API Documentation](WEBSOCKETS.md)
- [Configuration Files](../config/)
