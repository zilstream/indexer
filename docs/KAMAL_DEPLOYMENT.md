# Kamal v2 Deployment Guide - Centrifugo Setup

This guide covers deploying the ZilStream indexer with Centrifugo using Kamal v2.

## Overview

Centrifugo is configured as a Kamal **accessory**, which means it:
- Runs as a separate Docker container on your server
- Is managed by Kamal (start, stop, restart, logs)
- Gets deployed alongside your main application
- Persists across application deployments

## Prerequisites

1. **Kamal v2** installed locally
2. **1Password** (or another secrets manager) with Centrifugo secrets
3. **Server** with Docker installed
4. **SSL certificate** (handled by Kamal proxy via Let's Encrypt)

## Configuration Files Updated

The following files have been configured for Kamal deployment:

1. **`config/deploy.yml`** - Main Kamal configuration
2. **`.kamal/secrets`** - Secrets management
3. **`config/centrifugo.json`** - Centrifugo config with env vars

## Step 1: Add Secrets to 1Password

Add the following secrets to your 1Password vault (`ZilStream/StreamSecrets`):

### Generate Secrets

```bash
# Generate random 32-character secrets
openssl rand -hex 32  # For CENTRIFUGO_API_KEY
openssl rand -hex 32  # For CENTRIFUGO_JWT_SECRET
```

### Add to 1Password

Using 1Password CLI or web interface, add:

- **Field Name**: `CENTRIFUGO_API_KEY`
  **Value**: `<generated-api-key>`

- **Field Name**: `CENTRIFUGO_JWT_SECRET`
  **Value**: `<generated-jwt-secret>`

### Verify Secrets

```bash
# Test that Kamal can fetch the secrets
kamal secrets fetch --adapter 1password \
  --account XO76NS4A4ZBNVA6D3XIYJ4KIRM \
  --from ZilStream/StreamSecrets \
  CENTRIFUGO_API_KEY CENTRIFUGO_JWT_SECRET
```

## Step 2: Update config.yaml (Local Development)

For local development, update your `config.yaml`:

```yaml
centrifugo:
  api_url: "http://localhost:8001/api"
  api_key: "your-local-api-key"  # Same as CENTRIFUGO_API_KEY
  ws_url: "ws://localhost:8001/connection/websocket"
  jwt_secret: "your-local-jwt-secret"  # Same as CENTRIFUGO_JWT_SECRET
  enabled: true
```

## Step 3: Deploy Centrifugo Accessory

### First-Time Setup

Deploy only the Centrifugo accessory:

```bash
kamal accessory boot centrifugo
```

This will:
1. Pull the `centrifugo/centrifugo:v5` image
2. Upload `config/centrifugo.json` to the server
3. Inject secrets from 1Password
4. Start Centrifugo on port 8001

### Verify Deployment

```bash
# Check if Centrifugo is running
kamal accessory details centrifugo

# View logs
kamal accessory logs centrifugo

# Test health endpoint (SSH into server)
curl http://localhost:8001/health
```

## Step 4: Configure Proxy for WebSocket

Kamal's Traefik proxy needs special configuration for WebSocket connections. This is already configured in `config/deploy.yml`:

```yaml
proxy:
  response_timeout: 3600      # Long timeout for WebSocket connections
  buffering:
    requests: false           # Disable buffering for real-time
    responses: false
```

### Add Centrifugo Route to Proxy

Since Centrifugo runs on port 8001 and the indexer on 8080, we need to route WebSocket traffic. Create a custom Traefik configuration:

**Option A: Use Kamal proxy labels (Recommended)**

Add to your `config/deploy.yml` under the main app:

```yaml
servers:
  web:
    hosts:
      - indexer
    labels:
      # Route /ws/* to Centrifugo accessory
      traefik.http.routers.centrifugo-ws.rule: "Host(`v2-api.zilstream.com`) && PathPrefix(`/ws`)"
      traefik.http.routers.centrifugo-ws.entrypoints: "websecure"
      traefik.http.routers.centrifugo-ws.tls.certresolver: "letsencrypt"
      traefik.http.services.centrifugo-ws.loadbalancer.server.port: "8001"
      traefik.http.services.centrifugo-ws.loadbalancer.server.scheme: "http"
```

**Option B: Direct Centrifugo on subdomain**

Alternatively, expose Centrifugo on a separate subdomain:

```yaml
accessories:
  centrifugo:
    image: centrifugo/centrifugo:v5
    host: indexer
    port: 8001
    labels:
      traefik.enable: true
      traefik.http.routers.centrifugo.rule: "Host(`ws.zilstream.com`)"
      traefik.http.routers.centrifugo.entrypoints: "websecure"
      traefik.http.routers.centrifugo.tls.certresolver: "letsencrypt"
```

Then update your app's `CENTRIFUGO_WS_URL`:
```yaml
env:
  clear:
    CENTRIFUGO_WS_URL: wss://ws.zilstream.com/connection/websocket
```

## Step 5: Deploy Main Application

Deploy the indexer with Centrifugo integration:

```bash
# Full deployment
kamal deploy

# Or just redeploy the app (Centrifugo keeps running)
kamal app deploy
```

The application will now:
1. Connect to Centrifugo via `http://172.18.0.1:8001/api` (internal Docker network)
2. Expose WebSocket to clients via `wss://v2-api.zilstream.com/ws`

## Step 6: Test Real-Time Updates

### From Your Server

```bash
# SSH into server
kamal app exec -i bash

# Inside container, test API endpoints
curl http://localhost:8080/ws/info
curl -X POST http://localhost:8080/ws/token

# Test Centrifugo is reachable
curl http://172.18.0.1:8001/health
```

### From Client

```javascript
// Get connection info
const info = await fetch('https://v2-api.zilstream.com/ws/info').then(r => r.json());
console.log('WebSocket URL:', info.url);

// Get token
const { token } = await fetch('https://v2-api.zilstream.com/ws/token', { 
  method: 'POST' 
}).then(r => r.json());

// Connect
import { Centrifuge } from 'centrifuge';
const centrifuge = new Centrifuge(info.url, { token });
centrifuge.connect();
```

## Management Commands

### Accessory Management

```bash
# View Centrifugo status
kamal accessory details centrifugo

# View logs
kamal accessory logs centrifugo

# Follow logs in real-time
kamal accessory logs centrifugo -f

# Restart Centrifugo
kamal accessory reboot centrifugo

# Stop Centrifugo
kamal accessory stop centrifugo

# Start Centrifugo
kamal accessory start centrifugo

# Remove Centrifugo
kamal accessory remove centrifugo
```

### Application Management

```bash
# Deploy everything (app + accessories)
kamal deploy

# Deploy only the app (Centrifugo keeps running)
kamal app deploy

# Rollback deployment
kamal app rollback

# View app logs
kamal app logs -f
```

### Secrets Management

```bash
# Test secrets fetch
kamal secrets fetch --adapter 1password \
  --account XO76NS4A4ZBNVA6D3XIYJ4KIRM \
  --from ZilStream/StreamSecrets \
  CENTRIFUGO_API_KEY CENTRIFUGO_JWT_SECRET

# Extract a specific secret
SECRETS=$(kamal secrets fetch --adapter 1password \
  --account XO76NS4A4ZBNVA6D3XIYJ4KIRM \
  --from ZilStream/StreamSecrets \
  CENTRIFUGO_API_KEY)
echo $SECRETS | kamal secrets extract CENTRIFUGO_API_KEY -
```

## Monitoring & Debugging

### Check Connectivity

```bash
# SSH into server
ssh indexer

# Check if Centrifugo container is running
docker ps | grep centrifugo

# Check container logs
docker logs zilstream-indexer-centrifugo

# Test internal connectivity from app container
docker exec -it zilstream-indexer-web curl http://172.18.0.1:8001/health

# Check Centrifugo stats
curl http://localhost:8001/health
curl http://localhost:8001/stats
```

### Common Issues

**Issue: WebSocket connection refused**
```bash
# Check if Centrifugo is running
kamal accessory details centrifugo

# Check proxy configuration
kamal proxy config

# Restart proxy
kamal proxy reboot
```

**Issue: App can't connect to Centrifugo**
```bash
# Check Docker network
docker network inspect kamal

# Verify 172.18.0.1 is the gateway
docker exec zilstream-indexer-web ip route | grep default

# Test from app container
kamal app exec -i curl http://172.18.0.1:8001/health
```

**Issue: Secrets not loading**
```bash
# Verify 1Password access
op account list

# Test secret fetch
kamal secrets fetch --adapter 1password \
  --account XO76NS4A4ZBNVA6D3XIYJ4KIRM \
  --from ZilStream/StreamSecrets CENTRIFUGO_API_KEY

# Check environment variables in container
kamal accessory exec centrifugo -i env | grep CENTRIFUGO
```

## Production Checklist

- [ ] Secrets added to 1Password vault
- [ ] `config/centrifugo.json` uses environment variables
- [ ] `config/deploy.yml` references secrets correctly
- [ ] SSL/TLS configured (Kamal proxy handles this)
- [ ] CORS origins restricted in `centrifugo.json` (currently `["*"]`)
- [ ] WebSocket proxy timeout set to 3600s
- [ ] Tested WebSocket connection from client
- [ ] Verified real-time updates are working
- [ ] Monitored Centrifugo logs for errors

## Updating Centrifugo

### Update Configuration

1. Edit `config/centrifugo.json`
2. Reboot the accessory:
```bash
kamal accessory reboot centrifugo
```

### Update Image Version

1. Edit `config/deploy.yml` and change `centrifugo/centrifugo:v5` to newer version
2. Pull new image and reboot:
```bash
kamal accessory reboot centrifugo
```

## CORS Configuration for Production

For production, restrict CORS in `config/centrifugo.json`:

```json
{
  "allowed_origins": [
    "https://zilstream.com",
    "https://www.zilstream.com",
    "https://app.zilstream.com"
  ]
}
```

Then redeploy:
```bash
kamal accessory reboot centrifugo
```

## Architecture Diagram

```
┌─────────────────────────────────────────────────────┐
│                  Kamal Server                        │
│  ┌──────────────────────────────────────────────┐   │
│  │           Traefik Proxy (kamal-proxy)        │   │
│  │  ┌────────────────┐  ┌──────────────────┐   │   │
│  │  │ v2-api.zilstream.com │ /ws → Centrifugo│   │   │
│  │  │ /* → Indexer   │  │  (port 8001)     │   │   │
│  │  └────────────────┘  └──────────────────┘   │   │
│  └──────────────────────────────────────────────┘   │
│                                                      │
│  ┌──────────────────┐       ┌──────────────────┐   │
│  │  Indexer App     │◄─────►│  Centrifugo      │   │
│  │  (zilstream-     │  HTTP │  (zilstream-     │   │
│  │   indexer-web)   │  API  │   indexer-       │   │
│  │  Port: 8080      │       │   centrifugo)    │   │
│  └──────────────────┘       └──────────────────┘   │
│          │                           ▲              │
│          │                           │              │
│          ▼                           │              │
│  ┌──────────────────┐                │              │
│  │   PostgreSQL     │                │              │
│  │   (Host)         │                │              │
│  │   Port: 5432     │                │              │
│  └──────────────────┘                │              │
│                                       │              │
└───────────────────────────────────────┼──────────────┘
                                        │
                                        │ WSS
                                        ▼
                              ┌──────────────────┐
                              │  Client Apps     │
                              │  (Web/Mobile)    │
                              └──────────────────┘
```

## Reference

- [Kamal Documentation](https://kamal-deploy.org/)
- [Centrifugo Documentation](https://centrifugal.dev/)
- [WebSocket API Guide](WEBSOCKETS.md)
- [Real-Time Setup Guide](REALTIME_SETUP.md)
