# Kamal Deployment - Quick Start

Quick reference for deploying ZilStream Indexer with Centrifugo.

## 🚀 First-Time Setup

### 1. Add Secrets to 1Password

```bash
# Generate secrets
openssl rand -hex 32  # CENTRIFUGO_API_KEY
openssl rand -hex 32  # CENTRIFUGO_JWT_SECRET
```

Add to 1Password vault: `ZilStream/StreamSecrets`

### 2. Deploy Centrifugo

```bash
kamal accessory boot centrifugo
```

### 3. Deploy Application

```bash
kamal deploy
```

## 📋 Common Commands

### Application
```bash
kamal deploy                    # Full deploy (app + accessories)
kamal app deploy               # Deploy only app
kamal app restart              # Restart app
kamal app logs -f              # Follow logs
kamal app exec -i bash         # SSH into container
```

### Centrifugo Accessory
```bash
kamal accessory boot centrifugo      # Deploy Centrifugo
kamal accessory reboot centrifugo    # Restart Centrifugo
kamal accessory logs centrifugo -f   # Follow logs
kamal accessory details centrifugo   # Show status
kamal accessory stop centrifugo      # Stop
kamal accessory remove centrifugo    # Remove
```

### Proxy
```bash
kamal proxy reboot             # Restart Traefik
kamal proxy logs -f            # Follow proxy logs
kamal proxy config             # Show config
```

### Secrets
```bash
kamal secrets fetch --adapter 1password \
  --account XO76NS4A4ZBNVA6D3XIYJ4KIRM \
  --from ZilStream/StreamSecrets \
  CENTRIFUGO_API_KEY CENTRIFUGO_JWT_SECRET
```

## 🔍 Troubleshooting

### Check if everything is running
```bash
kamal details                       # Overall status
kamal accessory details centrifugo  # Centrifugo status
```

### Test connectivity
```bash
# From server
curl http://localhost:8001/health

# From app container
kamal app exec -i curl http://172.18.0.1:8001/health

# From internet
curl https://v2-api.zilstream.com/ws/info
curl -X POST https://v2-api.zilstream.com/ws/token
```

### View logs
```bash
kamal app logs -f                  # App logs
kamal accessory logs centrifugo -f # Centrifugo logs
kamal proxy logs -f                # Proxy logs
```

## 🔧 Configuration Files

- **`config/deploy.yml`** - Main Kamal config
- **`.kamal/secrets`** - Secrets management
- **`config/centrifugo.json`** - Centrifugo config
- **`config.yaml`** - App config (local dev)

## 📚 Full Documentation

- **[Kamal Deployment Guide](docs/KAMAL_DEPLOYMENT.md)** - Complete deployment docs
- **[WebSocket API](docs/WEBSOCKETS.md)** - Client integration guide  
- **[Real-Time Setup](docs/REALTIME_SETUP.md)** - Configuration details

## ⚡ Quick Deploy Workflow

```bash
# 1. Make changes to code
git add .
git commit -m "Your changes"

# 2. Deploy only the app (Centrifugo keeps running)
kamal app deploy

# 3. Watch logs
kamal app logs -f
```

## 🎯 Production Checklist

- [ ] Secrets in 1Password
- [ ] Centrifugo deployed: `kamal accessory details centrifugo`
- [ ] App deployed: `kamal app details`
- [ ] WebSocket working: Test at `/ws/info` and `/ws/token`
- [ ] Real-time updates working: Subscribe to `dex.pairs`
- [ ] Monitoring logs for errors

## 🆘 Emergency Rollback

```bash
kamal app rollback    # Rollback to previous version
```

## 📞 Support

Check logs first:
```bash
kamal app logs -n 100           # Last 100 lines
kamal accessory logs centrifugo -n 100
```

SSH into server if needed:
```bash
ssh indexer
docker ps
docker logs zilstream-indexer-centrifugo
```
