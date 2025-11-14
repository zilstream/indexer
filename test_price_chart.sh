#!/bin/bash
# Test script for price chart endpoint

# Get a V2 pool address
V2_POOL=$(PGPASSWORD="" psql -h localhost -U postgres -d zilstream -t -c "SELECT address FROM dex_pools WHERE protocol = 'uniswap_v2' AND liquidity_usd > 0 LIMIT 1;" | xargs)
echo "Testing with V2 pool: $V2_POOL"

# Get a V3 pool address
V3_POOL=$(PGPASSWORD="" psql -h localhost -U postgres -d zilstream -t -c "SELECT address FROM dex_pools WHERE protocol = 'uniswap_v3' AND liquidity_usd > 0 LIMIT 1;" | xargs)
echo "Testing with V3 pool: $V3_POOL"

# Start the server in background (API only mode)
echo "Starting API server..."
./bin/server -config config.yaml -role api > /tmp/indexer.log 2>&1 &
SERVER_PID=$!

# Wait for server to be ready
echo "Waiting for API server to be ready..."
for i in {1..10}; do
  if curl -s http://localhost:8080/health > /dev/null 2>&1; then
    echo "Server is ready!"
    break
  fi
  sleep 1
done

# Test V2 endpoint
echo -e "\n=== Testing V2 Pool Price Chart ==="
curl -s "http://localhost:8080/pairs/$V2_POOL/chart/price" | jq -C '.'

# Test V3 endpoint if available
if [ -n "$V3_POOL" ]; then
  echo -e "\n=== Testing V3 Pool Price Chart ==="
  curl -s "http://localhost:8080/pairs/$V3_POOL/chart/price" | jq -C '.'
fi

# Cleanup
kill $SERVER_PID 2>/dev/null
echo -e "\nTest complete"
