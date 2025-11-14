-- Debug queries for SEED token price change calculation

-- 1. Check what stablecoins are recognized
SELECT address, symbol, price_usd 
FROM tokens 
WHERE symbol ILIKE ANY(ARRAY['USDT', 'USDC', 'DAI', 'BUSD', 'ZUSDT', 'zUSDT', 'ZUSD', 'XSGD', 'kUSD']);

-- 2. Check SEED's pools and their liquidity/volume weights
SELECT 
    protocol,
    address AS pool,
    token0, token1,
    COALESCE(liquidity_usd,0) + COALESCE(volume_usd_24h,0) AS weight
FROM dex_pools
WHERE lower(token0) = lower('0xe64ca52ef34fdd7e20c0c7fb2e392cc9b4f6d049')
   OR lower(token1) = lower('0xe64ca52ef34fdd7e20c0c7fb2e392cc9b4f6d049')
ORDER BY weight DESC;

-- 3. Check current SEED price and metrics
SELECT address, symbol, price_usd, price_change_24h, price_change_7d, total_liquidity_usd
FROM tokens
WHERE lower(address) = lower('0xe64ca52ef34fdd7e20c0c7fb2e392cc9b4f6d049');

-- 4. Check SEED/kUSD V2 pair - current reserves
SELECT 
    p.address,
    p.reserve0, p.reserve1,
    t0.symbol AS token0_symbol, t0.decimals AS dec0,
    t1.symbol AS token1_symbol, t1.decimals AS dec1,
    p.reserve_usd
FROM uniswap_v2_pairs p
LEFT JOIN tokens t0 ON t0.address = p.token0
LEFT JOIN tokens t1 ON t1.address = p.token1
WHERE lower(p.address) = lower('0x2f9a9d490e5615312c50b840dbadfb9961133cae');

-- 5. Check latest sync for SEED/kUSD pair
SELECT timestamp, reserve0, reserve1,
    to_timestamp(timestamp) AS sync_time
FROM uniswap_v2_syncs
WHERE lower(pair) = lower('0x2f9a9d490e5615312c50b840dbadfb9961133cae')
ORDER BY timestamp DESC
LIMIT 5;

-- 6. Check sync 24h ago for SEED/kUSD pair
SELECT timestamp, reserve0, reserve1,
    to_timestamp(timestamp) AS sync_time,
    EXTRACT(EPOCH FROM (now() AT TIME ZONE 'UTC')) - timestamp AS seconds_ago
FROM uniswap_v2_syncs
WHERE lower(pair) = lower('0x2f9a9d490e5615312c50b840dbadfb9961133cae')
  AND timestamp <= EXTRACT(EPOCH FROM (now() AT TIME ZONE 'UTC' - interval '24 hours'))
ORDER BY timestamp DESC
LIMIT 5;

-- 7. Test the actual price calculation for current state
WITH pair_info AS (
    SELECT '0x2f9a9d490e5615312c50b840dbadfb9961133cae' AS pair_addr,
           '0xe64ca52ef34fdd7e20c0c7fb2e392cc9b4f6d049' AS token0,
           '0xe9df5b4b1134a3aadf693db999786699b016239e' AS token1,
           18 AS dec0, 6 AS dec1
),
stables AS (
    SELECT ARRAY(SELECT lower(address) FROM tokens WHERE symbol ILIKE ANY(ARRAY['USDT', 'USDC', 'DAI', 'BUSD', 'ZUSDT', 'zUSDT', 'ZUSD', 'XSGD', 'kUSD'])) AS addrs
),
latest_sync AS (
    SELECT reserve0, reserve1
    FROM uniswap_v2_syncs
    WHERE lower(pair) = (SELECT pair_addr FROM pair_info)
    ORDER BY timestamp DESC
    LIMIT 1
)
SELECT 
    s.reserve0, s.reserve1,
    (s.reserve0::numeric / POWER(10, 18))::text AS reserve0_formatted,
    (s.reserve1::numeric / POWER(10, 6))::text AS reserve1_formatted,
    ((s.reserve1::numeric / POWER(10, 6)) / NULLIF(s.reserve0::numeric / POWER(10, 18), 0))::text AS calculated_price,
    CASE 
        WHEN lower((SELECT token1 FROM pair_info)) = ANY(SELECT unnest(addrs) FROM stables) THEN 'YES - kUSD is recognized as stable'
        ELSE 'NO - kUSD not recognized'
    END AS is_token1_stable
FROM latest_sync s
CROSS JOIN stables;
