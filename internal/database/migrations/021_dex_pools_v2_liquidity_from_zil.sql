-- Migration 021: Compute V2 liquidity_usd for WZIL pairs from current ZIL price
-- Instead of using token0's aggregated price, calculate from the pool's actual reserves/sqrt_price
--
-- V3 formula: (sqrt_price_x96^2 / 2^192) / 10^(dec1-dec0) = token0 price in token1 (human units)
-- V2 formula: (reserve1/10^dec1) / (reserve0/10^dec0) = token0 price in token1 (human units)

DROP VIEW IF EXISTS dex_pools CASCADE;

CREATE OR REPLACE VIEW dex_pools AS
WITH zil_price AS (
    SELECT COALESCE(price, 0) AS usd
    FROM prices_zil_usd_minute
    ORDER BY ts DESC
    LIMIT 1
),
wzil AS (
    SELECT '0x94e18ae7dd5ee57b55f30c4b63e2760c09efb192' AS addr
),
stablecoins AS (
    SELECT LOWER(address) AS addr FROM tokens
    WHERE UPPER(symbol) IN ('USDT', 'USDC', 'DAI', 'BUSD', 'ZUSDT', 'ZUSD', 'XSGD', 'KUSD')
)
SELECT
    'uniswap_v2'::text               AS protocol,
    p.address                        AS address,
    p.token0                         AS token0,
    p.token1                         AS token1,
    t0.symbol                        AS token0_symbol,
    t0.name                          AS token0_name,
    t1.symbol                        AS token1_symbol,
    t1.name                          AS token1_name,
    NULL::numeric                    AS fee,
    p.reserve0                       AS reserve0,
    p.reserve1                       AS reserve1,
    NULL::numeric                    AS liquidity,
    CASE
        WHEN LOWER(p.token0) = (SELECT addr FROM wzil) THEN
            ((p.reserve0::numeric / POWER(10::numeric, COALESCE(t0.decimals, 18))) * 2 * zp.usd)
        WHEN LOWER(p.token1) = (SELECT addr FROM wzil) THEN
            ((p.reserve1::numeric / POWER(10::numeric, COALESCE(t1.decimals, 18))) * 2 * zp.usd)
        ELSE p.reserve_usd
    END                              AS liquidity_usd,
    p.volume_usd                     AS volume_usd,
    NULL::numeric                    AS fees_usd,
    -- V2 price calculation
    -- rate = (reserve1/10^dec1) / (reserve0/10^dec0) = token0 price in token1
    -- After normalization in Go, WZIL/stablecoin becomes token1, so we compute the "interesting" token's price
    CASE
        -- token0 is WZIL, token1 is stablecoin → will swap to stablecoin/WZIL, show stablecoin's price
        -- rate = WZIL price in stablecoin, so stablecoin_usd = (1/rate) * zil_price
        WHEN LOWER(p.token0) = (SELECT addr FROM wzil) AND LOWER(p.token1) IN (SELECT addr FROM stablecoins) THEN
            CASE WHEN p.reserve0 > 0 AND p.reserve1 > 0 THEN
                ((p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18))) /
                 NULLIF(p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18)), 0)) * zp.usd
            ELSE NULL END
        -- token0 is WZIL (token1 is regular token) → will swap, compute token1's USD price
        WHEN LOWER(p.token0) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.reserve0 > 0 AND p.reserve1 > 0 THEN
                ((p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18))) /
                 NULLIF(p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18)), 0)) * zp.usd
            ELSE NULL END
        -- token0 is stablecoin, token1 is WZIL → will swap to WZIL/stablecoin, show WZIL's price
        WHEN LOWER(p.token0) IN (SELECT addr FROM stablecoins) AND LOWER(p.token1) = (SELECT addr FROM wzil) THEN
            zp.usd
        -- token0 is stablecoin (token1 is regular token) → will swap, compute token1's USD price
        WHEN LOWER(p.token0) IN (SELECT addr FROM stablecoins) THEN
            CASE WHEN p.reserve0 > 0 AND p.reserve1 > 0 THEN
                ((p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18))) /
                 NULLIF(p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18)), 0)) * COALESCE(t0.price_usd, 1)
            ELSE NULL END
        -- token1 is WZIL → token0 stays, compute token0's USD price
        WHEN LOWER(p.token1) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.reserve0 > 0 AND p.reserve1 > 0 THEN
                ((p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18))) /
                 NULLIF(p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18)), 0)) * zp.usd
            ELSE NULL END
        -- token1 is stablecoin → token0 stays, compute token0's USD price
        WHEN LOWER(p.token1) IN (SELECT addr FROM stablecoins) THEN
            CASE WHEN p.reserve0 > 0 AND p.reserve1 > 0 THEN
                ((p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18))) /
                 NULLIF(p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18)), 0)) * COALESCE(t1.price_usd, 1)
            ELSE NULL END
        ELSE t0.price_usd
    END                              AS price_usd,
    -- price_eth = price in WZIL terms
    CASE
        WHEN LOWER(p.token0) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.reserve0 > 0 AND p.reserve1 > 0 THEN
                (p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18))) /
                NULLIF(p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18)), 0)
            ELSE NULL END
        WHEN LOWER(p.token1) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.reserve0 > 0 AND p.reserve1 > 0 THEN
                (p.reserve1::numeric / POWER(10, COALESCE(t1.decimals, 18))) /
                NULLIF(p.reserve0::numeric / POWER(10, COALESCE(t0.decimals, 18)), 0)
            ELSE NULL END
        ELSE t0.price_eth
    END                              AS price_eth,
    p.txn_count                      AS txn_count,
    p.created_at_block               AS created_at_block,
    p.created_at_timestamp           AS created_at_timestamp,
    to_timestamp(p.created_at_timestamp) AS created_at,
    CASE
      WHEN NULLIF(GREATEST(
        COALESCE(ls.ts, 0),
        COALESCE(lm.ts, 0),
        COALESCE(lb.ts, 0),
        COALESCE(ly.ts, 0)
      ), 0) IS NULL THEN NULL
      ELSE to_timestamp(GREATEST(
        COALESCE(ls.ts, 0),
        COALESCE(lm.ts, 0),
        COALESCE(lb.ts, 0),
        COALESCE(ly.ts, 0)
      ))
    END                               AS last_activity,
    COALESCE(v24.volume_usd_24h, 0)::numeric AS volume_usd_24h
FROM uniswap_v2_pairs p
CROSS JOIN zil_price zp
LEFT JOIN tokens t0 ON t0.address = p.token0
LEFT JOIN tokens t1 ON t1.address = p.token1
LEFT JOIN LATERAL (
  SELECT SUM(s.amount_usd) AS volume_usd_24h
  FROM uniswap_v2_swaps s
  WHERE s.pair = p.address
    AND s.amount_usd IS NOT NULL
    AND s.timestamp >= EXTRACT(EPOCH FROM NOW())::bigint - 86400
) v24 ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(s.timestamp) AS ts FROM uniswap_v2_swaps s WHERE s.pair = p.address
) ls ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(m.timestamp) AS ts FROM uniswap_v2_mints m WHERE m.pair = p.address
) lm ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(b.timestamp) AS ts FROM uniswap_v2_burns b WHERE b.pair = p.address
) lb ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(y.timestamp) AS ts FROM uniswap_v2_syncs y WHERE y.pair = p.address
) ly ON TRUE

UNION ALL

SELECT
    'uniswap_v3'::text               AS protocol,
    p.address                        AS address,
    p.token0                         AS token0,
    p.token1                         AS token1,
    t0.symbol                        AS token0_symbol,
    t0.name                          AS token0_name,
    t1.symbol                        AS token1_symbol,
    t1.name                          AS token1_name,
    p.fee                            AS fee,
    p.reserve0                       AS reserve0,
    p.reserve1                       AS reserve1,
    p.liquidity                      AS liquidity,
    CASE WHEN (t0.price_usd IS NOT NULL OR t1.price_usd IS NOT NULL) THEN
      ((COALESCE(p.reserve0,0) / POWER(10::numeric, COALESCE(t0.decimals, 18))) * COALESCE(t0.price_usd, 0)) +
      ((COALESCE(p.reserve1,0) / POWER(10::numeric, COALESCE(t1.decimals, 18))) * COALESCE(t1.price_usd, 0))
    ELSE NULL END                    AS liquidity_usd,
    p.volume_usd                     AS volume_usd,
    p.fees_usd                       AS fees_usd,
    -- V3 price from sqrt_price_x96
    -- rate = (sqrt^2 / 2^192) / 10^(dec1-dec0) = token0 price in token1 (human units)
    CASE
        -- token0 is WZIL, token1 is stablecoin → will swap to stablecoin/WZIL, show stablecoin's price
        -- rate = WZIL price in stablecoin, so stablecoin_usd = (1/rate) * zil_price
        WHEN LOWER(p.token0) = (SELECT addr FROM wzil) AND LOWER(p.token1) IN (SELECT addr FROM stablecoins) THEN
            CASE WHEN p.sqrt_price_x96 IS NOT NULL AND p.sqrt_price_x96 > 0 AND p.liquidity > 0 THEN
                (1.0 / NULLIF((POWER(p.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
                 POWER(10::numeric, COALESCE(t1.decimals, 18) - COALESCE(t0.decimals, 18)), 0)) * zp.usd
            ELSE NULL END
        -- token0 is WZIL (token1 is regular token) → will swap, compute token1's USD price
        WHEN LOWER(p.token0) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.sqrt_price_x96 IS NOT NULL AND p.sqrt_price_x96 > 0 AND p.liquidity > 0 THEN
                (1.0 / NULLIF((POWER(p.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
                 POWER(10::numeric, COALESCE(t1.decimals, 18) - COALESCE(t0.decimals, 18)), 0)) * zp.usd
            ELSE NULL END
        -- token0 is stablecoin, token1 is WZIL → will swap to WZIL/stablecoin, show WZIL's price
        WHEN LOWER(p.token0) IN (SELECT addr FROM stablecoins) AND LOWER(p.token1) = (SELECT addr FROM wzil) THEN
            zp.usd
        -- token0 is stablecoin (token1 is regular token) → will swap, compute token1's USD price
        WHEN LOWER(p.token0) IN (SELECT addr FROM stablecoins) THEN
            CASE WHEN p.sqrt_price_x96 IS NOT NULL AND p.sqrt_price_x96 > 0 AND p.liquidity > 0 THEN
                (1.0 / NULLIF((POWER(p.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
                 POWER(10::numeric, COALESCE(t1.decimals, 18) - COALESCE(t0.decimals, 18)), 0)) * COALESCE(t0.price_usd, 1)
            ELSE NULL END
        -- token1 is WZIL → token0 stays, compute token0's USD price = rate × ZIL price
        WHEN LOWER(p.token1) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.sqrt_price_x96 IS NOT NULL AND p.sqrt_price_x96 > 0 AND p.liquidity > 0 THEN
                ((POWER(p.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
                 POWER(10::numeric, COALESCE(t1.decimals, 18) - COALESCE(t0.decimals, 18))) * zp.usd
            ELSE NULL END
        -- token1 is stablecoin → token0 stays, compute token0's USD price
        WHEN LOWER(p.token1) IN (SELECT addr FROM stablecoins) THEN
            CASE WHEN p.sqrt_price_x96 IS NOT NULL AND p.sqrt_price_x96 > 0 AND p.liquidity > 0 THEN
                ((POWER(p.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
                 POWER(10::numeric, COALESCE(t1.decimals, 18) - COALESCE(t0.decimals, 18))) * COALESCE(t1.price_usd, 1)
            ELSE NULL END
        ELSE t0.price_usd
    END                              AS price_usd,
    -- price_eth for V3 = price in WZIL terms
    CASE
        WHEN LOWER(p.token0) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.sqrt_price_x96 IS NOT NULL AND p.sqrt_price_x96 > 0 AND p.liquidity > 0 THEN
                1.0 / NULLIF((POWER(p.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
                 POWER(10::numeric, COALESCE(t1.decimals, 18) - COALESCE(t0.decimals, 18)), 0)
            ELSE NULL END
        WHEN LOWER(p.token1) = (SELECT addr FROM wzil) THEN
            CASE WHEN p.sqrt_price_x96 IS NOT NULL AND p.sqrt_price_x96 > 0 AND p.liquidity > 0 THEN
                (POWER(p.sqrt_price_x96::numeric, 2) / POWER(2::numeric, 192)) /
                 POWER(10::numeric, COALESCE(t1.decimals, 18) - COALESCE(t0.decimals, 18))
            ELSE NULL END
        ELSE t0.price_eth
    END                              AS price_eth,
    p.txn_count                      AS txn_count,
    p.created_at_block               AS created_at_block,
    p.created_at_timestamp           AS created_at_timestamp,
    to_timestamp(p.created_at_timestamp) AS created_at,
    CASE
      WHEN NULLIF(GREATEST(
        COALESCE(ls.ts, 0),
        COALESCE(lm.ts, 0),
        COALESCE(lb.ts, 0),
        COALESCE(lc.ts, 0)
      ), 0) IS NULL THEN NULL
      ELSE to_timestamp(GREATEST(
        COALESCE(ls.ts, 0),
        COALESCE(lm.ts, 0),
        COALESCE(lb.ts, 0),
        COALESCE(lc.ts, 0)
      ))
    END                               AS last_activity,
    COALESCE(v24.volume_usd_24h, 0)::numeric AS volume_usd_24h
FROM uniswap_v3_pools p
CROSS JOIN zil_price zp
LEFT JOIN tokens t0 ON t0.address = p.token0
LEFT JOIN tokens t1 ON t1.address = p.token1
LEFT JOIN LATERAL (
  SELECT SUM(s.amount_usd) AS volume_usd_24h
  FROM uniswap_v3_swaps s
  WHERE s.pool = p.address
    AND s.amount_usd IS NOT NULL
    AND s.timestamp >= EXTRACT(EPOCH FROM NOW())::bigint - 86400
) v24 ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(s.timestamp) AS ts FROM uniswap_v3_swaps s WHERE s.pool = p.address
) ls ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(m.timestamp) AS ts FROM uniswap_v3_mints m WHERE m.pool = p.address
) lm ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(b.timestamp) AS ts FROM uniswap_v3_burns b WHERE b.pool = p.address
) lb ON TRUE
LEFT JOIN LATERAL (
  SELECT MAX(c.timestamp) AS ts FROM uniswap_v3_collects c WHERE c.pool = p.address
) lc ON TRUE;

COMMENT ON VIEW dex_pools IS 'Unified view of all DEX pools. price_usd is computed from pool rate × ZIL price (not aggregated token price).';
