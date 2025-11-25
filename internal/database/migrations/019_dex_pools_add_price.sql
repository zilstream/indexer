-- Migration 019: Add price_usd and price_eth to dex_pools view
-- Price represents token0 priced in USD/ETH terms, derived from token prices

CREATE OR REPLACE VIEW dex_pools AS
SELECT
    'uniswap_v2'::text               AS protocol,
    p.address                        AS address,
    p.token0                         AS token0,
    p.token1                         AS token1,
    t0.symbol                        AS token0_symbol,
    t0.name                          AS token0_name,
    t1.symbol                        AS token1_symbol,
    t1.name                          AS token1_name,
    NULL::numeric                    AS fee,              -- V2 has no fee tier per pool
    p.reserve0                       AS reserve0,         -- V2 reserves
    p.reserve1                       AS reserve1,
    NULL::numeric                    AS liquidity,        -- V2 has LP total_supply instead; leave null here
    p.reserve_usd                    AS liquidity_usd,    -- USD liquidity for V2
    p.volume_usd                     AS volume_usd,       -- lifetime volume (kept for backward-compat)
    NULL::numeric                    AS fees_usd,         -- V2 fees not tracked here
    t0.price_usd                     AS price_usd,        -- token0 price in USD
    t0.price_eth                     AS price_eth,        -- token0 price in ETH
    p.txn_count                      AS txn_count,
    p.created_at_block               AS created_at_block,
    p.created_at_timestamp           AS created_at_timestamp,
    to_timestamp(p.created_at_timestamp) AS created_at,   -- convenience datetime
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
    END                               AS last_activity,   -- datetime
    COALESCE(v24.volume_usd_24h, 0)::numeric AS volume_usd_24h -- rolling 24h USD volume
FROM uniswap_v2_pairs p
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
    p.fee                            AS fee,              -- V3 fee tier
    p.reserve0                       AS reserve0,
    p.reserve1                       AS reserve1,
    p.liquidity                      AS liquidity,        -- V3 in-range liquidity
    -- Approximate USD liquidity for V3 from current reserves and token prices
    CASE WHEN (t0.price_usd IS NOT NULL OR t1.price_usd IS NOT NULL) THEN
      ((COALESCE(p.reserve0,0) / POWER(10::numeric, COALESCE(t0.decimals, 18))) * COALESCE(t0.price_usd, 0)) +
      ((COALESCE(p.reserve1,0) / POWER(10::numeric, COALESCE(t1.decimals, 18))) * COALESCE(t1.price_usd, 0))
    ELSE NULL END                    AS liquidity_usd,
    p.volume_usd                     AS volume_usd,       -- lifetime volume
    p.fees_usd                       AS fees_usd,
    t0.price_usd                     AS price_usd,        -- token0 price in USD
    t0.price_eth                     AS price_eth,        -- token0 price in ETH
    p.txn_count                      AS txn_count,
    p.created_at_block               AS created_at_block,
    p.created_at_timestamp           AS created_at_timestamp,
    to_timestamp(p.created_at_timestamp) AS created_at,   -- convenience datetime
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
    END                               AS last_activity,   -- datetime
    COALESCE(v24.volume_usd_24h, 0)::numeric AS volume_usd_24h -- rolling 24h USD volume
FROM uniswap_v3_pools p
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

COMMENT ON VIEW dex_pools IS 'Unified view of all DEX pools across Uniswap V2 (pairs) and Uniswap V3 (pools), with token0 price in USD/ETH, 24h volume and last activity.';
