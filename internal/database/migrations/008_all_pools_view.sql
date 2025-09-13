-- Migration 008: Combined DEX pools view (Uniswap V2 + V3)

-- This view provides a unified list of all pools across Uniswap V2 pairs and Uniswap V3 pools.
-- Columns are normalized where possible. Metrics that are protocol-specific are nullable on the other protocol.

CREATE OR REPLACE VIEW dex_pools AS
SELECT
    'uniswap_v2'::text               AS protocol,
    p.address                        AS address,
    p.token0                         AS token0,
    p.token1                         AS token1,
    NULL::numeric                    AS fee,              -- V2 has no fee tier per pool
    p.reserve0                       AS reserve0,         -- V2 reserves
    p.reserve1                       AS reserve1,
    NULL::numeric                    AS liquidity,        -- V2 has LP total_supply instead; leave null here
    p.reserve_usd                    AS liquidity_usd,    -- USD liquidity for V2
    p.volume_usd                     AS volume_usd,
    NULL::numeric                    AS fees_usd,         -- V2 fees not tracked here
    p.txn_count                      AS txn_count,
    p.created_at_block               AS created_at_block,
    p.created_at_timestamp           AS created_at_timestamp
FROM uniswap_v2_pairs p
UNION ALL
SELECT
    'uniswap_v3'::text               AS protocol,
    p.address                        AS address,
    p.token0                         AS token0,
    p.token1                         AS token1,
    p.fee                            AS fee,              -- V3 fee tier
    NULL::numeric                    AS reserve0,         -- V3 does not expose raw reserves in this table
    NULL::numeric                    AS reserve1,
    p.liquidity                      AS liquidity,        -- V3 in-range liquidity
    NULL::numeric                    AS liquidity_usd,    -- Not tracked at pool level for V3 (can be derived externally)
    p.volume_usd                     AS volume_usd,
    p.fees_usd                       AS fees_usd,
    p.txn_count                      AS txn_count,
    p.created_at_block               AS created_at_block,
    p.created_at_timestamp           AS created_at_timestamp
FROM uniswap_v3_pools p;

COMMENT ON VIEW dex_pools IS 'Unified view of all DEX pools across Uniswap V2 (pairs) and Uniswap V3 (pools).';
