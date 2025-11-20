-- Migration 018: Add maker column to dex_pair_events view
-- Join with transactions table to get the transaction sender (maker)

DROP VIEW IF EXISTS dex_pair_events;

CREATE OR REPLACE VIEW dex_pair_events AS
SELECT
    e.*,
    t.from_address AS maker
FROM (
    -- V2 swaps
    SELECT
      'uniswap_v2'::text        AS protocol,
      'swap'::text              AS event_type,
      s.id,
      s.transaction_hash,
      s.log_index,
      s.block_number,
      s.timestamp,
      s.pair                    AS address,
      s.sender,
      s.recipient,
      NULL::varchar(42)         AS to_address,
      s.amount0_in,
      s.amount1_in,
      s.amount0_out,
      s.amount1_out,
      NULL::numeric             AS liquidity,
      s.amount_usd
    FROM uniswap_v2_swaps s

    UNION ALL
    -- V2 mints
    SELECT
      'uniswap_v2',
      'mint',
      m.id,
      m.transaction_hash,
      m.log_index,
      m.block_number,
      m.timestamp,
      m.pair                   AS address,
      m.sender,
      NULL::varchar(42)        AS recipient,
      m.to_address,
      m.amount0                AS amount0_in,
      m.amount1                AS amount1_in,
      0::numeric               AS amount0_out,
      0::numeric               AS amount1_out,
      m.liquidity              AS liquidity,
      m.amount_usd
    FROM uniswap_v2_mints m

    UNION ALL
    -- V2 burns
    SELECT
      'uniswap_v2',
      'burn',
      b.id,
      b.transaction_hash,
      b.log_index,
      b.block_number,
      b.timestamp,
      b.pair                   AS address,
      b.sender,
      NULL::varchar(42)        AS recipient,
      b.to_address,
      0::numeric               AS amount0_in,
      0::numeric               AS amount1_in,
      b.amount0                AS amount0_out,
      b.amount1                AS amount1_out,
      b.liquidity              AS liquidity,
      b.amount_usd
    FROM uniswap_v2_burns b

    UNION ALL
    -- V3 swaps
    SELECT
      'uniswap_v3',
      'swap',
      s.id,
      s.transaction_hash,
      s.log_index,
      s.block_number,
      s.timestamp,
      s.pool                  AS address,
      s.sender,
      s.recipient,
      NULL::varchar(42)       AS to_address,
      CASE WHEN s.amount0 > 0 THEN s.amount0 ELSE 0 END AS amount0_in,
      CASE WHEN s.amount1 > 0 THEN s.amount1 ELSE 0 END AS amount1_in,
      CASE WHEN s.amount0 < 0 THEN ABS(s.amount0) ELSE 0 END AS amount0_out,
      CASE WHEN s.amount1 < 0 THEN ABS(s.amount1) ELSE 0 END AS amount1_out,
      NULL::numeric           AS liquidity,
      s.amount_usd
    FROM uniswap_v3_swaps s

    UNION ALL
    -- V3 mints
    SELECT
      'uniswap_v3',
      'mint',
      m.id,
      m.transaction_hash,
      m.log_index,
      m.block_number,
      m.timestamp,
      m.pool                 AS address,
      m.owner                AS sender,
      NULL::varchar(42)      AS recipient,
      m.owner                AS to_address,
      CASE WHEN m.amount0 > 0 THEN m.amount0 ELSE 0 END AS amount0_in,
      CASE WHEN m.amount1 > 0 THEN m.amount1 ELSE 0 END AS amount1_in,
      0::numeric             AS amount0_out,
      0::numeric             AS amount1_out,
      m.amount               AS liquidity,
      m.amount_usd
    FROM uniswap_v3_mints m

    UNION ALL
    -- V3 burns
    SELECT
      'uniswap_v3',
      'burn',
      b.id,
      b.transaction_hash,
      b.log_index,
      b.block_number,
      b.timestamp,
      b.pool                 AS address,
      b.owner                AS sender,
      NULL::varchar(42)      AS recipient,
      b.owner                AS to_address,
      0::numeric             AS amount0_in,
      0::numeric             AS amount1_in,
      CASE WHEN b.amount0 > 0 THEN b.amount0 ELSE 0 END AS amount0_out,
      CASE WHEN b.amount1 > 0 THEN b.amount1 ELSE 0 END AS amount1_out,
      b.amount               AS liquidity,
      b.amount_usd
    FROM uniswap_v3_burns b
) e
LEFT JOIN transactions t ON e.transaction_hash = t.hash;

COMMENT ON VIEW dex_pair_events IS 'Unified stream of swaps, mints, and burns across Uniswap V2 pairs and Uniswap V3 pools with normalized columns and maker address.';
