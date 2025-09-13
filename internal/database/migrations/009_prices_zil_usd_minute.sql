-- Migration 009: ZIL/USD minute price table

-- Stores minute-bucketed ZIL→USD prices with source attribution.
-- Timestamps are UTC minute precision.

CREATE TABLE IF NOT EXISTS prices_zil_usd_minute (
    ts          TIMESTAMPTZ   PRIMARY KEY,
    price       NUMERIC(18,9) NOT NULL,  -- ZIL → USD
    source      TEXT          NOT NULL,
    inserted_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
    UNIQUE (ts, source)
);

COMMENT ON TABLE prices_zil_usd_minute IS 'Minute-bucketed ZIL→USD prices with source attribution';
COMMENT ON COLUMN prices_zil_usd_minute.ts IS 'UTC timestamp truncated to minute';
COMMENT ON COLUMN prices_zil_usd_minute.price IS 'ZIL priced in USD (NUMERIC(18,9))';
COMMENT ON COLUMN prices_zil_usd_minute.source IS 'Data source identifier (bootstrap_csv|manual|provider)';
