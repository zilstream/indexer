-- 012_optional_text_search.sql
-- Optional indexes requiring pg_trgm extension for fuzzy text search
-- This migration is safe to fail if the extension is not available

-- Enable pg_trgm extension if available (will error if not available, but migration will continue)
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Fuzzy search indexes for token symbol and name (only if pg_trgm is available)
DO $$
BEGIN
    -- Check if pg_trgm extension is available
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'pg_trgm') THEN
        -- Create trigram indexes for fuzzy search (cannot use CONCURRENTLY in functions)
        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_tokens_symbol_trgm
                 ON tokens USING gin(symbol gin_trgm_ops)';

        EXECUTE 'CREATE INDEX IF NOT EXISTS idx_tokens_name_trgm
                 ON tokens USING gin(name gin_trgm_ops)';

        RAISE NOTICE 'Trigram indexes created successfully';
    ELSE
        RAISE NOTICE 'pg_trgm extension not available, skipping trigram indexes';
    END IF;
END $$;