-- Migration 002: Add support for Zilliqa-specific transaction types

-- Add transaction_type column to track the original transaction type
ALTER TABLE transactions 
ADD COLUMN IF NOT EXISTS transaction_type INTEGER DEFAULT 0,
ADD COLUMN IF NOT EXISTS original_type_hex VARCHAR(20);

-- Add index for transaction type queries
CREATE INDEX IF NOT EXISTS idx_tx_type ON transactions(transaction_type);

-- Add comment to explain transaction types
COMMENT ON COLUMN transactions.transaction_type IS 'Transaction type: 0=Legacy, 1=EIP-2930, 2=EIP-1559, 3=EIP-4844, >1000=Zilliqa-specific';
COMMENT ON COLUMN transactions.original_type_hex IS 'Original hex type value from Zilliqa RPC (e.g., 0xdd870)';