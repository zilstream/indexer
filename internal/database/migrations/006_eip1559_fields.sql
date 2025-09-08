-- Add EIP-1559 and additional receipt fields to transactions table

ALTER TABLE transactions
ADD COLUMN IF NOT EXISTS max_fee_per_gas NUMERIC(78, 0),
ADD COLUMN IF NOT EXISTS max_priority_fee_per_gas NUMERIC(78, 0),
ADD COLUMN IF NOT EXISTS contract_address VARCHAR(42),
ADD COLUMN IF NOT EXISTS cumulative_gas_used BIGINT,
ADD COLUMN IF NOT EXISTS effective_gas_price NUMERIC(78, 0);

-- Add index for contract addresses
CREATE INDEX IF NOT EXISTS idx_tx_contract_address ON transactions(contract_address) WHERE contract_address IS NOT NULL;