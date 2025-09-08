#!/bin/bash

echo "Running Zilstream Indexer Tests"
echo "================================"
echo ""

# Run all tests
echo "Running all tests..."
go test ./... -v

echo ""
echo "Test Summary:"
echo "-------------"

# Show summary
go test ./... 2>&1 | grep -E "^(ok|FAIL)"

echo ""
echo "Specific test suites:"
echo "--------------------"

echo ""
echo "1. Database tests:"
go test ./internal/database -v -count=1

echo ""
echo "2. Module tests (including Zilliqa transaction validation):"
go test ./internal/modules -v -count=1

echo ""
echo "3. Processor tests (block processing):"
go test ./internal/processor -run TestBlockProcessor -v -count=1

echo ""
echo "4. Uniswap V2 tests (PairCreated, token extraction):"
go test ./internal/processor -run TestUniswapV2Events -v -count=1