.PHONY: build run clean test migrate-up migrate-down docker-build help

# Variables
BINARY_NAME=indexer
DOCKER_IMAGE=zilstream-indexer
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"
CONFIG?=config.yaml
CSV?=data/zilliqa_historical_prices.csv
SOURCE?=bootstrap_csv

# Default target
all: build

## help: Show this help message
help:
	@echo 'Usage:'
	@sed -n 's/^##//p' ${MAKEFILE_LIST} | column -t -s ':' | sed -e 's/^/ /'

## build: Build the binary
build:
	@echo "Building $(BINARY_NAME)..."
	@go build $(LDFLAGS) -o bin/$(BINARY_NAME) cmd/indexer/main.go

## run: Run the indexer
run:
	@echo "Running indexer..."
	@go run cmd/indexer/main.go --config=$(CONFIG)

## load-zil-prices: Load historical ZIL/USD prices from CSV into prices_zil_usd_minute
load-zil-prices:
	@echo "Loading ZIL/USD prices from $(CSV) with source=$(SOURCE)..."
	@go run cmd/load_zil_prices_csv/main.go --config=$(CONFIG) --csv=$(CSV) --source=$(SOURCE)

## build-load-zil-prices: Build the CSV loader binary
build-load-zil-prices:
	@echo "Building CSV loader..."
	@go build $(LDFLAGS) -o bin/load_zil_prices_csv cmd/load_zil_prices_csv/main.go

## clean: Clean build artifacts and test cache
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@go clean -testcache
	@rm -f coverage.out coverage.html

## test: Run all tests
test:
	@echo "Running tests..."
	@go test ./... -short

## test-verbose: Run all tests with verbose output
test-verbose:
	@echo "Running tests (verbose)..."
	@go test ./... -v

## test-database: Test database package
test-database:
	@echo "Testing database package..."
	@go test ./internal/database -v -count=1

## test-modules: Test modules (including Zilliqa transaction handling)
test-modules:
	@echo "Testing modules package..."
	@go test ./internal/modules -v -count=1

## test-processor: Test processor (block processing)
test-processor:
	@echo "Testing processor package..."
	@go test ./internal/processor -run TestBlockProcessor -v -count=1

## test-uniswap: Test Uniswap V2 events (PairCreated, token extraction)
test-uniswap:
	@echo "Testing Uniswap V2 events..."
	@go test ./internal/processor -run TestUniswapV2Events -v -count=1

## test-coverage: Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test ./... -coverprofile=coverage.out
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

## test-summary: Quick test summary
test-summary:
	@echo "Test Summary:"
	@echo "============="
	@go test ./... 2>&1 | grep -E "^(ok|FAIL|\?)" | column -t

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

## migrate-up: Run database migrations up
migrate-up:
	@echo "Running migrations..."
	@for file in internal/database/migrations/*.sql; do \
		echo "Applying migration: $$file"; \
		psql -h localhost -U melvin -d zilstream -f $$file || exit 1; \
	done
	@echo "All migrations applied successfully"

## migrate-down: Run database migrations down
migrate-down:
	@echo "Rolling back migrations..."
	@psql -h localhost -U melvin -d zilstream -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"
	@echo "All tables dropped"

## db-create: Create database
db-create:
	@echo "Creating database..."
	@psql -h localhost -U melvin -d postgres -c "CREATE DATABASE zilstream;" || true

## db-drop: Drop database
db-drop:
	@echo "Dropping database..."
	@psql -h localhost -U melvin -d postgres -c "DROP DATABASE IF EXISTS zilstream;"

## db-reset: Reset database (drop and recreate)
db-reset: db-drop db-create migrate-up
	@echo "Database reset complete"

## docker-build: Build Docker image
docker-build:
	@echo "Building Docker image..."
	@docker build -t $(DOCKER_IMAGE):$(VERSION) .
	@docker tag $(DOCKER_IMAGE):$(VERSION) $(DOCKER_IMAGE):latest

## docker-run: Run with Docker Compose
docker-run:
	@echo "Starting with Docker Compose..."
	@docker-compose up -d

## docker-stop: Stop Docker Compose
docker-stop:
	@echo "Stopping Docker Compose..."
	@docker-compose down

## lint: Run linter
lint:
	@echo "Running linter..."
	@golangci-lint run ./... || true

## fmt: Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@gofmt -s -w .

# Development helpers
## dev: Run with hot reload (requires air)
dev:
	@which air > /dev/null || go install github.com/cosmtrek/air@latest
	@air -c .air.toml