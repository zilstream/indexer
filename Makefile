.PHONY: build run clean test migrate-up migrate-down docker-build help

# Variables
BINARY_NAME=indexer
DOCKER_IMAGE=zilstream-indexer
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS=-ldflags "-X main.Version=$(VERSION)"

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
	@go run cmd/indexer/main.go --config=config.yaml

## clean: Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/
	@go clean

## test: Run tests
test:
	@echo "Running tests..."
	@go test -v -race -cover ./...

## deps: Download dependencies
deps:
	@echo "Downloading dependencies..."
	@go mod download
	@go mod tidy

## migrate-up: Run database migrations up
migrate-up:
	@echo "Running migrations..."
	@psql -h localhost -U postgres -d zilstream -f internal/database/migrations/001_initial.sql

## migrate-down: Run database migrations down
migrate-down:
	@echo "Rolling back migrations..."
	@psql -h localhost -U postgres -d zilstream -c "DROP TABLE IF EXISTS transactions, blocks, indexer_state CASCADE;"

## db-create: Create database
db-create:
	@echo "Creating database..."
	@psql -h localhost -U postgres -c "CREATE DATABASE zilstream;" || true

## db-drop: Drop database
db-drop:
	@echo "Dropping database..."
	@psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS zilstream;"

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