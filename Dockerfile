FROM golang:1.25-alpine AS builder

# Install git for private dependencies if needed
RUN apk add --no-cache git ca-certificates tzdata

WORKDIR /app

# Copy go mod and sum files first for better caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the indexer binary explicitly from cmd/indexer
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o indexer ./cmd/indexer

FROM alpine:latest

# Install ca-certificates for HTTPS requests and tzdata for timezone support
RUN apk --no-cache add ca-certificates tzdata

WORKDIR /app

# Create non-root user
RUN adduser -D -s /bin/sh appuser

# Copy runtime assets
COPY --from=builder /app/indexer ./indexer
COPY --from=builder /app/config.yaml ./config.yaml
COPY --from=builder /app/manifests ./manifests
COPY --from=builder /app/data ./data

# Change ownership to non-root user
RUN chown appuser:appuser /app/indexer

# Switch to non-root user
USER appuser

CMD ["./indexer"]
