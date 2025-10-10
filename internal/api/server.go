package api

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
)

type APIServer struct {
	mux    *http.ServeMux
	db     *pgxpool.Pool
	logger zerolog.Logger
}

func NewAPIServer(db *pgxpool.Pool, logger zerolog.Logger) *APIServer {
	s := &APIServer{
		mux:    http.NewServeMux(),
		db:     db,
		logger: logger.With().Str("component", "api").Logger(),
	}
	s.registerRoutes()
	return s
}

func (s *APIServer) Start(ctx context.Context, addr string) error {
	s.logger.Info().Str("addr", addr).Msg("Starting API server")
	server := &http.Server{
		Addr:    addr,
		Handler: s.logMiddleware(s.mux),
	}
	// Shutdown goroutine
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.logger.Info().Msg("Shutting down API server...")
		_ = server.Shutdown(shutdownCtx)
	}()
	// Start serving (returns http.ErrServerClosed on shutdown)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *APIServer) registerRoutes() {
	// Health & status (lightweight inline versions)
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		JSON(w, http.StatusOK, map[string]any{
			"status": "healthy",
			"timestamp": time.Now().UTC(),
		}, nil)
	})
	// Simple status using last indexed block (if available)
	s.mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		lastBlock, err := s.getLastBlock(ctx)
		if err != nil {
			Error(w, http.StatusInternalServerError, err.Error())
			return
		}
		JSON(w, http.StatusOK, map[string]any{
			"last_block": lastBlock,
			"time": time.Now().UTC(),
		}, nil)
	})

	// Collections
	s.mux.HandleFunc("/tokens", s.handleTokens)
	s.mux.HandleFunc("/pairs", s.handlePairs)
	s.mux.HandleFunc("/stats", s.handleStats)
	s.mux.HandleFunc("/blocks", s.handleBlocks)
	s.mux.HandleFunc("/transactions", s.handleTransactions)

	// Pair-scoped prefix for events
	s.mux.HandleFunc("/pairs/", s.handlePairPrefix)
	
	// Block and transaction detail endpoints
	s.mux.HandleFunc("/blocks/", s.handleBlockDetail)
	s.mux.HandleFunc("/transactions/", s.handleTransactionDetail)
}

func (s *APIServer) logMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		s.logger.Info().
			Str("method", r.Method).
			Str("path", r.URL.Path).
			Dur("latency", time.Since(start)).
			Msg("http")
	})
}

func (s *APIServer) getLastBlock(ctx context.Context) (any, error) {
	// Try quick query; fall back to null on error
	q := `SELECT number, timestamp FROM blocks ORDER BY number DESC LIMIT 1`
	var number *int64
	var ts *int64
	row := s.db.QueryRow(ctx, q)
	if err := row.Scan(&number, &ts); err != nil {
		return nil, nil
	}
	return map[string]any{"number": number, "timestamp": ts}, nil
}

func (s *APIServer) handleTokens(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset, page, perPage := parsePagination(r)
	var search *string
	if v := r.URL.Query().Get("search"); v != "" { search = &v }
	items, err := database.ListTokens(ctx, s.db, limit, offset, search)
	if err != nil { Error(w, http.StatusInternalServerError, err.Error()); return }
	pg := &Pagination{Page: page, PerPage: perPage, HasNext: len(items) == perPage}
	JSON(w, http.StatusOK, items, pg)
}

func (s *APIServer) handlePairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset, page, perPage := parsePagination(r)
	sortBy := r.URL.Query().Get("sort_by")
	if sortBy == "" {
		sortBy = "volume_24h" // default
	}
	sortOrder := r.URL.Query().Get("sort_order")
	if sortOrder == "" {
		sortOrder = "desc" // default
	}
	items, err := database.ListPairs(ctx, s.db, limit, offset, sortBy, sortOrder)
	if err != nil { Error(w, http.StatusInternalServerError, err.Error()); return }
	pg := &Pagination{Page: page, PerPage: perPage, HasNext: len(items) == perPage}
	JSON(w, http.StatusOK, items, pg)
}

func (s *APIServer) handlePairPrefix(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/pairs/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		Error(w, http.StatusNotFound, "not found")
		return
	}
	address, sub := parts[0], parts[1]
	switch sub {
	case "events":
		s.handlePairEvents(w, r, address)
	default:
		Error(w, http.StatusNotFound, "not found")
	}
}

func (s *APIServer) handlePairEvents(w http.ResponseWriter, r *http.Request, address string) {
	ctx := r.Context()
	limit, offset, page, perPage := parsePagination(r)
	var eventType *string
	if v := r.URL.Query().Get("type"); v != "" { eventType = &v }
	var protocol *string
	if v := r.URL.Query().Get("protocol"); v != "" { protocol = &v }
	items, err := database.ListPairEvents(ctx, s.db, address, eventType, protocol, limit, offset)
	if err != nil { Error(w, http.StatusInternalServerError, err.Error()); return }
	pg := &Pagination{Page: page, PerPage: perPage, HasNext: len(items) == perPage}
	JSON(w, http.StatusOK, items, pg)
}

func (s *APIServer) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	stats, err := database.GetStats(ctx, s.db)
	if err != nil { Error(w, http.StatusInternalServerError, err.Error()); return }
	JSON(w, http.StatusOK, stats, nil)
}

func (s *APIServer) handleBlocks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset, page, perPage := parsePagination(r)
	items, err := database.ListBlocks(ctx, s.db, limit, offset)
	if err != nil { Error(w, http.StatusInternalServerError, err.Error()); return }
	pg := &Pagination{Page: page, PerPage: perPage, HasNext: len(items) == perPage}
	JSON(w, http.StatusOK, items, pg)
}

func (s *APIServer) handleBlockDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	numberStr := strings.TrimPrefix(r.URL.Path, "/blocks/")
	number, err := strconv.ParseInt(numberStr, 10, 64)
	if err != nil {
		Error(w, http.StatusBadRequest, "invalid block number")
		return
	}
	block, err := database.GetBlock(ctx, s.db, number)
	if err != nil {
		Error(w, http.StatusNotFound, "block not found")
		return
	}
	JSON(w, http.StatusOK, block, nil)
}

func (s *APIServer) handleTransactions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	limit, offset, page, perPage := parsePagination(r)
	items, err := database.ListTransactions(ctx, s.db, limit, offset)
	if err != nil { Error(w, http.StatusInternalServerError, err.Error()); return }
	pg := &Pagination{Page: page, PerPage: perPage, HasNext: len(items) == perPage}
	JSON(w, http.StatusOK, items, pg)
}

func (s *APIServer) handleTransactionDetail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	hash := strings.TrimPrefix(r.URL.Path, "/transactions/")
	tx, err := database.GetTransaction(ctx, s.db, hash)
	if err != nil {
		Error(w, http.StatusNotFound, "transaction not found")
		return
	}
	JSON(w, http.StatusOK, tx, nil)
}
