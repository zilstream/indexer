package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

// StatusProvider is an interface for getting status information
type StatusProvider interface {
	GetStatus(ctx context.Context) (map[string]interface{}, error)
}

// HealthServer provides health and status endpoints
type HealthServer struct {
	provider StatusProvider
	logger   zerolog.Logger
}

// NewHealthServer creates a new health server
func NewHealthServer(provider StatusProvider, logger zerolog.Logger) *HealthServer {
	return &HealthServer{
		provider: provider,
		logger:   logger.With().Str("component", "health").Logger(),
	}
}

// Start starts the health server
func (h *HealthServer) Start(port string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/status", h.handleStatus)
	
	h.logger.Info().Str("port", port).Msg("Starting health server")
	return http.ListenAndServe(port, mux)
}

// handleHealth handles the health check endpoint
func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleStatus handles the status endpoint
func (h *HealthServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	
	status, err := h.provider.GetStatus(ctx)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to get status")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{
			"error": err.Error(),
		})
		return
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}