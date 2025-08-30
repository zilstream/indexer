package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
	"github.com/zilstream/indexer/internal/rpc"
	"github.com/zilstream/indexer/internal/sync"
)

type HealthServer struct {
	db         *database.DB
	rpc        *rpc.Client
	syncMgr    *sync.Manager
	logger     zerolog.Logger
	port       string
}

type HealthStatus struct {
	Status      string                 `json:"status"`
	Timestamp   time.Time              `json:"timestamp"`
	Database    DatabaseStatus         `json:"database"`
	RPC         RPCStatus              `json:"rpc"`
	Sync        map[string]interface{} `json:"sync"`
}

type DatabaseStatus struct {
	Connected bool   `json:"connected"`
	Error     string `json:"error,omitempty"`
}

type RPCStatus struct {
	Connected   bool   `json:"connected"`
	Endpoint    string `json:"endpoint"`
	ChainID     int64  `json:"chain_id"`
	LatestBlock uint64 `json:"latest_block"`
	Error       string `json:"error,omitempty"`
}

func NewHealthServer(
	db *database.DB,
	rpc *rpc.Client,
	syncMgr *sync.Manager,
	logger zerolog.Logger,
	port string,
) *HealthServer {
	return &HealthServer{
		db:      db,
		rpc:     rpc,
		syncMgr: syncMgr,
		logger:  logger,
		port:    port,
	}
}

func (h *HealthServer) Start() error {
	mux := http.NewServeMux()
	
	// Health endpoint
	mux.HandleFunc("/health", h.handleHealth)
	
	// Metrics endpoint
	mux.HandleFunc("/metrics", h.handleMetrics)
	
	// Ready endpoint (for k8s readiness probe)
	mux.HandleFunc("/ready", h.handleReady)
	
	// Live endpoint (for k8s liveness probe)
	mux.HandleFunc("/live", h.handleLive)
	
	server := &http.Server{
		Addr:         ":" + h.port,
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
	}
	
	h.logger.Info().Str("port", h.port).Msg("Starting health server")
	
	return server.ListenAndServe()
}

func (h *HealthServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	
	status := h.getHealthStatus(ctx)
	
	// Determine HTTP status code
	httpStatus := http.StatusOK
	if status.Status != "healthy" {
		httpStatus = http.StatusServiceUnavailable
	}
	
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(status)
}

func (h *HealthServer) getHealthStatus(ctx context.Context) HealthStatus {
	status := HealthStatus{
		Timestamp: time.Now(),
		Status:    "healthy",
	}
	
	// Check database
	status.Database = h.checkDatabase(ctx)
	if !status.Database.Connected {
		status.Status = "unhealthy"
	}
	
	// Check RPC
	status.RPC = h.checkRPC(ctx)
	if !status.RPC.Connected {
		status.Status = "unhealthy"
	}
	
	// Get sync status
	if h.syncMgr != nil {
		status.Sync = h.syncMgr.GetStatus()
		
		// Check if we're too far behind
		if behindBy, ok := status.Sync["behind_by"].(int64); ok && behindBy > 100 {
			status.Status = "degraded"
		}
	}
	
	return status
}

func (h *HealthServer) checkDatabase(ctx context.Context) DatabaseStatus {
	status := DatabaseStatus{Connected: true}
	
	// Simple ping check
	err := h.db.Pool().Ping(ctx)
	if err != nil {
		status.Connected = false
		status.Error = err.Error()
	}
	
	return status
}

func (h *HealthServer) checkRPC(ctx context.Context) RPCStatus {
	status := RPCStatus{
		Connected: true,
		Endpoint:  "https://api.zilliqa.com",
		ChainID:   32769,
	}
	
	// Check if connected
	if !h.rpc.IsConnected(ctx) {
		status.Connected = false
		status.Error = "RPC not connected"
		return status
	}
	
	// Get latest block
	latestBlock, err := h.rpc.GetLatestBlockNumber(ctx)
	if err != nil {
		status.Connected = false
		status.Error = err.Error()
	} else {
		status.LatestBlock = latestBlock
	}
	
	return status
}

func (h *HealthServer) handleMetrics(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	
	metrics := h.collectMetrics(ctx)
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func (h *HealthServer) collectMetrics(ctx context.Context) map[string]interface{} {
	metrics := make(map[string]interface{})
	
	// Database metrics
	if h.db != nil {
		stats := h.db.Pool().Stat()
		metrics["database"] = map[string]interface{}{
			"total_conns":    stats.TotalConns(),
			"idle_conns":     stats.IdleConns(),
			"acquired_conns": stats.AcquiredConns(),
		}
	}
	
	// Sync metrics
	if h.syncMgr != nil {
		metrics["sync"] = h.syncMgr.GetStatus()
	}
	
	// RPC metrics
	rpcStatus := h.checkRPC(ctx)
	metrics["rpc"] = map[string]interface{}{
		"connected":    rpcStatus.Connected,
		"latest_block": rpcStatus.LatestBlock,
	}
	
	return metrics
}

func (h *HealthServer) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	
	// Check if we're ready to serve traffic
	dbOk := h.db != nil && h.db.Pool().Ping(ctx) == nil
	rpcOk := h.rpc != nil && h.rpc.IsConnected(ctx)
	
	if dbOk && rpcOk {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ready"))
	} else {
		w.WriteHeader(http.StatusServiceUnavailable)
		w.Write([]byte("not ready"))
	}
}

func (h *HealthServer) handleLive(w http.ResponseWriter, r *http.Request) {
	// Simple liveness check - if we can respond, we're alive
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("alive"))
}