package prices

import (
	"context"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Provider exposes point-in-time ZIL→USD lookups.
type Provider interface {
	// PriceZILUSD returns the ZIL→USD price for the minute at or prior to ts.
	// ok=false when no price can be found within the allowed lookback policy (if enforced by impl).
	PriceZILUSD(ctx context.Context, ts time.Time) (price string, ok bool)
}

// PostgresProvider implements Provider against the prices_zil_usd_minute table with a small in-memory cache.
type PostgresProvider struct {
	pool          *pgxpool.Pool
	maxCacheItems int

	mu    sync.RWMutex
	cache map[int64]string // key: minute Unix ts (UTC), value: decimal string price
	fifo  []int64          // simple FIFO eviction order
}

// NewPostgresProvider creates a new provider.
func NewPostgresProvider(pool *pgxpool.Pool, maxCacheItems int) *PostgresProvider {
	if maxCacheItems <= 0 {
		maxCacheItems = 10_000 // ~1 week if fully dense
	}
	return &PostgresProvider{
		pool:          pool,
		maxCacheItems: maxCacheItems,
		cache:         make(map[int64]string, maxCacheItems),
		fifo:          make([]int64, 0, maxCacheItems),
	}
}

// normalize to UTC minute bucket
func minuteBucket(ts time.Time) time.Time {
	return ts.UTC().Truncate(time.Minute)
}

func (p *PostgresProvider) PriceZILUSD(ctx context.Context, ts time.Time) (string, bool) {
	bucket := minuteBucket(ts)
	key := bucket.Unix()

	// fast path: cache
	p.mu.RLock()
	if price, ok := p.cache[key]; ok {
		p.mu.RUnlock()
		return price, true
	}
	p.mu.RUnlock()

	// DB lookup: nearest prior minute
	var price string
	row := p.pool.QueryRow(ctx, `
		SELECT price::text
		FROM prices_zil_usd_minute
		WHERE ts <= $1
		ORDER BY ts DESC
		LIMIT 1
	`, bucket)
	if err := row.Scan(&price); err != nil {
		return "", false
	}

	// populate cache (best-effort)
	p.mu.Lock()
	if _, exists := p.cache[key]; !exists {
		p.cache[key] = price
		p.fifo = append(p.fifo, key)
		if len(p.fifo) > p.maxCacheItems {
			old := p.fifo[0]
			p.fifo = p.fifo[1:]
			delete(p.cache, old)
		}
	}
	p.mu.Unlock()

	return price, true
}
