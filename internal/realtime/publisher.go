package realtime

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/centrifugal/gocent/v3"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
	"github.com/zilstream/indexer/internal/database"
)

type Publisher struct {
	gc          *gocent.Client
	db          *pgxpool.Pool
	logger      zerolog.Logger
	mu          sync.Mutex
	pending     map[string]struct{}
	flushCh     chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	currentBlock uint64
}

type PublishConfig struct {
	APIURL string
	APIKey string
}

func NewPublisher(config PublishConfig, db *pgxpool.Pool, logger zerolog.Logger) *Publisher {
	ctx, cancel := context.WithCancel(context.Background())
	
	p := &Publisher{
		gc: gocent.New(gocent.Config{
			Addr: config.APIURL,
			Key:  config.APIKey,
		}),
		db:      db,
		logger:  logger.With().Str("component", "realtime-publisher").Logger(),
		pending: make(map[string]struct{}),
		flushCh: make(chan struct{}, 1),
		ctx:     ctx,
		cancel:  cancel,
	}
	
	p.startFlusher()
	return p
}

func (p *Publisher) startFlusher() {
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		
		for {
			select {
			case <-p.ctx.Done():
				p.logger.Info().Msg("Stopping publisher flusher")
				return
			case <-ticker.C:
				p.flush(p.ctx)
			case <-p.flushCh:
				p.flush(p.ctx)
			}
		}
	}()
}

func (p *Publisher) EnqueuePairChanged(address string) {
	addr := strings.ToLower(address)
	p.mu.Lock()
	p.pending[addr] = struct{}{}
	p.mu.Unlock()
	
	select {
	case p.flushCh <- struct{}{}:
	default:
	}
}

func (p *Publisher) PublishEvent(address string, eventType string, data interface{}) {
	payload := map[string]any{
		"type":       "pair.event",
		"event_type": eventType,
		"data":       data,
	}

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		p.logger.Warn().Err(err).Msg("Failed to marshal event payload")
		return
	}

	channel := fmt.Sprintf("dex.pair.%s", strings.ToLower(address))

	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		if _, err := p.gc.Publish(p.ctx, channel, payloadBytes); err != nil {
			// Ignore errors if context is cancelled (shutting down)
			if p.ctx.Err() != nil {
				return
			}
			p.logger.Warn().
				Err(err).
				Str("pair", address).
				Str("channel", channel).
				Msg("Failed to publish pair event")
		}
	}()
}

func (p *Publisher) SetCurrentBlock(blockNumber uint64) {
	p.mu.Lock()
	p.currentBlock = blockNumber
	p.mu.Unlock()
}

func (p *Publisher) Flush() {
	p.flush(p.ctx)
}

func (p *Publisher) flush(ctx context.Context) {
	p.mu.Lock()
	if len(p.pending) == 0 {
		p.mu.Unlock()
		return
	}
	
	addrs := make([]string, 0, len(p.pending))
	for addr := range p.pending {
		addrs = append(addrs, addr)
	}
	currentBlock := p.currentBlock
	p.pending = make(map[string]struct{})
	p.mu.Unlock()
	
	p.logger.Debug().
		Int("count", len(addrs)).
		Uint64("block", currentBlock).
		Msg("Flushing pair updates")
	
	pairs, err := database.GetPairsByAddresses(ctx, p.db, addrs)
	if err != nil {
		p.logger.Error().Err(err).Msg("Failed to fetch pair summaries")
		return
	}
	
	if len(pairs) == 0 {
		return
	}
	
	now := time.Now().UTC()
	timestamp := now.Unix()
	
	for _, pair := range pairs {
		payload := map[string]any{
			"type":         "pair.update",
			"block_number": currentBlock,
			"ts":           timestamp,
			"pair":         pair,
		}
		
		payloadBytes, err := json.Marshal(payload)
		if err != nil {
			p.logger.Warn().Err(err).Msg("Failed to marshal pair payload")
			continue
		}
		
		channel := fmt.Sprintf("dex.pair.%s", strings.ToLower(pair.Address))
		if _, err := p.gc.Publish(ctx, channel, payloadBytes); err != nil {
			p.logger.Warn().
				Err(err).
				Str("pair", pair.Address).
				Str("channel", channel).
				Msg("Failed to publish pair update")
		}
	}
	
	items := make([]any, 0, len(pairs))
	for _, pair := range pairs {
		items = append(items, pair)
	}
	
	batchPayload := map[string]any{
		"type":         "pair.batch",
		"block_number": currentBlock,
		"ts":           timestamp,
		"items":        items,
	}
	
	batchPayloadBytes, err := json.Marshal(batchPayload)
	if err != nil {
		p.logger.Warn().Err(err).Msg("Failed to marshal batch payload")
		return
	}
	
	if _, err := p.gc.Publish(ctx, "dex.pairs", batchPayloadBytes); err != nil {
		p.logger.Warn().Err(err).Msg("Failed to publish batch update")
	} else {
		p.logger.Debug().
			Int("count", len(items)).
			Uint64("block", currentBlock).
			Msg("Published batch update")
	}
}

func (p *Publisher) Close() error {
	p.logger.Info().Msg("Closing publisher")
	p.cancel()
	p.wg.Wait()
	return nil
}
