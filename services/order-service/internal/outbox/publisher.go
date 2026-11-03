// Package outbox relays committed outbox rows to Kafka.
//
// Delivery is at-least-once: if the process dies after Kafka accepted a batch
// but before the rows were marked published, the batch is sent again.
// Consumers deduplicate by event ID.
package outbox

import (
	"context"
	"errors"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Repository interface {
	ClaimOutbox(ctx context.Context, limit, maxAttempts int, publish func([]domain.OutboxEvent) (map[string]bool, error)) (int, error)
}

type Writer interface {
	WriteMessages(ctx context.Context, msgs ...kafka.Message) error
}

type Config struct {
	Interval    time.Duration
	BatchSize   int
	MaxAttempts int
}

type Publisher struct {
	repo   Repository
	writer Writer
	cfg    Config
	logger *logging.Logger
}

func New(repo Repository, writer Writer, cfg Config, logger *logging.Logger) *Publisher {
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 12
	}
	return &Publisher{repo: repo, writer: writer, cfg: cfg, logger: logger}
}

// Run polls the outbox until ctx is cancelled. When a batch is full it polls
// again immediately to drain a backlog.
func (p *Publisher) Run(ctx context.Context) {
	for {
		n, err := p.PublishOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			p.logger.Error("publish outbox batch", err, nil)
		}
		if n == p.cfg.BatchSize {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(p.cfg.Interval):
		}
	}
}

// PublishOnce claims one batch and writes it to Kafka.
func (p *Publisher) PublishOnce(ctx context.Context) (int, error) {
	return p.repo.ClaimOutbox(ctx, p.cfg.BatchSize, p.cfg.MaxAttempts, func(batch []domain.OutboxEvent) (map[string]bool, error) {
		msgs := make([]kafka.Message, 0, len(batch))
		for _, e := range batch {
			msgs = append(msgs, kafka.Message{
				// Keyed by order: all events of one order go to one partition.
				Key:   []byte(e.AggregateID),
				Value: e.Payload,
				Headers: []kafka.Header{
					{Key: events.HeaderEventType, Value: []byte(e.EventType)},
					{Key: "event_id", Value: []byte(e.ID)},
				},
			})
		}
		delivered := make(map[string]bool, len(batch))
		err := p.writer.WriteMessages(ctx, msgs...)
		if err == nil {
			for _, e := range batch {
				delivered[e.ID] = true
			}
			return delivered, nil
		}
		// kafka-go reports per-message results for a partially failed batch.
		var writeErrs kafka.WriteErrors
		if errors.As(err, &writeErrs) {
			for i, e := range batch {
				if i < len(writeErrs) && writeErrs[i] == nil {
					delivered[e.ID] = true
				}
			}
		}
		return delivered, err
	})
}
