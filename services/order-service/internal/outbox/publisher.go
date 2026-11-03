// Package outbox relays committed outbox rows to Kafka.
//
// Delivery is at-least-once: if the process dies after Kafka accepted a batch
// but before the rows were marked published, the batch is sent again.
// Consumers deduplicate by event ID.
package outbox

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
)

var (
	published = promauto.NewCounter(prometheus.CounterOpts{
		Name: "outbox_published_total",
		Help: "Outbox events published to Kafka.",
	})
	publishLag = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "outbox_publish_lag_seconds",
		Help:    "Time from an event being committed to it being accepted by Kafka.",
		Buckets: prometheus.ExponentialBuckets(0.005, 2, 14),
	})
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
	// NoDrain waits a full Interval even after a full batch, and
	// WritePerMessage sends one Kafka request per event. Both reproduce the
	// original publisher so its publish lag can be measured against this one.
	NoDrain         bool
	WritePerMessage bool
}

type Publisher struct {
	repo   Repository
	writer Writer
	cfg    Config
}

func New(repo Repository, writer Writer, cfg Config) *Publisher {
	if cfg.Interval <= 0 {
		cfg.Interval = 500 * time.Millisecond
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 100
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 12
	}
	return &Publisher{repo: repo, writer: writer, cfg: cfg}
}

// Run polls the outbox until ctx is cancelled. When a batch is full it polls
// again immediately to drain a backlog.
func (p *Publisher) Run(ctx context.Context) {
	for {
		n, err := p.PublishOnce(ctx)
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.ErrorContext(ctx, "publish outbox batch", "error", err)
		}
		if n == p.cfg.BatchSize && !p.cfg.NoDrain {
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
	n, err := p.repo.ClaimOutbox(ctx, p.cfg.BatchSize, p.cfg.MaxAttempts, func(batch []domain.OutboxEvent) (map[string]bool, error) {
		msgs := make([]kafka.Message, 0, len(batch))
		for _, e := range batch {
			msgs = append(msgs, kafka.Message{
				// Keyed by order: all events of one order go to one partition.
				Key:   []byte(e.AggregateID),
				Value: e.Payload,
				Headers: []kafka.Header{
					{Key: events.HeaderEventType, Value: []byte(e.EventType)},
					{Key: events.HeaderEventID, Value: []byte(e.ID)},
					{Key: events.HeaderTraceParent, Value: []byte(e.TraceParent)},
				},
			})
		}
		delivered := make(map[string]bool, len(batch))
		defer func() {
			for _, e := range batch {
				if delivered[e.ID] {
					published.Inc()
					publishLag.Observe(time.Since(e.CreatedAt).Seconds())
				}
			}
		}()
		if p.cfg.WritePerMessage {
			for i, m := range msgs {
				if err := p.writer.WriteMessages(ctx, m); err != nil {
					return delivered, err
				}
				delivered[batch[i].ID] = true
			}
			return delivered, nil
		}
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
	return n, err
}
