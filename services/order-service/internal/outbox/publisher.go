package outbox

import (
	"context"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Repository interface {
	PendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error)
	MarkOutboxPublished(ctx context.Context, eventID string) error
	MarkOutboxFailed(ctx context.Context, eventID string, reason string) error
}

type Publisher struct {
	repo   Repository
	writer *kafka.Writer
	logger *logging.Logger
}

func New(repo Repository, writer *kafka.Writer, logger *logging.Logger) *Publisher {
	return &Publisher{repo: repo, writer: writer, logger: logger}
}

func (p *Publisher) Run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.publishBatch(ctx)
		}
	}
}

func (p *Publisher) publishBatch(ctx context.Context) {
	events, err := p.repo.PendingOutbox(ctx, 20)
	if err != nil {
		p.logger.Error("fetch outbox events failed", err, nil)
		return
	}
	for _, event := range events {
		err := p.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(event.ID),
			Value: event.Payload,
			Headers: []kafka.Header{
				{Key: "event_type", Value: []byte(event.EventType)},
			},
		})
		if err != nil {
			_ = p.repo.MarkOutboxFailed(ctx, event.ID, err.Error())
			p.logger.Error("publish outbox event failed", err, map[string]any{"event_id": event.ID})
			continue
		}
		if err := p.repo.MarkOutboxPublished(ctx, event.ID); err != nil {
			p.logger.Error("mark outbox event published failed", err, map[string]any{"event_id": event.ID})
		}
	}
}
