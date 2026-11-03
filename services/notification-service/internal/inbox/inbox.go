// Package inbox turns order events into customer notifications exactly once,
// on top of at-least-once delivery.
package inbox

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
)

var (
	sent = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "notifications_sent_total",
		Help: "Notifications sent, by event type.",
	}, []string{"event_type"})
	duplicates = promauto.NewCounter(prometheus.CounterOpts{
		Name: "notifications_duplicate_events_total",
		Help: "Redelivered events skipped by the inbox.",
	})
)

type Inbox struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Inbox {
	return &Inbox{db: db}
}

// Handler records the event ID and writes the notification in one
// transaction. A redelivered event hits the primary key on processed_events
// and is skipped, so the customer is never notified twice.
func (in *Inbox) Handler() platformkafka.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		event, err := events.Decode(msg.Value)
		if err != nil || event.EventID == "" {
			return fmt.Errorf("decode event: %v: %w", err, platformkafka.ErrPermanent)
		}
		subject, body, ok := render(event)
		if !ok {
			return nil
		}

		tx, err := in.db.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx) //nolint:errcheck

		tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, event.EventID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() == 0 {
			duplicates.Inc()
			return nil
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO notifications (id, event_id, order_id, recipient, subject, body)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			uuid.NewString(), event.EventID, event.OrderID, event.CustomerEmail, subject, body); err != nil {
			return err
		}
		if err := tx.Commit(ctx); err != nil {
			return err
		}
		sent.WithLabelValues(event.EventType).Inc()
		slog.InfoContext(ctx, "notification sent", "order_id", event.OrderID, "type", event.EventType)
		return nil
	}
}

func render(e events.OrderEvent) (subject, body string, ok bool) {
	amount := fmt.Sprintf("$%d.%02d", e.TotalCents/100, e.TotalCents%100)
	switch e.EventType {
	case events.OrderPaid:
		return "Order confirmed", fmt.Sprintf("We received your payment of %s for order %s.", amount, e.OrderID), true
	case events.OrderFailed:
		return "Order could not be completed", fmt.Sprintf("Order %s failed: %s.", e.OrderID, e.Reason), true
	case events.OrderExpired:
		return "Order expired", fmt.Sprintf("Order %s expired before payment and its items were released.", e.OrderID), true
	case events.OrderCancelled:
		return "Order cancelled", fmt.Sprintf("Order %s was cancelled.", e.OrderID), true
	case events.OrderRefunded:
		return "Refund issued", fmt.Sprintf("We refunded %s for order %s.", amount, e.OrderID), true
	}
	return "", "", false
}
