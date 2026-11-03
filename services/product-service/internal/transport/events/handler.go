// Package events applies order lifecycle events to stock reservations.
package events

import (
	"context"
	"errors"
	"fmt"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"github.com/segmentio/kafka-go"
)

type Reservations interface {
	Release(ctx context.Context, orderID string) error
	Restock(ctx context.Context, orderID string) error
	Confirm(ctx context.Context, orderID string) error
}

// Handler confirms stock for paid orders, releases it for orders that will
// never be paid, and returns it for refunded orders. Release and Confirm are idempotent, so redelivered events are
// harmless and no separate deduplication table is needed.
func Handler(res Reservations) platformkafka.Handler {
	return func(ctx context.Context, msg kafka.Message) error {
		event, err := events.Decode(msg.Value)
		if err != nil {
			return fmt.Errorf("decode event: %v: %w", err, platformkafka.ErrPermanent)
		}
		switch {
		case event.EventType == events.OrderPaid:
			err = res.Confirm(ctx, event.OrderID)
		case event.ReleasesStock():
			err = res.Release(ctx, event.OrderID)
		case event.EventType == events.OrderRefunded:
			err = res.Restock(ctx, event.OrderID)
		default:
			return nil
		}
		if errors.Is(err, domain.ErrInconsistent) {
			return fmt.Errorf("%v: %w", err, platformkafka.ErrPermanent)
		}
		return err
	}
}
