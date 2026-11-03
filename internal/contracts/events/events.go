// Package events is the shared contract for messages on the order event topic.
//
// It plays the same role for Kafka that the proto files play for gRPC: the
// producer (order-service) and the consumers (product-service,
// notification-service) agree on this envelope and nothing else.
package events

import (
	"encoding/json"
	"time"
)

// Topic carries every order lifecycle event. Messages are keyed by order ID so
// all events for one order land on the same partition, in order.
const Topic = "order.events"

// DeadLetterTopic receives messages a consumer could not process.
const DeadLetterTopic = "order.events.dlq"

const (
	OrderPaid      = "order.paid"
	OrderFailed    = "order.failed"
	OrderExpired   = "order.expired"
	OrderCancelled = "order.cancelled"
	OrderRefunded  = "order.refunded"
)

// Kafka headers set on every message: the event type (so consumers can route
// without decoding), the event ID, and the W3C trace context of the request
// that caused the event.
const (
	HeaderEventType   = "event_type"
	HeaderEventID     = "event_id"
	HeaderTraceParent = "traceparent"
)

// OrderEvent is the payload of every message on Topic.
type OrderEvent struct {
	EventID       string    `json:"event_id"`
	EventType     string    `json:"event_type"`
	OrderID       string    `json:"order_id"`
	CustomerEmail string    `json:"customer_email"`
	TotalCents    int64     `json:"total_cents"`
	Reason        string    `json:"reason,omitempty"`
	OccurredAt    time.Time `json:"occurred_at"`
}

// ReleasesStock reports whether this event means the order will never be paid,
// so any stock held for it must be returned.
func (e OrderEvent) ReleasesStock() bool {
	switch e.EventType {
	case OrderFailed, OrderExpired, OrderCancelled:
		return true
	}
	return false
}

func Decode(data []byte) (OrderEvent, error) {
	var event OrderEvent
	err := json.Unmarshal(data, &event)
	return event, err
}
