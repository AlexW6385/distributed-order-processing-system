package domain

import (
	"errors"
	"time"
)

// Order lifecycle:
//
//	pending ──reserve ok──▶ stock_reserved ──pay──▶ payment_pending ──succeeded──▶ paid ──refund──▶ refund_pending ──▶ refunded
//	   │                         │   │                     │
//	   └─reserve failed/timeout─▶ failed ◀────declined──────┘
//	                             │   └─deadline passed──▶ expired
//	                             └─customer cancels────▶ cancelled
//
// Every transition is a conditional UPDATE (WHERE status = <from>), so two
// concurrent actors can never both move the same order out of a state.
const (
	StatusPending        = "pending"
	StatusStockReserved  = "stock_reserved"
	StatusPaymentPending = "payment_pending"
	StatusPaid           = "paid"
	StatusFailed         = "failed"
	StatusExpired        = "expired"
	StatusCancelled      = "cancelled"
	StatusRefundPending  = "refund_pending"
	StatusRefunded       = "refunded"
)

var (
	ErrNotFound = errors.New("order not found")
	ErrInvalid  = errors.New("invalid request")
	// ErrConflict means the order is not in a state that allows the operation,
	// usually because another request or a background worker moved it first.
	ErrConflict = errors.New("order state conflict")
	// ErrRejected means a dependency gave a definite "no", such as
	// insufficient stock.
	ErrRejected = errors.New("rejected")
	// ErrUnavailable means a dependency call failed in a way that leaves its
	// outcome unknown; the operation may or may not have happened.
	ErrUnavailable = errors.New("dependency unavailable")
	// ErrNotSent means the call was refused locally (circuit breaker open):
	// the dependency is down, but certainly did not act on the request.
	ErrNotSent = errors.New("dependency unavailable, request not sent")
)

// IsTerminal reports whether the order has settled. A paid order is settled
// but can still be refunded.
func IsTerminal(status string) bool {
	switch status {
	case StatusPaid, StatusFailed, StatusExpired, StatusCancelled, StatusRefunded:
		return true
	}
	return false
}

type Order struct {
	ID            string
	CustomerEmail string
	Status        string
	TotalCents    int64
	FailureReason string
	PaymentKey    string
	Items         []Item
	ExpiresAt     *time.Time
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Item struct {
	ProductID      string
	ProductName    string
	Quantity       int32
	UnitPriceCents int64
	SubtotalCents  int64
}

type ItemInput struct {
	ProductID string
	Quantity  int32
}

type OutboxEvent struct {
	ID          string
	AggregateID string
	EventType   string
	Payload     []byte
	TraceParent string
	Attempts    int
	CreatedAt   time.Time
}

// PaymentResult is the definite outcome of a charge.
type PaymentResult struct {
	Succeeded     bool
	DeclineReason string
}
