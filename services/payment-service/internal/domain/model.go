package domain

import (
	"errors"
	"time"
)

const (
	StatusSucceeded = "succeeded"
	StatusDeclined  = "declined"
)

var (
	ErrInvalid  = errors.New("invalid request")
	ErrNotFound = errors.New("not found")
	// ErrMismatch means a charge or refund was retried with different details
	// than the original, or refers to a payment that cannot be refunded.
	ErrMismatch = errors.New("request does not match the recorded payment")
)

type Payment struct {
	ID             string
	OrderID        string
	IdempotencyKey string
	AmountCents    int64
	Status         string
	DeclineReason  string
	Refunded       bool
	CreatedAt      time.Time
}

type Refund struct {
	ID          string
	OrderID     string
	AmountCents int64
}

// LedgerSummary is a trial balance of the ledger.
type LedgerSummary struct {
	TotalCents    int64 // sum of all lines; zero when the books balance
	CashCents     int64
	RevenueCents  int64
	ChargedCents  int64
	RefundedCents int64
}
