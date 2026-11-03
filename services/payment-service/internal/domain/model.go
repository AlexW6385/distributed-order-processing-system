package domain

import "errors"

const (
	StatusSucceeded = "succeeded"
	StatusDeclined  = "declined"
)

var (
	ErrInvalid = errors.New("invalid request")
	// ErrMismatch means a charge was retried for an order with different
	// details than the original charge.
	ErrMismatch = errors.New("charge does not match the original request")
)

type Payment struct {
	ID             string
	OrderID        string
	IdempotencyKey string
	AmountCents    int64
	Status         string
	DeclineReason  string
}
