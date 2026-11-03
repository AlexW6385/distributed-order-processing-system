package domain

import "errors"

var (
	ErrInvalid           = errors.New("invalid request")
	ErrNotFound          = errors.New("not found")
	ErrInsufficientStock = errors.New("insufficient stock")
	// ErrReservationClosed means the order's reservation was already released
	// (or confirmed), so it cannot be reserved again.
	ErrReservationClosed = errors.New("reservation closed")
	// ErrInconsistent means an event asked for a transition that should be
	// impossible, such as confirming a released reservation.
	ErrInconsistent = errors.New("inconsistent reservation state")
)

const (
	ReservationReserved  = "reserved"
	ReservationConfirmed = "confirmed"
	ReservationReleased  = "released"
)

type Product struct {
	ID          string
	Name        string
	Description string
	PriceCents  int64
	Stock       int32
}

type ReservationLine struct {
	ProductID string
	Quantity  int32
}

type ReservedItem struct {
	ProductID      string
	ProductName    string
	Quantity       int32
	UnitPriceCents int64
	SubtotalCents  int64
}
