// Package service simulates a payment provider in front of a double-entry
// ledger.
//
// It is deliberately unreliable when configured to be: FaultRate makes some
// new charges and refunds commit but answer slowly, so the caller times out
// without knowing the outcome. That is the failure a real payment
// integration has to survive.
package service

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	charges = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_charges_total",
		Help: "New charges recorded, by status (succeeded, declined).",
	}, []string{"status"})
	refunds = promauto.NewCounter(prometheus.CounterOpts{
		Name: "payments_refunds_total",
		Help: "New refunds recorded.",
	})
	replays = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "payments_replays_total",
		Help: "Charge or refund requests answered from an earlier result (retries).",
	}, []string{"operation"})
)

type Repository interface {
	Insert(ctx context.Context, p domain.Payment) (*domain.Payment, error)
	FindByOrder(ctx context.Context, orderID string) (*domain.Payment, error)
	Refund(ctx context.Context, orderID string, amountCents int64) (*domain.Refund, bool, error)
	List(ctx context.Context, afterCreated time.Time, afterID string, limit int) ([]domain.Payment, error)
	LedgerSummary(ctx context.Context) (domain.LedgerSummary, error)
}

type Config struct {
	// DeclineAboveCents declines charges above this amount. 0 disables.
	DeclineAboveCents int64
	// FaultRate is the probability (0..1) that a new charge or refund is
	// recorded but the response is delayed by FaultDelay.
	FaultRate  float64
	FaultDelay time.Duration
}

type Service struct {
	repo Repository
	cfg  Config
}

func New(repo Repository, cfg Config) *Service {
	if cfg.FaultDelay <= 0 {
		cfg.FaultDelay = 5 * time.Second
	}
	return &Service{repo: repo, cfg: cfg}
}

// Charge charges an order at most once and returns the recorded result.
func (s *Service) Charge(ctx context.Context, orderID string, amountCents int64, key string) (*domain.Payment, error) {
	if orderID == "" {
		return nil, fmt.Errorf("%w: order id is required", domain.ErrInvalid)
	}
	if amountCents <= 0 {
		return nil, fmt.Errorf("%w: amount must be positive", domain.ErrInvalid)
	}
	if existing, err := s.repo.FindByOrder(ctx, orderID); err != nil {
		return nil, err
	} else if existing != nil {
		replays.WithLabelValues("charge").Inc()
		return checkReplay(existing, amountCents)
	}

	payment := domain.Payment{
		ID:             uuid.NewString(),
		OrderID:        orderID,
		IdempotencyKey: key,
		AmountCents:    amountCents,
		Status:         domain.StatusSucceeded,
	}
	if s.cfg.DeclineAboveCents > 0 && amountCents > s.cfg.DeclineAboveCents {
		payment.Status = domain.StatusDeclined
		payment.DeclineReason = fmt.Sprintf("amount exceeds limit of %d cents", s.cfg.DeclineAboveCents)
	}
	stored, err := s.repo.Insert(ctx, payment)
	if err != nil {
		return nil, err
	}
	if stored.ID != payment.ID {
		replays.WithLabelValues("charge").Inc() // a concurrent charge won the insert
		return checkReplay(stored, amountCents)
	}
	charges.WithLabelValues(stored.Status).Inc()
	return stored, s.maybeStall(ctx)
}

// Refund refunds an order's successful charge in full, at most once.
func (s *Service) Refund(ctx context.Context, orderID string, amountCents int64) (*domain.Refund, error) {
	if orderID == "" || amountCents <= 0 {
		return nil, fmt.Errorf("%w: order id and a positive amount are required", domain.ErrInvalid)
	}
	refund, created, err := s.repo.Refund(ctx, orderID, amountCents)
	if err != nil {
		return nil, err
	}
	if !created {
		replays.WithLabelValues("refund").Inc()
		return refund, nil
	}
	refunds.Inc()
	return refund, s.maybeStall(ctx)
}

func (s *Service) List(ctx context.Context, afterCreated time.Time, afterID string, limit int) ([]domain.Payment, error) {
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	return s.repo.List(ctx, afterCreated, afterID, limit)
}

func (s *Service) LedgerSummary(ctx context.Context) (domain.LedgerSummary, error) {
	return s.repo.LedgerSummary(ctx)
}

// maybeStall delays the response to a committed operation, so the caller may
// give up before hearing back.
func (s *Service) maybeStall(ctx context.Context) error {
	if s.cfg.FaultRate <= 0 || rand.Float64() >= s.cfg.FaultRate {
		return nil
	}
	select {
	case <-time.After(s.cfg.FaultDelay):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func checkReplay(existing *domain.Payment, amountCents int64) (*domain.Payment, error) {
	if existing.AmountCents != amountCents {
		return nil, fmt.Errorf("%w: order %s was charged %d cents, not %d",
			domain.ErrMismatch, existing.OrderID, existing.AmountCents, amountCents)
	}
	return existing, nil
}
