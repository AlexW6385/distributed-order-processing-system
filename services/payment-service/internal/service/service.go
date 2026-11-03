// Package service simulates a payment provider.
//
// It is deliberately unreliable when configured to be: FaultRate makes some
// charges succeed but answer slowly, so the caller times out without knowing
// the outcome. That is the failure a real payment integration has to survive.
package service

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
	"github.com/google/uuid"
)

type Repository interface {
	Insert(ctx context.Context, p domain.Payment) (*domain.Payment, error)
	FindByOrder(ctx context.Context, orderID string) (*domain.Payment, error)
}

type Config struct {
	// DeclineAboveCents declines charges above this amount. 0 disables.
	DeclineAboveCents int64
	// FaultRate is the probability (0..1) that a new charge is recorded but
	// the response is delayed by FaultDelay.
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
		return s.checkReplay(existing, amountCents)
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
		// A concurrent charge for the same order won the insert.
		return s.checkReplay(stored, amountCents)
	}

	if s.cfg.FaultRate > 0 && rand.Float64() < s.cfg.FaultRate {
		// The charge is committed, but the caller will likely give up before
		// it hears back.
		select {
		case <-time.After(s.cfg.FaultDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return stored, nil
}

func (s *Service) checkReplay(existing *domain.Payment, amountCents int64) (*domain.Payment, error) {
	if existing.AmountCents != amountCents {
		return nil, fmt.Errorf("%w: order %s was charged %d cents, not %d",
			domain.ErrMismatch, existing.OrderID, existing.AmountCents, amountCents)
	}
	return existing, nil
}
