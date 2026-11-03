package service

import (
	"context"
	"fmt"

	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
)

type Repository interface {
	ListProducts(ctx context.Context) ([]domain.Product, error)
	ReserveStock(ctx context.Context, orderID string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error)
	Release(ctx context.Context, orderID string) error
	Confirm(ctx context.Context, orderID string) error
}

type Service struct {
	repo Repository
}

func New(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) ListProducts(ctx context.Context) ([]domain.Product, error) {
	return s.repo.ListProducts(ctx)
}

func (s *Service) ReserveStock(ctx context.Context, orderID string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error) {
	if orderID == "" {
		return nil, 0, fmt.Errorf("%w: order id is required", domain.ErrInvalid)
	}
	if len(lines) == 0 {
		return nil, 0, fmt.Errorf("%w: at least one item is required", domain.ErrInvalid)
	}
	seen := map[string]bool{}
	for _, line := range lines {
		if line.ProductID == "" || line.Quantity <= 0 {
			return nil, 0, fmt.Errorf("%w: each item needs a product id and positive quantity", domain.ErrInvalid)
		}
		if seen[line.ProductID] {
			return nil, 0, fmt.Errorf("%w: duplicate product %s", domain.ErrInvalid, line.ProductID)
		}
		seen[line.ProductID] = true
	}
	return s.repo.ReserveStock(ctx, orderID, lines)
}

// Release and Confirm are driven by order events, not by RPCs.

func (s *Service) Release(ctx context.Context, orderID string) error {
	return s.repo.Release(ctx, orderID)
}

func (s *Service) Confirm(ctx context.Context, orderID string) error {
	return s.repo.Confirm(ctx, orderID)
}
