package service

import (
	"context"
	"errors"

	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
)

type Repository interface {
	ListProducts(ctx context.Context) ([]domain.Product, error)
	ReserveStock(ctx context.Context, orderID string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error)
	ReleaseReservation(ctx context.Context, orderID string) error
	ConfirmReservation(ctx context.Context, orderID string) error
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
		return nil, 0, errors.New("order id is required")
	}
	if len(lines) == 0 {
		return nil, 0, errors.New("at least one item is required")
	}
	for _, line := range lines {
		if line.ProductID == "" || line.Quantity <= 0 {
			return nil, 0, errors.New("each item needs a product id and positive quantity")
		}
	}
	return s.repo.ReserveStock(ctx, orderID, lines)
}

func (s *Service) ReleaseReservation(ctx context.Context, orderID string) error {
	if orderID == "" {
		return errors.New("order id is required")
	}
	return s.repo.ReleaseReservation(ctx, orderID)
}

func (s *Service) ConfirmReservation(ctx context.Context, orderID string) error {
	if orderID == "" {
		return errors.New("order id is required")
	}
	return s.repo.ConfirmReservation(ctx, orderID)
}
