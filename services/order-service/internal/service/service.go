package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
)

type Repository interface {
	CreatePending(ctx context.Context, email string) (*domain.Order, error)
	SaveReserved(ctx context.Context, orderID string, items []domain.Item, totalCents int64) (*domain.Order, error)
	MarkFailed(ctx context.Context, orderID string, reason string) error
	MarkPaidAndCreateOutbox(ctx context.Context, order *domain.Order) (*domain.Order, error)
	Get(ctx context.Context, orderID string) (*domain.Order, error)
}

type ProductClient interface {
	ReserveStock(ctx context.Context, orderID string, items []domain.ItemInput) ([]domain.Item, int64, error)
	ReleaseReservation(ctx context.Context, orderID string) error
	ConfirmReservation(ctx context.Context, orderID string) error
}

type PaymentClient interface {
	PayOrder(ctx context.Context, orderID string, amountCents int64, idempotencyKey string) error
}

type Service struct {
	repo    Repository
	product ProductClient
	payment PaymentClient
	logger  *logging.Logger
}

func New(repo Repository, product ProductClient, payment PaymentClient, logger *logging.Logger) *Service {
	return &Service{repo: repo, product: product, payment: payment, logger: logger}
}

func (s *Service) CreateOrder(ctx context.Context, email string, input []domain.ItemInput) (*domain.Order, error) {
	if strings.TrimSpace(email) == "" {
		return nil, errors.New("customer email is required")
	}
	if len(input) == 0 {
		return nil, errors.New("at least one item is required")
	}
	for _, item := range input {
		if strings.TrimSpace(item.ProductID) == "" || item.Quantity <= 0 {
			return nil, errors.New("each item needs a product id and positive quantity")
		}
	}

	order, err := s.repo.CreatePending(ctx, email)
	if err != nil {
		return nil, err
	}

	items, total, err := s.product.ReserveStock(ctx, order.ID, input)
	if err != nil {
		_ = s.repo.MarkFailed(ctx, order.ID, err.Error())
		_ = s.product.ReleaseReservation(ctx, order.ID)
		return nil, fmt.Errorf("reserve stock: %w", err)
	}
	return s.repo.SaveReserved(ctx, order.ID, items, total)
}

func (s *Service) GetOrder(ctx context.Context, orderID string) (*domain.Order, error) {
	if orderID == "" {
		return nil, errors.New("order id is required")
	}
	return s.repo.Get(ctx, orderID)
}

func (s *Service) PayOrder(ctx context.Context, orderID string, idempotencyKey string) (*domain.Order, error) {
	if idempotencyKey == "" {
		return nil, errors.New("idempotency key is required")
	}
	order, err := s.repo.Get(ctx, orderID)
	if err != nil {
		return nil, err
	}
	if order.Status == domain.StatusPaid {
		return order, nil
	}
	if order.Status != domain.StatusStockReserved {
		return nil, fmt.Errorf("order status %s cannot be paid", order.Status)
	}

	if err := s.payment.PayOrder(ctx, order.ID, order.TotalCents, idempotencyKey); err != nil {
		_ = s.product.ReleaseReservation(ctx, order.ID)
		_ = s.repo.MarkFailed(ctx, order.ID, err.Error())
		return nil, fmt.Errorf("pay order: %w", err)
	}

	paid, err := s.repo.MarkPaidAndCreateOutbox(ctx, order)
	if err != nil {
		return nil, err
	}
	if err := s.product.ConfirmReservation(ctx, order.ID); err != nil {
		s.logger.Error("stock confirmation failed after payment", err, map[string]any{"order_id": order.ID})
	}
	return paid, nil
}
