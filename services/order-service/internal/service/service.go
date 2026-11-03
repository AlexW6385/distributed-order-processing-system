// Package service implements the checkout saga.
//
// The order service is the saga coordinator. It owns the order state machine
// and calls product-service (reserve stock) and payment-service (charge) over
// gRPC. Compensation is not done with direct calls: every terminal transition
// writes an event to the outbox in the same transaction, and product-service
// confirms or releases stock when it consumes that event. That makes
// compensation reliable even if this process crashes right after the state
// change.
package service

import (
	"context"
	"errors"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
)

type Repository interface {
	CreatePending(ctx context.Context, email, createKey string) (*domain.Order, bool, error)
	MarkReserved(ctx context.Context, orderID string, items []domain.Item, totalCents int64, expiresAt time.Time) (*domain.Order, error)
	StartPayment(ctx context.Context, orderID, paymentKey string) (*domain.Order, error)
	MarkPaid(ctx context.Context, orderID string) (*domain.Order, error)
	Close(ctx context.Context, orderID string, from []string, to, reason string) (*domain.Order, error)
	Get(ctx context.Context, orderID string) (*domain.Order, error)
}

// ProductClient reserves stock. Errors wrap domain.ErrRejected for a definite
// refusal and domain.ErrUnavailable when the outcome is unknown.
type ProductClient interface {
	ReserveStock(ctx context.Context, orderID string, items []domain.ItemInput) ([]domain.Item, int64, error)
}

// PaymentClient charges an order. A decline is a result, not an error. Errors
// wrap domain.ErrUnavailable when the outcome is unknown.
type PaymentClient interface {
	Charge(ctx context.Context, orderID string, amountCents int64, idempotencyKey string) (domain.PaymentResult, error)
}

type Config struct {
	// PaymentWindow is how long a reserved order may wait for payment before
	// it expires and its stock is released.
	PaymentWindow time.Duration
}

type Service struct {
	repo    Repository
	product ProductClient
	payment PaymentClient
	cfg     Config
	logger  *logging.Logger
}

func New(repo Repository, product ProductClient, payment PaymentClient, cfg Config, logger *logging.Logger) *Service {
	if cfg.PaymentWindow <= 0 {
		cfg.PaymentWindow = 15 * time.Minute
	}
	return &Service{repo: repo, product: product, payment: payment, cfg: cfg, logger: logger}
}

type CreateInput struct {
	CustomerEmail  string
	Items          []domain.ItemInput
	IdempotencyKey string
}

// CreateOrder creates an order and reserves its stock.
//
// If the reservation fails for any reason, including a timeout where stock
// may in fact have been reserved, the order is closed as failed. The
// order.failed event makes product-service release whatever it holds for the
// order, and product-service rejects a late reservation for an order it has
// already released, so stock cannot leak either way.
func (s *Service) CreateOrder(ctx context.Context, in CreateInput) (*domain.Order, error) {
	items, err := validate(in)
	if err != nil {
		return nil, err
	}

	order, created, err := s.repo.CreatePending(ctx, strings.TrimSpace(in.CustomerEmail), in.IdempotencyKey)
	if err != nil {
		return nil, err
	}
	if !created {
		// Replay of an earlier request: return whatever state it reached.
		return order, nil
	}

	reserved, total, err := s.product.ReserveStock(ctx, order.ID, items)
	if err != nil {
		reason := "stock reservation failed: " + err.Error()
		if _, closeErr := s.repo.Close(ctx, order.ID, []string{domain.StatusPending}, domain.StatusFailed, reason); closeErr != nil {
			// The recovery worker closes pending orders that stay stuck.
			s.logger.Error("close order after reservation failure", closeErr, map[string]any{"order_id": order.ID})
		}
		return nil, err
	}

	reservedOrder, err := s.repo.MarkReserved(ctx, order.ID, reserved, total, time.Now().Add(s.cfg.PaymentWindow))
	if err != nil {
		// Most likely the recovery worker already closed the order as failed;
		// its event releases the stock we just reserved.
		return nil, err
	}
	return reservedOrder, nil
}

func (s *Service) GetOrder(ctx context.Context, orderID string) (*domain.Order, error) {
	if strings.TrimSpace(orderID) == "" {
		return nil, fmt.Errorf("%w: order id is required", domain.ErrInvalid)
	}
	return s.repo.Get(ctx, orderID)
}

// PayOrder charges a reserved order.
//
// The stock_reserved -> payment_pending transition is the lock: of many
// concurrent PayOrder calls only one wins it and calls payment-service. The
// others see payment_pending or paid and return the current order.
//
// If the charge outcome is unknown (timeout, connection reset), the order stays
// payment_pending and is returned as is. The recovery worker retries the
// charge later; payment-service deduplicates by order ID, so a retry can never
// charge twice.
func (s *Service) PayOrder(ctx context.Context, orderID, idempotencyKey string) (*domain.Order, error) {
	if strings.TrimSpace(idempotencyKey) == "" {
		return nil, fmt.Errorf("%w: idempotency key is required", domain.ErrInvalid)
	}
	order, err := s.GetOrder(ctx, orderID)
	if err != nil {
		return nil, err
	}
	switch order.Status {
	case domain.StatusPaid, domain.StatusPaymentPending:
		return order, nil
	case domain.StatusStockReserved:
	default:
		return nil, fmt.Errorf("%w: order is %s", domain.ErrConflict, order.Status)
	}

	order, err = s.repo.StartPayment(ctx, orderID, idempotencyKey)
	if errors.Is(err, domain.ErrConflict) {
		current, getErr := s.repo.Get(ctx, orderID)
		if getErr != nil {
			return nil, getErr
		}
		if current.Status == domain.StatusPaid || current.Status == domain.StatusPaymentPending {
			return current, nil
		}
		if current.Status == domain.StatusStockReserved {
			return nil, fmt.Errorf("%w: payment window has closed", domain.ErrConflict)
		}
		return nil, fmt.Errorf("%w: order is %s", domain.ErrConflict, current.Status)
	}
	if err != nil {
		return nil, err
	}
	return s.SettlePayment(ctx, order)
}

// SettlePayment charges an order in payment_pending and records the outcome.
// It is safe to call repeatedly for the same order.
func (s *Service) SettlePayment(ctx context.Context, order *domain.Order) (*domain.Order, error) {
	result, err := s.payment.Charge(ctx, order.ID, order.TotalCents, order.PaymentKey)
	if err != nil {
		if errors.Is(err, domain.ErrUnavailable) {
			s.logger.Info("payment outcome unknown, will retry", map[string]any{"order_id": order.ID, "error": err.Error()})
			return order, nil
		}
		// A definite error such as an amount mismatch: retrying will not help.
		return s.closeAfterPayment(ctx, order.ID, "payment rejected: "+err.Error())
	}
	if !result.Succeeded {
		return s.closeAfterPayment(ctx, order.ID, "payment declined: "+result.DeclineReason)
	}
	paid, err := s.repo.MarkPaid(ctx, order.ID)
	if errors.Is(err, domain.ErrConflict) {
		return s.repo.Get(ctx, order.ID)
	}
	return paid, err
}

func (s *Service) closeAfterPayment(ctx context.Context, orderID, reason string) (*domain.Order, error) {
	closed, err := s.repo.Close(ctx, orderID, []string{domain.StatusPaymentPending}, domain.StatusFailed, reason)
	if errors.Is(err, domain.ErrConflict) {
		return s.repo.Get(ctx, orderID)
	}
	return closed, err
}

// CancelOrder cancels a reserved order that has not started payment.
func (s *Service) CancelOrder(ctx context.Context, orderID string) (*domain.Order, error) {
	if strings.TrimSpace(orderID) == "" {
		return nil, fmt.Errorf("%w: order id is required", domain.ErrInvalid)
	}
	return s.repo.Close(ctx, orderID, []string{domain.StatusStockReserved}, domain.StatusCancelled, "cancelled by customer")
}

func validate(in CreateInput) ([]domain.ItemInput, error) {
	email := strings.TrimSpace(in.CustomerEmail)
	if email == "" {
		return nil, fmt.Errorf("%w: customer email is required", domain.ErrInvalid)
	}
	if _, err := mail.ParseAddress(email); err != nil {
		return nil, fmt.Errorf("%w: customer email is not valid", domain.ErrInvalid)
	}
	if len(in.Items) == 0 {
		return nil, fmt.Errorf("%w: at least one item is required", domain.ErrInvalid)
	}
	// Merge duplicate lines so product-service sees one line per product.
	merged := map[string]int32{}
	var order []string
	for _, item := range in.Items {
		id := strings.TrimSpace(item.ProductID)
		if id == "" || item.Quantity <= 0 {
			return nil, fmt.Errorf("%w: each item needs a product id and positive quantity", domain.ErrInvalid)
		}
		if _, seen := merged[id]; !seen {
			order = append(order, id)
		}
		merged[id] += item.Quantity
	}
	out := make([]domain.ItemInput, 0, len(order))
	for _, id := range order {
		out = append(out, domain.ItemInput{ProductID: id, Quantity: merged[id]})
	}
	return out, nil
}
