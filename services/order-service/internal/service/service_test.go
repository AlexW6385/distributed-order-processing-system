package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
)

func TestCreateOrderMarksFailedWhenStockReservationFails(t *testing.T) {
	repo := newFakeRepo()
	product := &fakeProduct{reserveErr: errors.New("no stock")}
	svc := New(repo, product, &fakePayment{}, logging.New("test"))

	_, err := svc.CreateOrder(context.Background(), "alex@example.com", []domain.ItemInput{
		{ProductID: "prod-coffee-001", Quantity: 2},
	})
	if err == nil {
		t.Fatal("expected reserve error")
	}
	if repo.order.Status != domain.StatusFailed {
		t.Fatalf("status = %s, want %s", repo.order.Status, domain.StatusFailed)
	}
	if !product.released {
		t.Fatal("expected reservation release to be attempted")
	}
}

func TestPayOrderReleasesStockWhenPaymentFails(t *testing.T) {
	repo := newFakeRepo()
	repo.order = reservedOrder()
	product := &fakeProduct{}
	svc := New(repo, product, &fakePayment{err: errors.New("card declined")}, logging.New("test"))

	_, err := svc.PayOrder(context.Background(), repo.order.ID, "checkout-1")
	if err == nil {
		t.Fatal("expected payment error")
	}
	if repo.order.Status != domain.StatusFailed {
		t.Fatalf("status = %s, want %s", repo.order.Status, domain.StatusFailed)
	}
	if !product.released {
		t.Fatal("expected stock release")
	}
}

func TestPayOrderMarksPaidAndConfirmsReservation(t *testing.T) {
	repo := newFakeRepo()
	repo.order = reservedOrder()
	product := &fakeProduct{}
	svc := New(repo, product, &fakePayment{}, logging.New("test"))

	order, err := svc.PayOrder(context.Background(), repo.order.ID, "checkout-1")
	if err != nil {
		t.Fatalf("pay order: %v", err)
	}
	if order.Status != domain.StatusPaid {
		t.Fatalf("status = %s, want %s", order.Status, domain.StatusPaid)
	}
	if !product.confirmed {
		t.Fatal("expected reservation confirmation")
	}
}

type fakeRepo struct {
	order *domain.Order
}

func newFakeRepo() *fakeRepo {
	return &fakeRepo{}
}

func (r *fakeRepo) CreatePending(ctx context.Context, email string) (*domain.Order, error) {
	r.order = &domain.Order{
		ID:            "order-1",
		CustomerEmail: email,
		Status:        domain.StatusPending,
		CreatedAt:     time.Now(),
		UpdatedAt:     time.Now(),
	}
	return r.order, nil
}

func (r *fakeRepo) SaveReserved(ctx context.Context, orderID string, items []domain.Item, totalCents int64) (*domain.Order, error) {
	r.order.Status = domain.StatusStockReserved
	r.order.Items = items
	r.order.TotalCents = totalCents
	return r.order, nil
}

func (r *fakeRepo) MarkFailed(ctx context.Context, orderID string, reason string) error {
	r.order.Status = domain.StatusFailed
	r.order.FailureReason = reason
	return nil
}

func (r *fakeRepo) MarkPaidAndCreateOutbox(ctx context.Context, order *domain.Order) (*domain.Order, error) {
	r.order.Status = domain.StatusPaid
	return r.order, nil
}

func (r *fakeRepo) Get(ctx context.Context, orderID string) (*domain.Order, error) {
	return r.order, nil
}

type fakeProduct struct {
	reserveErr error
	released   bool
	confirmed  bool
}

func (f *fakeProduct) ReserveStock(ctx context.Context, orderID string, items []domain.ItemInput) ([]domain.Item, int64, error) {
	if f.reserveErr != nil {
		return nil, 0, f.reserveErr
	}
	return []domain.Item{{
		ProductID:      "prod-coffee-001",
		ProductName:    "Single Origin Coffee",
		Quantity:       2,
		UnitPriceCents: 1599,
		SubtotalCents:  3198,
	}}, 3198, nil
}

func (f *fakeProduct) ReleaseReservation(ctx context.Context, orderID string) error {
	f.released = true
	return nil
}

func (f *fakeProduct) ConfirmReservation(ctx context.Context, orderID string) error {
	f.confirmed = true
	return nil
}

type fakePayment struct {
	err error
}

func (f *fakePayment) PayOrder(ctx context.Context, orderID string, amountCents int64, idempotencyKey string) error {
	return f.err
}

func reservedOrder() *domain.Order {
	return &domain.Order{
		ID:            "order-1",
		CustomerEmail: "alex@example.com",
		Status:        domain.StatusStockReserved,
		TotalCents:    3198,
		Items: []domain.Item{{
			ProductID:      "prod-coffee-001",
			ProductName:    "Single Origin Coffee",
			Quantity:       2,
			UnitPriceCents: 1599,
			SubtotalCents:  3198,
		}},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
}
