package service

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/flashsale"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/sync/singleflight"
)

var (
	reservations = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stock_reservations_total",
		Help: "Reservation attempts, by result (reserved, insufficient, closed, flash_sold_out, flash_limit, error).",
	}, []string{"result"})
	restored = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "stock_restored_units_total",
		Help: "Units returned to stock, by reason (release, restock).",
	}, []string{"reason"})
)

type Repository interface {
	ListProducts(ctx context.Context) ([]domain.Product, error)
	ReserveStock(ctx context.Context, orderID, customer string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error)
	Release(ctx context.Context, orderID string) ([]domain.ReservedItem, string, error)
	Restock(ctx context.Context, orderID string) ([]domain.ReservedItem, string, error)
	Confirm(ctx context.Context, orderID string) error
	SetStock(ctx context.Context, productID string, stock int32) error
}

// Cache is a cache-aside store for the product catalog. Postgres stays the
// source of truth; the cache may be missing, stale for up to its TTL, or down.
type Cache interface {
	GetProducts(ctx context.Context) ([]domain.Product, bool, error)
	SetProducts(ctx context.Context, products []domain.Product) error
	Invalidate(ctx context.Context) error
}

// Gate is the flash-sale admission gate.
type Gate interface {
	Start(ctx context.Context, productID string, stock int) error
	Admit(ctx context.Context, productID, orderID, customer string, qty int32) (bool, error)
	Undo(ctx context.Context, productID, orderID, customer string, qty int32) error
}

type Service struct {
	repo  Repository
	cache Cache // optional
	gate  Gate  // optional
	load  singleflight.Group
}

func New(repo Repository, cache Cache, gate Gate) *Service {
	return &Service{repo: repo, cache: cache, gate: gate}
}

// ListProducts serves the catalog from cache when it can. On a miss,
// concurrent callers share one database read (singleflight), so an expired key
// does not send a burst of identical queries to Postgres. Stock shown here is
// advisory: reservations always check stock in Postgres under a row lock.
func (s *Service) ListProducts(ctx context.Context) ([]domain.Product, error) {
	if s.cache == nil {
		return s.repo.ListProducts(ctx)
	}
	if products, ok, err := s.cache.GetProducts(ctx); err == nil && ok {
		return products, nil
	} else if err != nil {
		slog.WarnContext(ctx, "product cache read failed, using database", "error", err)
	}
	v, err, _ := s.load.Do("products", func() (any, error) {
		products, err := s.repo.ListProducts(ctx)
		if err != nil {
			return nil, err
		}
		if err := s.cache.SetProducts(ctx, products); err != nil {
			slog.WarnContext(ctx, "product cache write failed", "error", err)
		}
		return products, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]domain.Product), nil
}

// ReserveStock reserves stock for an order. Lines for products on flash sale
// pass the Redis gate first; requests the gate rejects never reach Postgres.
// If Redis is unavailable the gate is skipped: Postgres alone still prevents
// overselling.
func (s *Service) ReserveStock(ctx context.Context, orderID, customer string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error) {
	if err := validate(orderID, lines); err != nil {
		return nil, 0, err
	}
	admitted, err := s.admit(ctx, orderID, customer, lines)
	if err != nil {
		return nil, 0, err
	}
	items, total, err := s.repo.ReserveStock(ctx, orderID, customer, lines)
	if err != nil {
		s.undo(ctx, orderID, customer, admitted)
		reservations.WithLabelValues(resultLabel(err)).Inc()
		return nil, 0, err
	}
	reservations.WithLabelValues("reserved").Inc()
	s.invalidate(ctx)
	return items, total, nil
}

func (s *Service) admit(ctx context.Context, orderID, customer string, lines []domain.ReservationLine) ([]domain.ReservationLine, error) {
	if s.gate == nil {
		return nil, nil
	}
	var admitted []domain.ReservationLine
	for _, line := range lines {
		ok, err := s.gate.Admit(ctx, line.ProductID, orderID, customer, line.Quantity)
		switch {
		case errors.Is(err, flashsale.ErrSoldOut), errors.Is(err, flashsale.ErrLimit):
			s.undo(ctx, orderID, customer, admitted)
			reservations.WithLabelValues(resultLabel(err)).Inc()
			return nil, fmt.Errorf("%w: %v", domain.ErrInsufficientStock, err)
		case err != nil:
			slog.WarnContext(ctx, "flash-sale gate unavailable, using database only", "error", err)
			return admitted, nil
		case ok:
			admitted = append(admitted, line)
		}
	}
	return admitted, nil
}

func (s *Service) undo(ctx context.Context, orderID, customer string, lines []domain.ReservationLine) {
	if s.gate == nil {
		return
	}
	for _, line := range lines {
		if err := s.gate.Undo(ctx, line.ProductID, orderID, customer, line.Quantity); err != nil {
			slog.WarnContext(ctx, "return flash-sale units failed", "order_id", orderID, "error", err)
		}
	}
}

// Release returns the stock of an order that will never be paid.
func (s *Service) Release(ctx context.Context, orderID string) error {
	items, customer, err := s.repo.Release(ctx, orderID)
	return s.afterRestore(ctx, orderID, customer, items, "release", err)
}

// Restock returns the stock of a refunded order.
func (s *Service) Restock(ctx context.Context, orderID string) error {
	items, customer, err := s.repo.Restock(ctx, orderID)
	return s.afterRestore(ctx, orderID, customer, items, "restock", err)
}

func (s *Service) afterRestore(ctx context.Context, orderID, customer string, items []domain.ReservedItem, reason string, err error) error {
	if err != nil {
		return err
	}
	lines := make([]domain.ReservationLine, 0, len(items))
	for _, item := range items {
		lines = append(lines, domain.ReservationLine{ProductID: item.ProductID, Quantity: item.Quantity})
		restored.WithLabelValues(reason).Add(float64(item.Quantity))
	}
	s.undo(ctx, orderID, customer, lines) // a no-op for products not on flash sale
	if len(items) > 0 {
		s.invalidate(ctx)
	}
	return nil
}

func (s *Service) Confirm(ctx context.Context, orderID string) error {
	return s.repo.Confirm(ctx, orderID)
}

// StartFlashSale sets the product's stock in Postgres and opens the Redis
// gate with the same number of units.
func (s *Service) StartFlashSale(ctx context.Context, productID string, stock int32) error {
	if productID == "" || stock <= 0 {
		return fmt.Errorf("%w: product id and positive stock are required", domain.ErrInvalid)
	}
	if s.gate == nil {
		return fmt.Errorf("%w: flash sales need Redis", domain.ErrInvalid)
	}
	if err := s.repo.SetStock(ctx, productID, stock); err != nil {
		return err
	}
	s.invalidate(ctx)
	return s.gate.Start(ctx, productID, int(stock))
}

// invalidate drops the cached catalog after stock changed. A reader that
// loaded the old rows just before the write can still put them back, so the
// short TTL bounds how stale the catalog can get.
func (s *Service) invalidate(ctx context.Context) {
	if s.cache == nil {
		return
	}
	if err := s.cache.Invalidate(ctx); err != nil {
		slog.WarnContext(ctx, "product cache invalidation failed", "error", err)
	}
}

func validate(orderID string, lines []domain.ReservationLine) error {
	if orderID == "" {
		return fmt.Errorf("%w: order id is required", domain.ErrInvalid)
	}
	if len(lines) == 0 {
		return fmt.Errorf("%w: at least one item is required", domain.ErrInvalid)
	}
	seen := map[string]bool{}
	for _, line := range lines {
		if line.ProductID == "" || line.Quantity <= 0 {
			return fmt.Errorf("%w: each item needs a product id and positive quantity", domain.ErrInvalid)
		}
		if seen[line.ProductID] {
			return fmt.Errorf("%w: duplicate product %s", domain.ErrInvalid, line.ProductID)
		}
		seen[line.ProductID] = true
	}
	return nil
}

func resultLabel(err error) string {
	switch {
	case errors.Is(err, flashsale.ErrSoldOut):
		return "flash_sold_out"
	case errors.Is(err, flashsale.ErrLimit):
		return "flash_limit"
	case errors.Is(err, domain.ErrInsufficientStock):
		return "insufficient"
	case errors.Is(err, domain.ErrReservationClosed):
		return "closed"
	}
	return "error"
}
