package service

import (
	"context"
	"fmt"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"golang.org/x/sync/singleflight"
)

type Repository interface {
	ListProducts(ctx context.Context) ([]domain.Product, error)
	ReserveStock(ctx context.Context, orderID string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error)
	Release(ctx context.Context, orderID string) error
	Confirm(ctx context.Context, orderID string) error
}

// Cache is a cache-aside store for the product catalog. Postgres stays the
// source of truth; the cache may be missing, stale for up to its TTL, or down.
type Cache interface {
	GetProducts(ctx context.Context) ([]domain.Product, bool, error)
	SetProducts(ctx context.Context, products []domain.Product) error
	Invalidate(ctx context.Context) error
}

type Service struct {
	repo  Repository
	cache Cache // optional
	load  singleflight.Group
	log   *logging.Logger
}

func New(repo Repository, cache Cache, log *logging.Logger) *Service {
	return &Service{repo: repo, cache: cache, log: log}
}

// ListProducts serves the catalog from cache when it can. On a miss, concurrent
// callers share one database read (singleflight), so an expired key does not
// send a burst of identical queries to Postgres. Stock shown here is advisory:
// reservations always check stock in Postgres under a row lock.
func (s *Service) ListProducts(ctx context.Context) ([]domain.Product, error) {
	if s.cache == nil {
		return s.repo.ListProducts(ctx)
	}
	if products, ok, err := s.cache.GetProducts(ctx); err == nil && ok {
		return products, nil
	} else if err != nil {
		s.log.Error("product cache read failed, using database", err, nil)
	}
	v, err, _ := s.load.Do("products", func() (any, error) {
		products, err := s.repo.ListProducts(ctx)
		if err != nil {
			return nil, err
		}
		if err := s.cache.SetProducts(ctx, products); err != nil {
			s.log.Error("product cache write failed", err, nil)
		}
		return products, nil
	})
	if err != nil {
		return nil, err
	}
	return v.([]domain.Product), nil
}

// invalidate drops the cached catalog after stock changed. A reader that
// loaded the old rows just before the write can still put them back, so the
// short TTL bounds how stale the catalog can get.
func (s *Service) invalidate(ctx context.Context) {
	if s.cache == nil {
		return
	}
	if err := s.cache.Invalidate(ctx); err != nil {
		s.log.Error("product cache invalidation failed", err, nil)
	}
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
	items, total, err := s.repo.ReserveStock(ctx, orderID, lines)
	if err != nil {
		return nil, 0, err
	}
	s.invalidate(ctx)
	return items, total, nil
}

// Release and Confirm are driven by order events, not by RPCs.

func (s *Service) Release(ctx context.Context, orderID string) error {
	if err := s.repo.Release(ctx, orderID); err != nil {
		return err
	}
	s.invalidate(ctx)
	return nil
}

func (s *Service) Confirm(ctx context.Context, orderID string) error {
	return s.repo.Confirm(ctx, orderID)
}
