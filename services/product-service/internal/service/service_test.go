package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
)

type fakeRepo struct {
	lists atomic.Int32
	delay time.Duration
}

func (r *fakeRepo) ListProducts(ctx context.Context) ([]domain.Product, error) {
	r.lists.Add(1)
	time.Sleep(r.delay)
	return []domain.Product{{ID: "p1", Stock: 3}}, nil
}
func (r *fakeRepo) ReserveStock(ctx context.Context, orderID string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error) {
	return nil, 0, nil
}
func (r *fakeRepo) Release(ctx context.Context, orderID string) error { return nil }
func (r *fakeRepo) Confirm(ctx context.Context, orderID string) error { return nil }

type fakeCache struct {
	mu          sync.Mutex
	products    []domain.Product
	invalidated int
	down        bool
}

func (c *fakeCache) GetProducts(ctx context.Context) ([]domain.Product, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down {
		return nil, false, errors.New("redis down")
	}
	return c.products, c.products != nil, nil
}
func (c *fakeCache) SetProducts(ctx context.Context, p []domain.Product) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.down {
		return errors.New("redis down")
	}
	c.products = p
	return nil
}
func (c *fakeCache) Invalidate(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.products = nil
	c.invalidated++
	return nil
}

func TestCacheHitSkipsDatabase(t *testing.T) {
	repo, cache := &fakeRepo{}, &fakeCache{}
	svc := New(repo, cache, logging.New("test"))
	for i := 0; i < 5; i++ {
		if _, err := svc.ListProducts(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if n := repo.lists.Load(); n != 1 {
		t.Fatalf("database read %d times, want 1", n)
	}
}

func TestConcurrentMissesShareOneDatabaseRead(t *testing.T) {
	repo, cache := &fakeRepo{delay: 50 * time.Millisecond}, &fakeCache{}
	svc := New(repo, cache, logging.New("test"))
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = svc.ListProducts(context.Background()) }()
	}
	wg.Wait()
	if n := repo.lists.Load(); n != 1 {
		t.Fatalf("database read %d times, want 1 (singleflight)", n)
	}
}

func TestStockChangesInvalidate(t *testing.T) {
	cache := &fakeCache{products: []domain.Product{{ID: "p1"}}}
	svc := New(&fakeRepo{}, cache, logging.New("test"))
	_, _, _ = svc.ReserveStock(context.Background(), "o1", []domain.ReservationLine{{ProductID: "p1", Quantity: 1}})
	_ = svc.Release(context.Background(), "o1")
	if cache.invalidated != 2 {
		t.Fatalf("invalidated %d times, want 2", cache.invalidated)
	}
}

func TestCacheOutageFallsBackToDatabase(t *testing.T) {
	repo := &fakeRepo{}
	svc := New(repo, &fakeCache{down: true}, logging.New("test"))
	products, err := svc.ListProducts(context.Background())
	if err != nil || len(products) != 1 {
		t.Fatalf("products=%v err=%v", products, err)
	}
}
