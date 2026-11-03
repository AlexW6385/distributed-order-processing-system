package service

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/flashsale"
)

type fakeRepo struct {
	lists     atomic.Int32
	reserves  atomic.Int32
	delay     time.Duration
	reserveFn func() error
}

func (r *fakeRepo) ListProducts(ctx context.Context) ([]domain.Product, error) {
	r.lists.Add(1)
	time.Sleep(r.delay)
	return []domain.Product{{ID: "p1", Stock: 3}}, nil
}
func (r *fakeRepo) ReserveStock(ctx context.Context, orderID, customer string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error) {
	r.reserves.Add(1)
	if r.reserveFn != nil {
		return nil, 0, r.reserveFn()
	}
	return []domain.ReservedItem{{ProductID: lines[0].ProductID, Quantity: lines[0].Quantity}}, 100, nil
}
func (r *fakeRepo) Release(ctx context.Context, orderID string) ([]domain.ReservedItem, string, error) {
	return []domain.ReservedItem{{ProductID: "p1", Quantity: 1}}, "c", nil
}
func (r *fakeRepo) Restock(ctx context.Context, orderID string) ([]domain.ReservedItem, string, error) {
	return []domain.ReservedItem{{ProductID: "p1", Quantity: 1}}, "c", nil
}
func (r *fakeRepo) Confirm(ctx context.Context, orderID string) error                 { return nil }
func (r *fakeRepo) SetStock(ctx context.Context, productID string, stock int32) error { return nil }

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

// fakeGate mimics the Redis gate for one product.
type fakeGate struct {
	mu       sync.Mutex
	tokens   int32
	admitted map[string]bool
	down     bool
	lostAck  bool // admit succeeds in Redis but the caller sees an error
}

func (g *fakeGate) Start(ctx context.Context, productID string, stock int) error {
	g.tokens, g.admitted = int32(stock), map[string]bool{}
	return nil
}
func (g *fakeGate) Admit(ctx context.Context, productID, orderID, customer string, qty int32) (bool, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.down {
		return false, errors.New("redis down")
	}
	if g.admitted == nil || productID != "hot" {
		return false, nil
	}
	if g.tokens < qty {
		return false, flashsale.ErrSoldOut
	}
	g.tokens -= qty
	g.admitted[orderID] = true
	if g.lostAck {
		return false, errors.New("context canceled")
	}
	return true, nil
}
func (g *fakeGate) Undo(ctx context.Context, productID, orderID, customer string, qty int32) error {
	if ctx.Err() != nil {
		return ctx.Err() // like Redis, a cancelled context fails the call
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.admitted[orderID] {
		delete(g.admitted, orderID)
		g.tokens += qty
	}
	return nil
}

func TestCacheHitSkipsDatabase(t *testing.T) {
	repo := &fakeRepo{}
	svc := New(repo, &fakeCache{}, nil)
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
	repo := &fakeRepo{delay: 50 * time.Millisecond}
	svc := New(repo, &fakeCache{}, nil)
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

func TestStockChangesInvalidateCache(t *testing.T) {
	cache := &fakeCache{products: []domain.Product{{ID: "p1"}}}
	svc := New(&fakeRepo{}, cache, nil)
	_, _, _ = svc.ReserveStock(context.Background(), "o1", "c", []domain.ReservationLine{{ProductID: "p1", Quantity: 1}})
	_ = svc.Release(context.Background(), "o1")
	_ = svc.Restock(context.Background(), "o2")
	if cache.invalidated != 3 {
		t.Fatalf("invalidated %d times, want 3", cache.invalidated)
	}
}

func TestCacheOutageFallsBackToDatabase(t *testing.T) {
	svc := New(&fakeRepo{}, &fakeCache{down: true}, nil)
	if products, err := svc.ListProducts(context.Background()); err != nil || len(products) != 1 {
		t.Fatalf("products=%v err=%v", products, err)
	}
}

func TestFlashSaleRejectsWithoutTouchingDatabase(t *testing.T) {
	repo, gate := &fakeRepo{}, &fakeGate{}
	svc := New(repo, nil, gate)
	_ = svc.StartFlashSale(context.Background(), "hot", 2)
	line := []domain.ReservationLine{{ProductID: "hot", Quantity: 1}}
	for i, want := range []error{nil, nil, domain.ErrInsufficientStock, domain.ErrInsufficientStock} {
		_, _, err := svc.ReserveStock(context.Background(), string(rune('a'+i)), "c", line)
		if !errors.Is(err, want) {
			t.Fatalf("order %d: err = %v, want %v", i, err, want)
		}
	}
	if n := repo.reserves.Load(); n != 2 {
		t.Fatalf("database reservations = %d, want 2 (rejections stay in Redis)", n)
	}
}

func TestFlashSaleReturnsUnitsWhenDatabaseRejects(t *testing.T) {
	repo, gate := &fakeRepo{reserveFn: func() error { return domain.ErrReservationClosed }}, &fakeGate{}
	svc := New(repo, nil, gate)
	_ = svc.StartFlashSale(context.Background(), "hot", 1)
	_, _, err := svc.ReserveStock(context.Background(), "o1", "c", []domain.ReservationLine{{ProductID: "hot", Quantity: 1}})
	if !errors.Is(err, domain.ErrReservationClosed) || gate.tokens != 1 {
		t.Fatalf("err=%v tokens=%d, want closed and the unit returned", err, gate.tokens)
	}
}

func TestFlashSaleGateOutageFallsBackToDatabase(t *testing.T) {
	repo, gate := &fakeRepo{}, &fakeGate{down: true}
	svc := New(repo, nil, gate)
	if _, _, err := svc.ReserveStock(context.Background(), "o1", "c", []domain.ReservationLine{{ProductID: "hot", Quantity: 1}}); err != nil {
		t.Fatal(err)
	}
}

// Regression: when the request's deadline passed mid-reservation, the units
// were returned with the cancelled request context, the call failed, and the
// sale undersold.
func TestFlashSaleReturnsUnitsWhenRequestIsCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	repo, gate := &fakeRepo{reserveFn: func() error { cancel(); return context.Canceled }}, &fakeGate{}
	svc := New(repo, nil, gate)
	_ = svc.StartFlashSale(context.Background(), "hot", 1)
	if _, _, err := svc.ReserveStock(ctx, "o1", "c", []domain.ReservationLine{{ProductID: "hot", Quantity: 1}}); err == nil {
		t.Fatal("expected the cancelled reservation to fail")
	}
	if gate.tokens != 1 {
		t.Fatalf("tokens = %d, want 1 (unit returned)", gate.tokens)
	}
}

// Regression: the Lua script ran but the caller got an error, then the
// reservation failed; the unit was never given back.
func TestFlashSaleReturnsUnitsWhenAdmissionOutcomeIsUnknown(t *testing.T) {
	repo, gate := &fakeRepo{reserveFn: func() error { return context.DeadlineExceeded }}, &fakeGate{}
	svc := New(repo, nil, gate)
	_ = svc.StartFlashSale(context.Background(), "hot", 1)
	gate.lostAck = true
	if _, _, err := svc.ReserveStock(context.Background(), "o1", "c", []domain.ReservationLine{{ProductID: "hot", Quantity: 1}}); err == nil {
		t.Fatal("expected the reservation to fail")
	}
	if gate.tokens != 1 {
		t.Fatalf("tokens = %d, want 1 (unit returned)", gate.tokens)
	}
}
