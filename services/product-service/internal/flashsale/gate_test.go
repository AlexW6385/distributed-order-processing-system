package flashsale

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

func newGate(t *testing.T) (*Gate, string) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping Redis test")
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	return New(rdb), "p-" + uuid.NewString()
}

func TestConcurrentBuyersNeverExceedStock(t *testing.T) {
	g, product := newGate(t)
	ctx := context.Background()
	if err := g.Start(ctx, product, 100); err != nil {
		t.Fatal(err)
	}
	var admitted, soldOut atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 2000; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ok, err := g.Admit(ctx, product, fmt.Sprintf("o%d", i), fmt.Sprintf("c%d", i), 1)
			switch {
			case ok:
				admitted.Add(1)
			case errors.Is(err, ErrSoldOut):
				soldOut.Add(1)
			default:
				t.Errorf("unexpected: ok=%v err=%v", ok, err)
			}
		}(i)
	}
	wg.Wait()
	if admitted.Load() != 100 || soldOut.Load() != 1900 {
		t.Fatalf("admitted=%d soldOut=%d, want 100/1900", admitted.Load(), soldOut.Load())
	}
}

func TestOnePerCustomerAndIdempotentRetries(t *testing.T) {
	g, product := newGate(t)
	ctx := context.Background()
	_ = g.Start(ctx, product, 10)

	if ok, err := g.Admit(ctx, product, "o1", "alice", 1); !ok || err != nil {
		t.Fatalf("first order: %v %v", ok, err)
	}
	if ok, err := g.Admit(ctx, product, "o1", "alice", 1); !ok || err != nil {
		t.Fatalf("retry of the same order must be admitted again: %v %v", ok, err)
	}
	if _, err := g.Admit(ctx, product, "o2", "alice", 1); !errors.Is(err, ErrLimit) {
		t.Fatalf("second order by the same customer: err = %v, want ErrLimit", err)
	}
	if _, err := g.Admit(ctx, product, "o3", "bob", 2); !errors.Is(err, ErrLimit) {
		t.Fatalf("two units: err = %v, want ErrLimit", err)
	}
}

func TestUndoReturnsUnitsOnce(t *testing.T) {
	g, product := newGate(t)
	ctx := context.Background()
	_ = g.Start(ctx, product, 1)
	_, _ = g.Admit(ctx, product, "o1", "alice", 1)
	if _, err := g.Admit(ctx, product, "o2", "bob", 1); !errors.Is(err, ErrSoldOut) {
		t.Fatalf("want sold out, got %v", err)
	}
	for i := 0; i < 3; i++ { // redelivered release events
		if err := g.Undo(ctx, product, "o1", "alice", 1); err != nil {
			t.Fatal(err)
		}
	}
	if ok, err := g.Admit(ctx, product, "o2", "bob", 1); !ok || err != nil {
		t.Fatalf("returned unit should be available: %v %v", ok, err)
	}
	if _, err := g.Admit(ctx, product, "o3", "carol", 1); !errors.Is(err, ErrSoldOut) {
		t.Fatalf("only one unit was returned; want sold out, got %v", err)
	}
}

func TestProductsWithoutSaleBypassGate(t *testing.T) {
	g, product := newGate(t)
	if ok, err := g.Admit(context.Background(), product, "o1", "alice", 5); ok || err != nil {
		t.Fatalf("non-sale product: ok=%v err=%v, want false/nil", ok, err)
	}
}
