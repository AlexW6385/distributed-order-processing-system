package repository

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/testdb"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/migrations"
)

func setup(t *testing.T) (*Repository, func(id string) int32) {
	db := testdb.New(t, migrations.FS)
	repo := New(db)
	stock := func(id string) int32 {
		var n int32
		if err := db.QueryRow(context.Background(), `SELECT stock FROM products WHERE id = $1`, id).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	return repo, stock
}

func setStock(t *testing.T, repo *Repository, id string, n int) {
	if _, err := repo.db.Exec(context.Background(), `UPDATE products SET stock = $2 WHERE id = $1`, id, n); err != nil {
		t.Fatal(err)
	}
}

func line(id string, qty int32) []domain.ReservationLine {
	return []domain.ReservationLine{{ProductID: id, Quantity: qty}}
}

func TestConcurrentReservationsNeverOversell(t *testing.T) {
	repo, stock := setup(t)
	ctx := context.Background()
	setStock(t, repo, "prod-lamp-009", 5)

	var wg sync.WaitGroup
	var mu sync.Mutex
	succeeded := 0
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, _, err := repo.ReserveStock(ctx, fmt.Sprintf("order-%d", i), "c@example.com", line("prod-lamp-009", 1))
			if err == nil {
				mu.Lock()
				succeeded++
				mu.Unlock()
			} else if !errors.Is(err, domain.ErrInsufficientStock) {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	if succeeded != 5 {
		t.Fatalf("succeeded = %d, want 5", succeeded)
	}
	if got := stock("prod-lamp-009"); got != 0 {
		t.Fatalf("stock = %d, want 0", got)
	}
}

func TestOverlappingOrdersDoNotDeadlock(t *testing.T) {
	repo, _ := setup(t)
	ctx := context.Background()
	ab := []domain.ReservationLine{{ProductID: "prod-mug-003", Quantity: 1}, {ProductID: "prod-pen-006", Quantity: 1}}
	ba := []domain.ReservationLine{{ProductID: "prod-pen-006", Quantity: 1}, {ProductID: "prod-mug-003", Quantity: 1}}

	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			lines := ab
			if i%2 == 1 {
				lines = ba
			}
			if _, _, err := repo.ReserveStock(ctx, fmt.Sprintf("order-%d", i), "c@example.com", lines); err != nil {
				t.Errorf("reserve %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()
}

func TestReserveIsIdempotentPerOrder(t *testing.T) {
	repo, stock := setup(t)
	ctx := context.Background()
	before := stock("prod-tea-002")

	first, total1, err := repo.ReserveStock(ctx, "order-1", "c@example.com", line("prod-tea-002", 3))
	if err != nil {
		t.Fatal(err)
	}
	second, total2, err := repo.ReserveStock(ctx, "order-1", "c@example.com", line("prod-tea-002", 3))
	if err != nil {
		t.Fatal(err)
	}
	if total1 != total2 || len(first) != len(second) {
		t.Fatalf("replay returned a different reservation")
	}
	if got := stock("prod-tea-002"); got != before-3 {
		t.Fatalf("stock = %d, want %d (reserved once)", got, before-3)
	}
}

func TestReleaseRestoresStockAndIsIdempotent(t *testing.T) {
	repo, stock := setup(t)
	ctx := context.Background()
	before := stock("prod-tea-002")

	if _, _, err := repo.ReserveStock(ctx, "order-1", "c@example.com", line("prod-tea-002", 4)); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, _, err := repo.Release(ctx, "order-1"); err != nil {
			t.Fatalf("release %d: %v", i, err)
		}
	}
	if got := stock("prod-tea-002"); got != before {
		t.Fatalf("stock = %d, want %d", got, before)
	}
}

func TestReleaseBeforeReserveBlocksLateReservation(t *testing.T) {
	repo, stock := setup(t)
	ctx := context.Background()
	before := stock("prod-tea-002")

	// The order was failed (and its release event consumed) before a delayed
	// ReserveStock request reached product-service.
	if _, _, err := repo.Release(ctx, "order-1"); err != nil {
		t.Fatal(err)
	}
	_, _, err := repo.ReserveStock(ctx, "order-1", "c@example.com", line("prod-tea-002", 2))
	if !errors.Is(err, domain.ErrReservationClosed) {
		t.Fatalf("err = %v, want ErrReservationClosed", err)
	}
	if got := stock("prod-tea-002"); got != before {
		t.Fatalf("stock = %d, want %d (nothing leaked)", got, before)
	}
}

func TestConfirmKeepsStockAndRejectsRelease(t *testing.T) {
	repo, stock := setup(t)
	ctx := context.Background()
	before := stock("prod-tea-002")

	if _, _, err := repo.ReserveStock(ctx, "order-1", "c@example.com", line("prod-tea-002", 1)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Confirm(ctx, "order-1"); err != nil {
		t.Fatal(err)
	}
	if err := repo.Confirm(ctx, "order-1"); err != nil {
		t.Fatalf("second confirm should be a no-op: %v", err)
	}
	if _, _, err := repo.Release(ctx, "order-1"); !errors.Is(err, domain.ErrInconsistent) {
		t.Fatalf("release after confirm: err = %v, want ErrInconsistent", err)
	}
	if got := stock("prod-tea-002"); got != before-1 {
		t.Fatalf("stock = %d, want %d", got, before-1)
	}
}

func TestFailedReservationLeavesNoTrace(t *testing.T) {
	repo, stock := setup(t)
	ctx := context.Background()
	setStock(t, repo, "prod-lamp-009", 1)
	before := stock("prod-mug-003")

	lines := []domain.ReservationLine{{ProductID: "prod-mug-003", Quantity: 1}, {ProductID: "prod-lamp-009", Quantity: 2}}
	if _, _, err := repo.ReserveStock(ctx, "order-1", "c@example.com", lines); !errors.Is(err, domain.ErrInsufficientStock) {
		t.Fatalf("err = %v, want ErrInsufficientStock", err)
	}
	if got := stock("prod-mug-003"); got != before {
		t.Fatalf("partial reservation was not rolled back: stock = %d, want %d", got, before)
	}
}

func TestRestockReturnsRefundedStockOnce(t *testing.T) {
	repo, stock := setup(t)
	ctx := context.Background()
	before := stock("prod-tea-002")
	if _, _, err := repo.ReserveStock(ctx, "order-1", "c@example.com", line("prod-tea-002", 2)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Confirm(ctx, "order-1"); err != nil {
		t.Fatal(err)
	}
	items, customer, err := repo.Restock(ctx, "order-1")
	if err != nil || len(items) != 1 || customer != "c@example.com" {
		t.Fatalf("restock: items=%v customer=%q err=%v", items, customer, err)
	}
	if items, _, err := repo.Restock(ctx, "order-1"); err != nil || len(items) != 0 {
		t.Fatalf("second restock should be a no-op: items=%v err=%v", items, err)
	}
	if got := stock("prod-tea-002"); got != before {
		t.Fatalf("stock = %d, want %d", got, before)
	}
}
