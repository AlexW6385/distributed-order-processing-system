package service_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/testdb"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

func newService(t *testing.T, cfg service.Config) (*service.Service, func() int) {
	db := testdb.New(t, migrations.FS)
	count := func() int {
		var n int
		_ = db.QueryRow(context.Background(), `SELECT count(*) FROM payments`).Scan(&n)
		return n
	}
	return service.New(repository.New(db), cfg), count
}

func TestConcurrentChargesForOneOrderChargeOnce(t *testing.T) {
	svc, count := newService(t, service.Config{})
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := svc.Charge(context.Background(), "order-1", 500, "key")
			if err != nil {
				t.Error(err)
				return
			}
			ids <- p.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("concurrent charges returned different payments")
		}
	}
	if n := count(); n != 1 {
		t.Fatalf("%d payments stored, want 1", n)
	}
}

func TestDeclineIsRecordedAndReplayed(t *testing.T) {
	svc, _ := newService(t, service.Config{DeclineAboveCents: 1000})
	for i := 0; i < 2; i++ {
		p, err := svc.Charge(context.Background(), "order-1", 5000, "key")
		if err != nil {
			t.Fatal(err)
		}
		if p.Status != domain.StatusDeclined {
			t.Fatalf("status = %s, want declined", p.Status)
		}
	}
}

func TestReplayWithDifferentAmountIsRejected(t *testing.T) {
	svc, _ := newService(t, service.Config{})
	if _, err := svc.Charge(context.Background(), "order-1", 500, "key"); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.Charge(context.Background(), "order-1", 900, "key"); !errors.Is(err, domain.ErrMismatch) {
		t.Fatalf("err = %v, want ErrMismatch", err)
	}
}

func newLedgerService(t *testing.T) (*service.Service, *pgxpool.Pool) {
	db := testdb.New(t, migrations.FS)
	return service.New(repository.New(db), service.Config{DeclineAboveCents: 10_000}), db
}

func TestLedgerBalancesAfterChargesAndRefunds(t *testing.T) {
	svc, _ := newLedgerService(t)
	ctx := context.Background()
	for i, amount := range []int64{500, 700, 900} {
		if _, err := svc.Charge(ctx, fmt.Sprintf("order-%d", i), amount, "k"); err != nil {
			t.Fatal(err)
		}
	}
	_, _ = svc.Charge(ctx, "declined", 50_000, "k") // declines post nothing
	if _, err := svc.Refund(ctx, "order-1", 700); err != nil {
		t.Fatal(err)
	}
	l, err := svc.LedgerSummary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if l.TotalCents != 0 || l.ChargedCents != 2100 || l.RefundedCents != 700 || l.CashCents != 1400 || l.RevenueCents != -1400 {
		t.Fatalf("ledger = %+v", l)
	}
}

func TestConcurrentRefundsRefundOnce(t *testing.T) {
	svc, db := newLedgerService(t)
	ctx := context.Background()
	if _, err := svc.Charge(ctx, "order-1", 500, "k"); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := svc.Refund(ctx, "order-1", 500); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	var refunds, entries int
	_ = db.QueryRow(ctx, `SELECT count(*) FROM refunds`).Scan(&refunds)
	_ = db.QueryRow(ctx, `SELECT count(*) FROM journal_entries WHERE kind = 'refund'`).Scan(&entries)
	if refunds != 1 || entries != 1 {
		t.Fatalf("refunds=%d refund entries=%d, want 1/1", refunds, entries)
	}
}

func TestRefundRequiresMatchingSuccessfulCharge(t *testing.T) {
	svc, _ := newLedgerService(t)
	ctx := context.Background()
	if _, err := svc.Refund(ctx, "never-charged", 100); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
	_, _ = svc.Charge(ctx, "declined", 50_000, "k")
	if _, err := svc.Refund(ctx, "declined", 50_000); !errors.Is(err, domain.ErrMismatch) {
		t.Fatalf("refund of a decline: err = %v, want ErrMismatch", err)
	}
	_, _ = svc.Charge(ctx, "order-1", 500, "k")
	if _, err := svc.Refund(ctx, "order-1", 400); !errors.Is(err, domain.ErrMismatch) {
		t.Fatalf("partial refund: err = %v, want ErrMismatch", err)
	}
}

func TestDatabaseRejectsUnbalancedEntries(t *testing.T) {
	_, db := newLedgerService(t)
	ctx := context.Background()
	tx, err := db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = tx.Exec(ctx, `INSERT INTO journal_entries (id, kind, order_id) VALUES ('e1', 'charge', 'o1')`)
	_, _ = tx.Exec(ctx, `INSERT INTO journal_lines (entry_id, account, amount_cents) VALUES ('e1', 'cash', 100)`)
	if err := tx.Commit(ctx); err == nil || !strings.Contains(err.Error(), "not balanced") {
		t.Fatalf("commit of a one-sided entry: err = %v, want 'not balanced'", err)
	}
}
