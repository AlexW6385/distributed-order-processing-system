package service_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/testdb"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/migrations"
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
