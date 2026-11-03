package service_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/testdb"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/recovery"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/migrations"
	"github.com/jackc/pgx/v5/pgxpool"
)

// --- fakes for the two downstream services ---------------------------------

type fakeProduct struct {
	calls atomic.Int32
	err   error
}

func (f *fakeProduct) ReserveStock(ctx context.Context, orderID string, items []domain.ItemInput) ([]domain.Item, int64, error) {
	f.calls.Add(1)
	if f.err != nil {
		return nil, 0, f.err
	}
	var out []domain.Item
	var total int64
	for _, in := range items {
		out = append(out, domain.Item{ProductID: in.ProductID, ProductName: in.ProductID, Quantity: in.Quantity, UnitPriceCents: 1000, SubtotalCents: 1000 * int64(in.Quantity)})
		total += 1000 * int64(in.Quantity)
	}
	return out, total, nil
}

// fakePayment behaves like payment-service: one result per order, recorded on
// the first call, so retries see the same answer.
type fakePayment struct {
	mu        sync.Mutex
	charged   map[string]domain.PaymentResult
	calls     atomic.Int32
	decline   bool
	lose      atomic.Bool // record the charge but report an unknown outcome
	slowStart chan struct{}
}

func newFakePayment() *fakePayment { return &fakePayment{charged: map[string]domain.PaymentResult{}} }

func (f *fakePayment) Charge(ctx context.Context, orderID string, amount int64, key string) (domain.PaymentResult, error) {
	f.calls.Add(1)
	if f.slowStart != nil {
		<-f.slowStart
	}
	f.mu.Lock()
	result, ok := f.charged[orderID]
	if !ok {
		result = domain.PaymentResult{Succeeded: !f.decline, DeclineReason: map[bool]string{true: "limit"}[f.decline]}
		f.charged[orderID] = result
	}
	f.mu.Unlock()
	if f.lose.Load() {
		return domain.PaymentResult{}, fmt.Errorf("%w: deadline exceeded", domain.ErrUnavailable)
	}
	return result, nil
}

// --- harness ----------------------------------------------------------------

type harness struct {
	db      *pgxpool.Pool
	repo    *repository.Repository
	svc     *service.Service
	product *fakeProduct
	payment *fakePayment
}

func newHarness(t *testing.T, window time.Duration) *harness {
	db := testdb.New(t, migrations.FS)
	h := &harness{db: db, repo: repository.New(db), product: &fakeProduct{}, payment: newFakePayment()}
	h.svc = service.New(h.repo, h.product, h.payment, service.Config{PaymentWindow: window}, logging.New("test"))
	return h
}

func (h *harness) create(t *testing.T) *domain.Order {
	t.Helper()
	order, err := h.svc.CreateOrder(context.Background(), service.CreateInput{
		CustomerEmail: "alex@example.com",
		Items:         []domain.ItemInput{{ProductID: "prod-coffee-001", Quantity: 2}},
	})
	if err != nil {
		t.Fatalf("create order: %v", err)
	}
	return order
}

func (h *harness) events(t *testing.T, orderID string) []string {
	t.Helper()
	rows, err := h.db.Query(context.Background(), `SELECT event_type FROM outbox_events WHERE aggregate_id = $1 ORDER BY created_at`, orderID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var e string
		_ = rows.Scan(&e)
		out = append(out, e)
	}
	return out
}

func (h *harness) recovery(pending, retry time.Duration) *recovery.Worker {
	return recovery.New(h.repo, h.svc, recovery.Config{PendingTimeout: pending, PaymentRetryAfter: retry}, logging.New("test"))
}

func assertEvents(t *testing.T, got []string, want ...string) {
	t.Helper()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("outbox events = %v, want %v", got, want)
	}
}

// --- tests ------------------------------------------------------------------

func TestHappyPath(t *testing.T) {
	h := newHarness(t, time.Minute)
	order := h.create(t)
	if order.Status != domain.StatusStockReserved || order.TotalCents != 2000 || order.ExpiresAt == nil {
		t.Fatalf("unexpected order after create: %+v", order)
	}
	paid, err := h.svc.PayOrder(context.Background(), order.ID, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if paid.Status != domain.StatusPaid {
		t.Fatalf("status = %s, want paid", paid.Status)
	}
	assertEvents(t, h.events(t, order.ID), events.OrderPaid)
}

// Regression test: two concurrent pay requests with different idempotency keys
// used to both reach payment-service, and the loser marked the paid order as
// failed and released its stock.
func TestConcurrentPayChargesOnceAndStaysPaid(t *testing.T) {
	h := newHarness(t, time.Minute)
	order := h.create(t)
	h.payment.slowStart = make(chan struct{})

	var wg sync.WaitGroup
	results := make(chan *domain.Order, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			o, err := h.svc.PayOrder(context.Background(), order.ID, fmt.Sprintf("key-%d", i))
			if err != nil {
				t.Errorf("pay %d: %v", i, err)
				return
			}
			results <- o
		}(i)
	}
	time.Sleep(200 * time.Millisecond) // let every request reach the claim
	close(h.payment.slowStart)
	wg.Wait()
	close(results)

	if n := h.payment.calls.Load(); n != 1 {
		t.Fatalf("payment-service called %d times, want 1", n)
	}
	for o := range results {
		if o.Status != domain.StatusPaid && o.Status != domain.StatusPaymentPending {
			t.Fatalf("a concurrent caller saw status %s", o.Status)
		}
	}
	final, _ := h.repo.Get(context.Background(), order.ID)
	if final.Status != domain.StatusPaid {
		t.Fatalf("final status = %s, want paid", final.Status)
	}
	assertEvents(t, h.events(t, order.ID), events.OrderPaid)
}

func TestUnknownPaymentOutcomeIsResolvedByRecovery(t *testing.T) {
	h := newHarness(t, time.Minute)
	order := h.create(t)

	h.payment.lose.Store(true)
	pending, err := h.svc.PayOrder(context.Background(), order.ID, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if pending.Status != domain.StatusPaymentPending {
		t.Fatalf("status = %s, want payment_pending (outcome unknown)", pending.Status)
	}
	assertEvents(t, h.events(t, order.ID)) // nothing decided yet

	// A client retry must not start a second charge attempt.
	again, err := h.svc.PayOrder(context.Background(), order.ID, "key-2")
	if err != nil || again.Status != domain.StatusPaymentPending {
		t.Fatalf("retry: status=%v err=%v", again, err)
	}

	h.payment.lose.Store(false)
	h.recovery(time.Minute, time.Nanosecond).RunOnce(context.Background())

	final, _ := h.repo.Get(context.Background(), order.ID)
	if final.Status != domain.StatusPaid {
		t.Fatalf("status after recovery = %s, want paid", final.Status)
	}
	if len(h.payment.charged) != 1 {
		t.Fatalf("charged %d orders, want 1", len(h.payment.charged))
	}
	assertEvents(t, h.events(t, order.ID), events.OrderPaid)
}

func TestDeclinedPaymentFailsOrder(t *testing.T) {
	h := newHarness(t, time.Minute)
	h.payment.decline = true
	order := h.create(t)

	failed, err := h.svc.PayOrder(context.Background(), order.ID, "key-1")
	if err != nil {
		t.Fatal(err)
	}
	if failed.Status != domain.StatusFailed {
		t.Fatalf("status = %s, want failed", failed.Status)
	}
	assertEvents(t, h.events(t, order.ID), events.OrderFailed)

	if _, err := h.svc.PayOrder(context.Background(), order.ID, "key-2"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("paying a failed order: err = %v, want ErrConflict", err)
	}
}

func TestReservationFailureFailsOrderAndEmitsRelease(t *testing.T) {
	for name, cause := range map[string]error{
		"rejected": fmt.Errorf("%w: insufficient stock", domain.ErrRejected),
		// The reservation may have happened; order.failed makes product-service
		// release it (or tombstone the order if it has not arrived yet).
		"timeout": fmt.Errorf("%w: deadline exceeded", domain.ErrUnavailable),
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, time.Minute)
			h.product.err = cause
			_, err := h.svc.CreateOrder(context.Background(), service.CreateInput{
				CustomerEmail: "alex@example.com",
				Items:         []domain.ItemInput{{ProductID: "prod-coffee-001", Quantity: 1}},
			})
			if !errors.Is(err, cause) {
				t.Fatalf("err = %v, want %v", err, cause)
			}
			var id, status string
			if err := h.db.QueryRow(context.Background(), `SELECT id, status FROM orders`).Scan(&id, &status); err != nil {
				t.Fatal(err)
			}
			if status != domain.StatusFailed {
				t.Fatalf("status = %s, want failed", status)
			}
			assertEvents(t, h.events(t, id), events.OrderFailed)
		})
	}
}

func TestCreateOrderIsIdempotent(t *testing.T) {
	h := newHarness(t, time.Minute)
	in := service.CreateInput{
		CustomerEmail:  "alex@example.com",
		Items:          []domain.ItemInput{{ProductID: "prod-coffee-001", Quantity: 1}},
		IdempotencyKey: "create-1",
	}
	first, err := h.svc.CreateOrder(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	second, err := h.svc.CreateOrder(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID {
		t.Fatalf("replay created a second order")
	}
	if n := h.product.calls.Load(); n != 1 {
		t.Fatalf("stock reserved %d times, want 1", n)
	}
}

func TestUnpaidOrderExpires(t *testing.T) {
	h := newHarness(t, 50*time.Millisecond)
	order := h.create(t)
	time.Sleep(100 * time.Millisecond)

	// Past the deadline the order can no longer be paid, even before the
	// recovery worker gets to it.
	if _, err := h.svc.PayOrder(context.Background(), order.ID, "key-1"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("pay after deadline: err = %v, want ErrConflict", err)
	}
	h.recovery(time.Minute, time.Minute).RunOnce(context.Background())

	final, _ := h.repo.Get(context.Background(), order.ID)
	if final.Status != domain.StatusExpired {
		t.Fatalf("status = %s, want expired", final.Status)
	}
	if h.payment.calls.Load() != 0 {
		t.Fatal("expired order was charged")
	}
	assertEvents(t, h.events(t, order.ID), events.OrderExpired)
}

func TestStuckPendingOrderIsFailed(t *testing.T) {
	h := newHarness(t, time.Minute)
	// Simulate a crash right after the order row was written.
	order, _, err := h.repo.CreatePending(context.Background(), "alex@example.com", "")
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	h.recovery(10*time.Millisecond, time.Minute).RunOnce(context.Background())

	final, _ := h.repo.Get(context.Background(), order.ID)
	if final.Status != domain.StatusFailed {
		t.Fatalf("status = %s, want failed", final.Status)
	}
	assertEvents(t, h.events(t, order.ID), events.OrderFailed)
}

func TestCancel(t *testing.T) {
	h := newHarness(t, time.Minute)
	order := h.create(t)
	cancelled, err := h.svc.CancelOrder(context.Background(), order.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != domain.StatusCancelled {
		t.Fatalf("status = %s, want cancelled", cancelled.Status)
	}
	if _, err := h.svc.CancelOrder(context.Background(), order.ID); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("second cancel: err = %v, want ErrConflict", err)
	}
	if _, err := h.svc.PayOrder(context.Background(), order.ID, "key-1"); !errors.Is(err, domain.ErrConflict) {
		t.Fatalf("pay after cancel: err = %v, want ErrConflict", err)
	}
	assertEvents(t, h.events(t, order.ID), events.OrderCancelled)
}

func TestCancelRacesPayment(t *testing.T) {
	h := newHarness(t, time.Minute)
	for i := 0; i < 20; i++ {
		order := h.create(t)
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = h.svc.PayOrder(context.Background(), order.ID, "k") }()
		go func() { defer wg.Done(); _, _ = h.svc.CancelOrder(context.Background(), order.ID) }()
		wg.Wait()

		final, _ := h.repo.Get(context.Background(), order.ID)
		evs := h.events(t, order.ID)
		switch final.Status {
		case domain.StatusPaid:
			assertEvents(t, evs, events.OrderPaid)
		case domain.StatusCancelled:
			assertEvents(t, evs, events.OrderCancelled)
		default:
			t.Fatalf("status = %s, want paid or cancelled", final.Status)
		}
	}
}

func TestValidation(t *testing.T) {
	h := newHarness(t, time.Minute)
	for _, in := range []service.CreateInput{
		{CustomerEmail: "", Items: []domain.ItemInput{{ProductID: "p", Quantity: 1}}},
		{CustomerEmail: "not-an-email", Items: []domain.ItemInput{{ProductID: "p", Quantity: 1}}},
		{CustomerEmail: "a@b.c"},
		{CustomerEmail: "a@b.c", Items: []domain.ItemInput{{ProductID: "p", Quantity: 0}}},
	} {
		if _, err := h.svc.CreateOrder(context.Background(), in); !errors.Is(err, domain.ErrInvalid) {
			t.Errorf("input %+v: err = %v, want ErrInvalid", in, err)
		}
	}
	if _, err := h.svc.PayOrder(context.Background(), "x", ""); !errors.Is(err, domain.ErrInvalid) {
		t.Errorf("missing key: err = %v, want ErrInvalid", err)
	}
	if _, err := h.svc.GetOrder(context.Background(), "does-not-exist"); !errors.Is(err, domain.ErrNotFound) {
		t.Errorf("unknown order: err = %v, want ErrNotFound", err)
	}
}
