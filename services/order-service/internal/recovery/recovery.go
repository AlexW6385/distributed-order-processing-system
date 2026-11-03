// Package recovery drives orders that got stuck in a non-terminal state.
//
// A request can die at any point: the process crashes, the client disconnects,
// a dependency times out. Instead of trying to handle every failure inline,
// the saga leaves the order in its current state and this worker moves it
// forward later. Every action it takes is a guarded transition, so running it
// on several replicas at once, or racing it against live requests, is safe.
package recovery

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	actions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "recovery_actions_total",
		Help: "Orders moved forward by the recovery worker, by action.",
	}, []string{"action"})
	outboxBacklog = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_backlog_events",
		Help: "Outbox events not yet published to Kafka.",
	})
	outboxOldest = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "outbox_oldest_pending_seconds",
		Help: "Age of the oldest unpublished outbox event.",
	})
)

type Repository interface {
	Stuck(ctx context.Context, status string, olderThan time.Duration, limit int) ([]domain.Order, error)
	PastDeadline(ctx context.Context, limit int) ([]domain.Order, error)
	Close(ctx context.Context, orderID string, from []string, to, reason string) (*domain.Order, error)
	OutboxBacklog(ctx context.Context) (int, time.Duration, error)
}

type Settler interface {
	SettlePayment(ctx context.Context, order *domain.Order) (*domain.Order, error)
	SettleRefund(ctx context.Context, order *domain.Order) (*domain.Order, error)
}

type Config struct {
	Interval time.Duration
	// PendingTimeout is how long an order may stay pending (reservation in
	// flight) before it is failed.
	PendingTimeout time.Duration
	// PaymentRetryAfter is how long an order sits in payment_pending or
	// refund_pending before the charge or refund is retried.
	PaymentRetryAfter time.Duration
	BatchSize         int
	// Concurrency bounds parallel calls to downstream services. After an
	// outage thousands of orders may need settling; serial retries would take
	// minutes.
	Concurrency int
}

type Worker struct {
	repo    Repository
	settler Settler
	cfg     Config
}

func New(repo Repository, settler Settler, cfg Config) *Worker {
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	if cfg.PendingTimeout <= 0 {
		cfg.PendingTimeout = time.Minute
	}
	if cfg.PaymentRetryAfter <= 0 {
		cfg.PaymentRetryAfter = 10 * time.Second
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 16
	}
	return &Worker{repo: repo, settler: settler, cfg: cfg}
}

func (w *Worker) Run(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.RunOnce(ctx)
		}
	}
}

// RunOnce performs one pass over each kind of stuck order.
func (w *Worker) RunOnce(ctx context.Context) {
	w.failStalePending(ctx)
	w.expireReservations(ctx)
	w.retry(ctx, domain.StatusPaymentPending, "payment", w.settler.SettlePayment)
	w.retry(ctx, domain.StatusRefundPending, "refund", w.settler.SettleRefund)
	if n, oldest, err := w.repo.OutboxBacklog(ctx); err == nil {
		outboxBacklog.Set(float64(n))
		outboxOldest.Set(oldest.Seconds())
	}
}

func (w *Worker) failStalePending(ctx context.Context) {
	w.drain(ctx, "pending", func() ([]domain.Order, error) {
		return w.repo.Stuck(ctx, domain.StatusPending, w.cfg.PendingTimeout, w.cfg.BatchSize)
	}, func(o *domain.Order) bool {
		return w.close(ctx, o.ID, domain.StatusPending, domain.StatusFailed, "stock reservation did not complete")
	})
}

func (w *Worker) expireReservations(ctx context.Context) {
	w.drain(ctx, "stock_reserved", func() ([]domain.Order, error) {
		return w.repo.PastDeadline(ctx, w.cfg.BatchSize)
	}, func(o *domain.Order) bool {
		return w.close(ctx, o.ID, domain.StatusStockReserved, domain.StatusExpired, "payment window closed")
	})
}

// retry settles orders stuck in status by calling settle again. Downstream
// services deduplicate by order ID, so a retry never repeats a charge or a
// refund.
func (w *Worker) retry(ctx context.Context, status, action string, settle func(context.Context, *domain.Order) (*domain.Order, error)) {
	w.drain(ctx, status, func() ([]domain.Order, error) {
		return w.repo.Stuck(ctx, status, w.cfg.PaymentRetryAfter, w.cfg.BatchSize)
	}, func(o *domain.Order) bool {
		settled, err := settle(ctx, o)
		if err != nil || settled.Status == status {
			return false // still unknown or dependency down; next pass
		}
		actions.WithLabelValues("settle_" + action).Inc()
		return true
	})
}

// drain processes batches with bounded concurrency until a batch is short,
// a batch makes no progress (the dependency is still down), or the pass has
// used its time budget.
func (w *Worker) drain(ctx context.Context, status string, next func() ([]domain.Order, error), handle func(*domain.Order) bool) {
	deadline := time.Now().Add(w.cfg.Interval)
	for time.Now().Before(deadline) && ctx.Err() == nil {
		batch, err := next()
		if err != nil {
			slog.ErrorContext(ctx, "list stuck orders", "status", status, "error", err)
			return
		}
		var progressed atomic.Int64
		sem := make(chan struct{}, w.cfg.Concurrency)
		var wg sync.WaitGroup
		for i := range batch {
			wg.Add(1)
			sem <- struct{}{}
			go func(o *domain.Order) {
				defer wg.Done()
				defer func() { <-sem }()
				if handle(o) {
					progressed.Add(1)
				}
			}(&batch[i])
		}
		wg.Wait()
		if len(batch) < w.cfg.BatchSize || progressed.Load() == 0 {
			return
		}
	}
}

func (w *Worker) close(ctx context.Context, orderID, from, to, reason string) bool {
	if _, err := w.repo.Close(ctx, orderID, []string{from}, to, reason); err != nil {
		// ErrConflict just means a live request moved the order first.
		return true
	}
	actions.WithLabelValues(to).Inc()
	return true
}
