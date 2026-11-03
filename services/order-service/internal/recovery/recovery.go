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
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
)

type Repository interface {
	Stuck(ctx context.Context, status string, olderThan time.Duration, limit int) ([]domain.Order, error)
	PastDeadline(ctx context.Context, limit int) ([]domain.Order, error)
	Close(ctx context.Context, orderID string, from []string, to, reason string) (*domain.Order, error)
}

type Settler interface {
	SettlePayment(ctx context.Context, order *domain.Order) (*domain.Order, error)
}

type Config struct {
	Interval time.Duration
	// PendingTimeout is how long an order may stay pending (reservation in
	// flight) before it is failed.
	PendingTimeout time.Duration
	// PaymentRetryAfter is how long an order sits in payment_pending before
	// the charge is retried.
	PaymentRetryAfter time.Duration
	BatchSize         int
}

type Worker struct {
	repo    Repository
	settler Settler
	cfg     Config
	log     *logging.Logger
}

func New(repo Repository, settler Settler, cfg Config, log *logging.Logger) *Worker {
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
		cfg.BatchSize = 50
	}
	return &Worker{repo: repo, settler: settler, cfg: cfg, log: log}
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
	w.retryPayments(ctx)
}

func (w *Worker) failStalePending(ctx context.Context) {
	orders, err := w.repo.Stuck(ctx, domain.StatusPending, w.cfg.PendingTimeout, w.cfg.BatchSize)
	if err != nil {
		w.log.Error("list stale pending orders", err, nil)
		return
	}
	for _, o := range orders {
		w.close(ctx, o.ID, domain.StatusPending, domain.StatusFailed, "stock reservation did not complete")
	}
}

func (w *Worker) expireReservations(ctx context.Context) {
	orders, err := w.repo.PastDeadline(ctx, w.cfg.BatchSize)
	if err != nil {
		w.log.Error("list expired reservations", err, nil)
		return
	}
	for _, o := range orders {
		w.close(ctx, o.ID, domain.StatusStockReserved, domain.StatusExpired, "payment window closed")
	}
}

func (w *Worker) retryPayments(ctx context.Context) {
	orders, err := w.repo.Stuck(ctx, domain.StatusPaymentPending, w.cfg.PaymentRetryAfter, w.cfg.BatchSize)
	if err != nil {
		w.log.Error("list pending payments", err, nil)
		return
	}
	for i := range orders {
		settled, err := w.settler.SettlePayment(ctx, &orders[i])
		if err != nil {
			w.log.Error("settle payment", err, map[string]any{"order_id": orders[i].ID})
			continue
		}
		if settled.Status != domain.StatusPaymentPending {
			w.log.Info("payment settled by recovery", map[string]any{"order_id": settled.ID, "status": settled.Status})
		}
	}
}

func (w *Worker) close(ctx context.Context, orderID, from, to, reason string) {
	if _, err := w.repo.Close(ctx, orderID, []string{from}, to, reason); err != nil {
		// ErrConflict just means a live request moved the order first.
		w.log.Info("recovery skipped order", map[string]any{"order_id": orderID, "reason": err.Error()})
		return
	}
	w.log.Info("recovery closed order", map[string]any{"order_id": orderID, "status": to})
}
