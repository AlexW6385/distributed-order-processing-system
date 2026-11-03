package repository

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/obs"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var transitions = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "order_transitions_total",
	Help: "Committed order state transitions, by target status.",
}, []string{"status"})

type Repository struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

const orderColumns = `id, customer_email, status, total_cents, failure_reason, payment_key, expires_at, created_at, updated_at`

// CreatePending inserts a new pending order. When createKey is not empty and
// an order with that key already exists, the existing order is returned and
// created is false.
func (r *Repository) CreatePending(ctx context.Context, email, createKey string) (order *domain.Order, created bool, err error) {
	var key any
	if createKey != "" {
		key = createKey
	}
	row := r.db.QueryRow(ctx, `
		INSERT INTO orders (id, customer_email, status, create_key)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (create_key) DO NOTHING
		RETURNING `+orderColumns, uuid.NewString(), email, domain.StatusPending, key)
	order, err = scanOrder(row)
	if errors.Is(err, domain.ErrNotFound) && createKey != "" {
		existing, err := r.getBy(ctx, r.db, `create_key = $1`, createKey)
		return existing, false, err
	}
	if err != nil {
		return nil, false, err
	}
	return order, true, nil
}

// MarkReserved moves a pending order to stock_reserved and stores its items.
func (r *Repository) MarkReserved(ctx context.Context, orderID string, items []domain.Item, totalCents int64, expiresAt time.Time) (*domain.Order, error) {
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		order, err := transition(ctx, tx, orderID, []string{domain.StatusPending}, domain.StatusStockReserved,
			`total_cents = $4, expires_at = $5`, totalCents, expiresAt)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if _, err := tx.Exec(ctx, `
				INSERT INTO order_items (order_id, product_id, product_name, quantity, unit_price_cents, subtotal_cents)
				VALUES ($1, $2, $3, $4, $5, $6)`,
				orderID, item.ProductID, item.ProductName, item.Quantity, item.UnitPriceCents, item.SubtotalCents); err != nil {
				return nil, err
			}
		}
		order.Items = items
		return order, nil
	})
}

// StartPayment claims a reserved, unexpired order for payment. Exactly one of
// any number of concurrent callers succeeds; the rest get ErrConflict.
func (r *Repository) StartPayment(ctx context.Context, orderID, paymentKey string) (*domain.Order, error) {
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		return transition(ctx, tx, orderID, []string{domain.StatusStockReserved}, domain.StatusPaymentPending,
			`payment_key = $4`, paymentKey, extraWhere(`expires_at > now()`))
	})
}

// AbortPayment returns an order to stock_reserved when its only charge
// attempt was never sent, so the customer can simply try again.
func (r *Repository) AbortPayment(ctx context.Context, orderID string) (*domain.Order, error) {
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		return transition(ctx, tx, orderID, []string{domain.StatusPaymentPending}, domain.StatusStockReserved, "")
	})
}

// MarkPaid records a successful payment and emits order.paid in the same
// transaction (transactional outbox).
func (r *Repository) MarkPaid(ctx context.Context, orderID string) (*domain.Order, error) {
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		order, err := transition(ctx, tx, orderID, []string{domain.StatusPaymentPending}, domain.StatusPaid, "")
		if err != nil {
			return nil, err
		}
		return order, insertEvent(ctx, tx, order, events.OrderPaid, "")
	})
}

// Close moves an order from one of the given states to a terminal failure
// state (failed, expired or cancelled) and emits the matching event, which
// tells product-service to release the order's stock.
func (r *Repository) Close(ctx context.Context, orderID string, from []string, to, reason string) (*domain.Order, error) {
	eventType, ok := closingEvents[to]
	if !ok {
		return nil, fmt.Errorf("%s is not a closing status", to)
	}
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		order, err := transition(ctx, tx, orderID, from, to, `failure_reason = $4`, reason)
		if err != nil {
			return nil, err
		}
		return order, insertEvent(ctx, tx, order, eventType, reason)
	})
}

// StartRefund claims a paid order for refund; concurrent refunds of the same
// order cannot both win.
func (r *Repository) StartRefund(ctx context.Context, orderID string) (*domain.Order, error) {
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		return transition(ctx, tx, orderID, []string{domain.StatusPaid}, domain.StatusRefundPending, "")
	})
}

// MarkRefunded records a completed refund and emits order.refunded, which
// tells product-service to return the stock.
func (r *Repository) MarkRefunded(ctx context.Context, orderID string) (*domain.Order, error) {
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		order, err := transition(ctx, tx, orderID, []string{domain.StatusRefundPending}, domain.StatusRefunded, "")
		if err != nil {
			return nil, err
		}
		return order, insertEvent(ctx, tx, order, events.OrderRefunded, "")
	})
}

// AbortRefund returns an order to paid when payment-service definitively
// refused the refund.
func (r *Repository) AbortRefund(ctx context.Context, orderID, reason string) (*domain.Order, error) {
	return r.inTx(ctx, func(tx pgx.Tx) (*domain.Order, error) {
		return transition(ctx, tx, orderID, []string{domain.StatusRefundPending}, domain.StatusPaid, `failure_reason = $4`, reason)
	})
}

var closingEvents = map[string]string{
	domain.StatusFailed:    events.OrderFailed,
	domain.StatusExpired:   events.OrderExpired,
	domain.StatusCancelled: events.OrderCancelled,
}

func (r *Repository) Get(ctx context.Context, orderID string) (*domain.Order, error) {
	return r.getBy(ctx, r.db, `id = $1`, orderID)
}

// Stuck returns up to limit orders that have been in status for longer than
// olderThan.
func (r *Repository) Stuck(ctx context.Context, status string, olderThan time.Duration, limit int) ([]domain.Order, error) {
	return r.list(ctx, `
		SELECT `+orderColumns+` FROM orders
		WHERE status = $1 AND updated_at < now() - make_interval(secs => $2)
		ORDER BY updated_at
		LIMIT $3`, status, olderThan.Seconds(), limit)
}

// PastDeadline returns reserved orders whose payment deadline has passed.
func (r *Repository) PastDeadline(ctx context.Context, limit int) ([]domain.Order, error) {
	return r.list(ctx, `
		SELECT `+orderColumns+` FROM orders
		WHERE status = $1 AND expires_at <= now()
		ORDER BY expires_at
		LIMIT $2`, domain.StatusStockReserved, limit)
}

// ListOrders returns up to limit orders updated after the (updatedAt, id)
// cursor, oldest first. Keyset pagination stays fast at any depth.
func (r *Repository) ListOrders(ctx context.Context, afterUpdated time.Time, afterID string, limit int) ([]domain.Order, error) {
	return r.list(ctx, `
		SELECT `+orderColumns+` FROM orders
		WHERE (updated_at, id) > ($1, $2)
		ORDER BY updated_at, id
		LIMIT $3`, afterUpdated, afterID, limit)
}

// OutboxBacklog returns the number of unpublished events and the age of the
// oldest one.
func (r *Repository) OutboxBacklog(ctx context.Context) (int, time.Duration, error) {
	var n int
	var oldest float64
	err := r.db.QueryRow(ctx, `
		SELECT count(*), coalesce(extract(epoch FROM now() - min(created_at)), 0)
		FROM outbox_events WHERE status = 'pending'`).Scan(&n, &oldest)
	return n, time.Duration(oldest * float64(time.Second)), err
}

// ClaimOutbox locks up to limit due events with FOR UPDATE SKIP LOCKED and
// passes them to publish while the lock is held. Several publisher replicas can
// run at once: each one skips rows another replica has locked. publish returns
// the IDs it delivered; the rest are rescheduled with exponential backoff.
//
// An event is only claimed once every earlier event of the same order is
// published. Without that rule, replica A could hold order.paid while replica
// B skips past it and publishes order.refunded first, and product-service
// would see the refund before the payment.
func (r *Repository) ClaimOutbox(ctx context.Context, limit, maxAttempts int, publish func([]domain.OutboxEvent) (delivered map[string]bool, err error)) (int, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, event_type, payload, trace_parent, attempts, created_at
		FROM outbox_events e
		WHERE status = 'pending' AND next_attempt_at <= now()
		  AND NOT EXISTS (
		      SELECT 1 FROM outbox_events earlier
		      WHERE earlier.aggregate_id = e.aggregate_id
		        AND earlier.status = 'pending'
		        AND earlier.seq < e.seq)
		ORDER BY seq
		LIMIT $1
		FOR UPDATE SKIP LOCKED`, limit)
	if err != nil {
		return 0, err
	}
	var batch []domain.OutboxEvent
	for rows.Next() {
		var e domain.OutboxEvent
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.EventType, &e.Payload, &e.TraceParent, &e.Attempts, &e.CreatedAt); err != nil {
			rows.Close()
			return 0, err
		}
		batch = append(batch, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(batch) == 0 {
		return 0, nil
	}

	delivered, publishErr := publish(batch)
	lastError := ""
	if publishErr != nil {
		lastError = publishErr.Error()
	}
	var published []string
	for _, e := range batch {
		if delivered[e.ID] {
			published = append(published, e.ID)
			continue
		}
		backoff := time.Duration(1<<min(e.Attempts, 8)) * time.Second
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events
			SET attempts = attempts + 1,
			    last_error = $2,
			    next_attempt_at = now() + make_interval(secs => $3),
			    status = CASE WHEN attempts + 1 >= $4 THEN 'failed' ELSE 'pending' END
			WHERE id = $1`, e.ID, lastError, backoff.Seconds(), maxAttempts); err != nil {
			return 0, err
		}
	}
	if len(published) > 0 {
		if _, err := tx.Exec(ctx, `
			UPDATE outbox_events SET status = 'published', published_at = now(), attempts = attempts + 1
			WHERE id = ANY($1)`, published); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(published), publishErr
}

// --- helpers ---------------------------------------------------------------

type extraWhere string

// transition runs a guarded UPDATE: it only matches the row while the order is
// still in one of the from states. set holds extra assignments using
// placeholders from $4 on; args fill them. An extraWhere arg adds a condition.
func transition(ctx context.Context, tx pgx.Tx, orderID string, from []string, to, set string, args ...any) (*domain.Order, error) {
	where := ""
	params := []any{orderID, to, from}
	for _, a := range args {
		if w, ok := a.(extraWhere); ok {
			where += " AND " + string(w)
			continue
		}
		params = append(params, a)
	}
	if set != "" {
		set = ", " + set
	}
	row := tx.QueryRow(ctx, `
		UPDATE orders SET status = $2, updated_at = now()`+set+`
		WHERE id = $1 AND status = ANY($3)`+where+`
		RETURNING `+orderColumns, params...)
	order, err := scanOrder(row)
	if errors.Is(err, domain.ErrNotFound) {
		// Distinguish "no such order" from "order is in another state".
		var status string
		if err := tx.QueryRow(ctx, `SELECT status FROM orders WHERE id = $1`, orderID).Scan(&status); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, domain.ErrNotFound
			}
			return nil, err
		}
		return nil, fmt.Errorf("%w: order is %s", domain.ErrConflict, status)
	}
	return order, err
}

func insertEvent(ctx context.Context, tx pgx.Tx, order *domain.Order, eventType, reason string) error {
	event := events.OrderEvent{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		OrderID:       order.ID,
		CustomerEmail: order.CustomerEmail,
		TotalCents:    order.TotalCents,
		Reason:        reason,
		OccurredAt:    time.Now().UTC(),
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (id, aggregate_type, aggregate_id, event_type, payload, trace_parent)
		VALUES ($1, 'order', $2, $3, $4, $5)`, event.EventID, order.ID, eventType, payload, obs.TraceParent(ctx))
	return err
}

func (r *Repository) inTx(ctx context.Context, fn func(pgx.Tx) (*domain.Order, error)) (*domain.Order, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck
	order, err := fn(tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	transitions.WithLabelValues(order.Status).Inc()
	if order.Items == nil {
		if order.Items, err = r.items(ctx, order.ID); err != nil {
			return nil, err
		}
	}
	return order, nil
}

type querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func (r *Repository) getBy(ctx context.Context, q querier, where string, arg any) (*domain.Order, error) {
	order, err := scanOrder(q.QueryRow(ctx, `SELECT `+orderColumns+` FROM orders WHERE `+where, arg))
	if err != nil {
		return nil, err
	}
	if order.Items, err = r.items(ctx, order.ID); err != nil {
		return nil, err
	}
	return order, nil
}

func (r *Repository) items(ctx context.Context, orderID string) ([]domain.Item, error) {
	rows, err := r.db.Query(ctx, `
		SELECT product_id, product_name, quantity, unit_price_cents, subtotal_cents
		FROM order_items WHERE order_id = $1 ORDER BY product_id`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []domain.Item{}
	for rows.Next() {
		var item domain.Item
		if err := rows.Scan(&item.ProductID, &item.ProductName, &item.Quantity, &item.UnitPriceCents, &item.SubtotalCents); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *Repository) list(ctx context.Context, sql string, args ...any) ([]domain.Order, error) {
	rows, err := r.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var orders []domain.Order
	for rows.Next() {
		order, err := scanOrder(rows)
		if err != nil {
			return nil, err
		}
		orders = append(orders, *order)
	}
	return orders, rows.Err()
}

func scanOrder(row pgx.Row) (*domain.Order, error) {
	var o domain.Order
	err := row.Scan(&o.ID, &o.CustomerEmail, &o.Status, &o.TotalCents, &o.FailureReason, &o.PaymentKey, &o.ExpiresAt, &o.CreatedAt, &o.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, domain.ErrNotFound
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "22P02" { // invalid input syntax
		return nil, domain.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return &o, nil
}
