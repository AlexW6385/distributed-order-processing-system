package repository

import (
	"context"
	"encoding/json"

	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) CreatePending(ctx context.Context, email string) (*domain.Order, error) {
	order := &domain.Order{
		ID:            uuid.NewString(),
		CustomerEmail: email,
		Status:        domain.StatusPending,
	}
	err := r.db.QueryRow(ctx, `
		INSERT INTO orders (id, customer_email, status)
		VALUES ($1, $2, $3)
		RETURNING created_at, updated_at`, order.ID, order.CustomerEmail, order.Status).
		Scan(&order.CreatedAt, &order.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return order, nil
}

func (r *Repository) SaveReserved(ctx context.Context, orderID string, items []domain.Item, totalCents int64) (*domain.Order, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	for _, item := range items {
		if _, err := tx.Exec(ctx, `
			INSERT INTO order_items
				(id, order_id, product_id, product_name, quantity, unit_price_cents, subtotal_cents)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			uuid.NewString(), orderID, item.ProductID, item.ProductName, item.Quantity, item.UnitPriceCents, item.SubtotalCents); err != nil {
			return nil, err
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE orders
		SET status = $1, total_cents = $2, updated_at = now()
		WHERE id = $3`,
		domain.StatusStockReserved, totalCents, orderID); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.Get(ctx, orderID)
}

func (r *Repository) MarkFailed(ctx context.Context, orderID string, reason string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE orders
		SET status = $1, failure_reason = $2, updated_at = now()
		WHERE id = $3`, domain.StatusFailed, reason, orderID)
	return err
}

func (r *Repository) MarkPaidAndCreateOutbox(ctx context.Context, order *domain.Order) (*domain.Order, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE orders
		SET status = $1, updated_at = now()
		WHERE id = $2`, domain.StatusPaid, order.ID); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(map[string]any{
		"order_id":       order.ID,
		"customer_email": order.CustomerEmail,
		"total_cents":    order.TotalCents,
	})
	if err != nil {
		return nil, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox_events
			(id, aggregate_type, aggregate_id, event_type, payload, status)
		VALUES ($1, 'order', $2, 'order.paid', $3, 'pending')`,
		uuid.NewString(), order.ID, payload); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.Get(ctx, order.ID)
}

func (r *Repository) Get(ctx context.Context, orderID string) (*domain.Order, error) {
	var order domain.Order
	err := r.db.QueryRow(ctx, `
		SELECT id, customer_email, status, total_cents, failure_reason, created_at, updated_at
		FROM orders
		WHERE id = $1`, orderID).
		Scan(&order.ID, &order.CustomerEmail, &order.Status, &order.TotalCents, &order.FailureReason, &order.CreatedAt, &order.UpdatedAt)
	if err != nil {
		return nil, err
	}

	rows, err := r.db.Query(ctx, `
		SELECT product_id, product_name, quantity, unit_price_cents, subtotal_cents
		FROM order_items
		WHERE order_id = $1
		ORDER BY product_name`, orderID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var item domain.Item
		if err := rows.Scan(&item.ProductID, &item.ProductName, &item.Quantity, &item.UnitPriceCents, &item.SubtotalCents); err != nil {
			return nil, err
		}
		order.Items = append(order.Items, item)
	}
	return &order, rows.Err()
}

func (r *Repository) PendingOutbox(ctx context.Context, limit int) ([]domain.OutboxEvent, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, event_type, payload
		FROM outbox_events
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []domain.OutboxEvent
	for rows.Next() {
		var event domain.OutboxEvent
		if err := rows.Scan(&event.ID, &event.EventType, &event.Payload); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (r *Repository) MarkOutboxPublished(ctx context.Context, eventID string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'published', published_at = now()
		WHERE id = $1`, eventID)
	return err
}

func (r *Repository) MarkOutboxFailed(ctx context.Context, eventID string, reason string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_events
		SET attempts = attempts + 1, last_error = $2, status = CASE WHEN attempts >= 9 THEN 'failed' ELSE 'pending' END
		WHERE id = $1`, eventID, reason)
	return err
}
