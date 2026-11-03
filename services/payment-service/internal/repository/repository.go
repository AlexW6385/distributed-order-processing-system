package repository

import (
	"context"
	"errors"

	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

// Insert stores p unless the order already has a payment. Either way it
// returns the payment that is now on record for the order, so two concurrent
// charges for one order always agree on a single result.
func (r *Repository) Insert(ctx context.Context, p domain.Payment) (*domain.Payment, error) {
	row := r.db.QueryRow(ctx, `
		INSERT INTO payments (id, order_id, idempotency_key, amount_cents, status, decline_reason)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (order_id) DO NOTHING
		RETURNING id, order_id, idempotency_key, amount_cents, status, decline_reason`,
		p.ID, p.OrderID, p.IdempotencyKey, p.AmountCents, p.Status, p.DeclineReason)
	stored, err := scan(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return r.FindByOrder(ctx, p.OrderID)
	}
	return stored, err
}

func (r *Repository) FindByOrder(ctx context.Context, orderID string) (*domain.Payment, error) {
	p, err := scan(r.db.QueryRow(ctx, `
		SELECT id, order_id, idempotency_key, amount_cents, status, decline_reason
		FROM payments WHERE order_id = $1`, orderID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

func scan(row pgx.Row) (*domain.Payment, error) {
	var p domain.Payment
	if err := row.Scan(&p.ID, &p.OrderID, &p.IdempotencyKey, &p.AmountCents, &p.Status, &p.DeclineReason); err != nil {
		return nil, err
	}
	return &p, nil
}
