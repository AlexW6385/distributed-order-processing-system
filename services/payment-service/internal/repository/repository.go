package repository

import (
	"context"

	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
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

func (r *Repository) FindByIdempotencyKey(ctx context.Context, key string) (*domain.Payment, error) {
	var payment domain.Payment
	err := r.db.QueryRow(ctx, `
		SELECT id, order_id, idempotency_key, amount_cents, status
		FROM payments
		WHERE idempotency_key = $1`, key).
		Scan(&payment.ID, &payment.OrderID, &payment.IdempotencyKey, &payment.AmountCents, &payment.Status)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &payment, nil
}

func (r *Repository) CreateSucceeded(ctx context.Context, orderID string, amountCents int64, key string) (*domain.Payment, error) {
	payment := &domain.Payment{
		ID:             uuid.NewString(),
		OrderID:        orderID,
		IdempotencyKey: key,
		AmountCents:    amountCents,
		Status:         "succeeded",
	}
	err := r.db.QueryRow(ctx, `
		INSERT INTO payments (id, order_id, idempotency_key, amount_cents, status)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (idempotency_key) DO UPDATE SET idempotency_key = EXCLUDED.idempotency_key
		RETURNING id, order_id, idempotency_key, amount_cents, status`,
		payment.ID, payment.OrderID, payment.IdempotencyKey, payment.AmountCents, payment.Status).
		Scan(&payment.ID, &payment.OrderID, &payment.IdempotencyKey, &payment.AmountCents, &payment.Status)
	if err != nil {
		return nil, err
	}
	return payment, nil
}
