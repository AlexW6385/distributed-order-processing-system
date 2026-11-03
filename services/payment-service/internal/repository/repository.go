package repository

import (
	"context"
	"errors"
	"fmt"
	"time"

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

const paymentColumns = `p.id, p.order_id, p.idempotency_key, p.amount_cents, p.status, p.decline_reason, r.order_id IS NOT NULL, p.created_at`

// Insert stores p unless the order already has a payment, and posts the
// charge to the ledger in the same transaction. Either way it returns the
// payment now on record, so concurrent charges for one order agree on a
// single result.
func (r *Repository) Insert(ctx context.Context, p domain.Payment) (*domain.Payment, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		INSERT INTO payments (id, order_id, idempotency_key, amount_cents, status, decline_reason)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (order_id) DO NOTHING`,
		p.ID, p.OrderID, p.IdempotencyKey, p.AmountCents, p.Status, p.DeclineReason)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 1 && p.Status == domain.StatusSucceeded {
		// Money in: debit cash, credit revenue.
		if err := post(ctx, tx, "charge", p.OrderID, map[string]int64{"cash": p.AmountCents, "revenue": -p.AmountCents}); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return r.FindByOrder(ctx, p.OrderID)
}

// Refund records a full refund for the order's successful payment, at most
// once, and posts the reversing ledger entry in the same transaction.
func (r *Repository) Refund(ctx context.Context, orderID string, amountCents int64) (*domain.Refund, bool, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	var status string
	var paid int64
	err = tx.QueryRow(ctx, `SELECT status, amount_cents FROM payments WHERE order_id = $1 FOR UPDATE`, orderID).Scan(&status, &paid)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, fmt.Errorf("%w: no payment for order %s", domain.ErrNotFound, orderID)
	}
	if err != nil {
		return nil, false, err
	}
	if status != domain.StatusSucceeded || paid != amountCents {
		return nil, false, fmt.Errorf("%w: payment is %s for %d cents", domain.ErrMismatch, status, paid)
	}

	refund := domain.Refund{ID: uuid.NewString(), OrderID: orderID, AmountCents: amountCents}
	tag, err := tx.Exec(ctx, `
		INSERT INTO refunds (id, order_id, amount_cents) VALUES ($1, $2, $3)
		ON CONFLICT (order_id) DO NOTHING`, refund.ID, orderID, amountCents)
	if err != nil {
		return nil, false, err
	}
	created := tag.RowsAffected() == 1
	if created {
		// Money out: debit revenue, credit cash.
		if err := post(ctx, tx, "refund", orderID, map[string]int64{"revenue": amountCents, "cash": -amountCents}); err != nil {
			return nil, false, err
		}
	} else if err := tx.QueryRow(ctx, `SELECT id FROM refunds WHERE order_id = $1`, orderID).Scan(&refund.ID); err != nil {
		return nil, false, err
	}
	return &refund, created, tx.Commit(ctx)
}

// post writes one journal entry. The deferred trigger rejects the commit if
// the lines do not sum to zero.
func post(ctx context.Context, tx pgx.Tx, kind, orderID string, lines map[string]int64) error {
	entryID := uuid.NewString()
	if _, err := tx.Exec(ctx, `INSERT INTO journal_entries (id, kind, order_id) VALUES ($1, $2, $3)`, entryID, kind, orderID); err != nil {
		return err
	}
	for account, amount := range lines {
		if _, err := tx.Exec(ctx, `INSERT INTO journal_lines (entry_id, account, amount_cents) VALUES ($1, $2, $3)`, entryID, account, amount); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository) FindByOrder(ctx context.Context, orderID string) (*domain.Payment, error) {
	p, err := scan(r.db.QueryRow(ctx, `
		SELECT `+paymentColumns+` FROM payments p LEFT JOIN refunds r ON r.order_id = p.order_id
		WHERE p.order_id = $1`, orderID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return p, err
}

// List pages through payments after the (createdAt, id) cursor.
func (r *Repository) List(ctx context.Context, afterCreated time.Time, afterID string, limit int) ([]domain.Payment, error) {
	rows, err := r.db.Query(ctx, `
		SELECT `+paymentColumns+` FROM payments p LEFT JOIN refunds r ON r.order_id = p.order_id
		WHERE (p.created_at, p.id) > ($1, $2)
		ORDER BY p.created_at, p.id
		LIMIT $3`, afterCreated, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []domain.Payment
	for rows.Next() {
		p, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

func (r *Repository) LedgerSummary(ctx context.Context) (domain.LedgerSummary, error) {
	var s domain.LedgerSummary
	err := r.db.QueryRow(ctx, `
		SELECT coalesce(sum(l.amount_cents), 0),
		       coalesce(sum(l.amount_cents) FILTER (WHERE l.account = 'cash'), 0),
		       coalesce(sum(l.amount_cents) FILTER (WHERE l.account = 'revenue'), 0),
		       coalesce(sum(l.amount_cents) FILTER (WHERE e.kind = 'charge' AND l.account = 'cash'), 0),
		       coalesce(-sum(l.amount_cents) FILTER (WHERE e.kind = 'refund' AND l.account = 'cash'), 0)
		FROM journal_lines l JOIN journal_entries e ON e.id = l.entry_id`).
		Scan(&s.TotalCents, &s.CashCents, &s.RevenueCents, &s.ChargedCents, &s.RefundedCents)
	return s, err
}

func scan(row pgx.Row) (*domain.Payment, error) {
	var p domain.Payment
	if err := row.Scan(&p.ID, &p.OrderID, &p.IdempotencyKey, &p.AmountCents, &p.Status, &p.DeclineReason, &p.Refunded, &p.CreatedAt); err != nil {
		return nil, err
	}
	return &p, nil
}
