package repository

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	db *pgxpool.Pool
}

func New(db *pgxpool.Pool) *Repository {
	return &Repository{db: db}
}

func (r *Repository) ListProducts(ctx context.Context) ([]domain.Product, error) {
	rows, err := r.db.Query(ctx, `
		SELECT id, name, description, price_cents, stock
		FROM products
		ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var products []domain.Product
	for rows.Next() {
		var p domain.Product
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.PriceCents, &p.Stock); err != nil {
			return nil, err
		}
		products = append(products, p)
	}
	return products, rows.Err()
}

// ReserveStock reserves stock for an order. It is idempotent: a retry for an
// order that is already reserved returns the existing reservation.
//
// Concurrency: the reservations row for the order is inserted first, so two
// concurrent calls for the same order serialize on its primary key. Product
// rows are then locked with SELECT ... FOR UPDATE in product-ID order; taking
// locks in one global order means two orders for overlapping products cannot
// deadlock each other.
func (r *Repository) ReserveStock(ctx context.Context, orderID string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		INSERT INTO reservations (order_id, status) VALUES ($1, 'reserved')
		ON CONFLICT (order_id) DO NOTHING`, orderID)
	if err != nil {
		return nil, 0, err
	}
	if tag.RowsAffected() == 0 {
		// Seen this order before: replay or tombstone.
		var status string
		if err := tx.QueryRow(ctx, `SELECT status FROM reservations WHERE order_id = $1 FOR UPDATE`, orderID).Scan(&status); err != nil {
			return nil, 0, err
		}
		if status != domain.ReservationReserved {
			return nil, 0, fmt.Errorf("%w: order %s is %s", domain.ErrReservationClosed, orderID, status)
		}
		items, total, err := reservedItems(ctx, tx, orderID)
		if err != nil {
			return nil, 0, err
		}
		return items, total, tx.Commit(ctx)
	}

	sorted := append([]domain.ReservationLine(nil), lines...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ProductID < sorted[j].ProductID })

	items := make([]domain.ReservedItem, 0, len(sorted))
	var total int64
	for _, line := range sorted {
		var product domain.Product
		err := tx.QueryRow(ctx, `
			SELECT id, name, price_cents, stock
			FROM products
			WHERE id = $1
			FOR UPDATE`, line.ProductID).
			Scan(&product.ID, &product.Name, &product.PriceCents, &product.Stock)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, 0, fmt.Errorf("%w: product %s", domain.ErrNotFound, line.ProductID)
		}
		if err != nil {
			return nil, 0, err
		}
		if product.Stock < line.Quantity {
			return nil, 0, fmt.Errorf("%w: product %s has %d left", domain.ErrInsufficientStock, line.ProductID, product.Stock)
		}

		subtotal := product.PriceCents * int64(line.Quantity)
		if _, err := tx.Exec(ctx, `UPDATE products SET stock = stock - $1, updated_at = now() WHERE id = $2`, line.Quantity, line.ProductID); err != nil {
			return nil, 0, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO reservation_items (order_id, product_id, product_name, quantity, unit_price_cents, subtotal_cents)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			orderID, product.ID, product.Name, line.Quantity, product.PriceCents, subtotal); err != nil {
			return nil, 0, err
		}
		items = append(items, domain.ReservedItem{
			ProductID:      product.ID,
			ProductName:    product.Name,
			Quantity:       line.Quantity,
			UnitPriceCents: product.PriceCents,
			SubtotalCents:  subtotal,
		})
		total += subtotal
	}
	if _, err := tx.Exec(ctx, `UPDATE reservations SET total_cents = $2 WHERE order_id = $1`, orderID, total); err != nil {
		return nil, 0, err
	}
	return items, total, tx.Commit(ctx)
}

// Release returns an order's reserved stock. It is idempotent. Releasing an
// order that has no reservation yet records a tombstone so a delayed
// ReserveStock for that order is rejected instead of leaking stock.
func (r *Repository) Release(ctx context.Context, orderID string) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	tag, err := tx.Exec(ctx, `
		INSERT INTO reservations (order_id, status) VALUES ($1, 'released')
		ON CONFLICT (order_id) DO NOTHING`, orderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return tx.Commit(ctx) // tombstone
	}

	var status string
	if err := tx.QueryRow(ctx, `SELECT status FROM reservations WHERE order_id = $1 FOR UPDATE`, orderID).Scan(&status); err != nil {
		return err
	}
	switch status {
	case domain.ReservationReleased:
		return nil
	case domain.ReservationConfirmed:
		return fmt.Errorf("%w: release of confirmed order %s", domain.ErrInconsistent, orderID)
	}

	// Lock products in the same global order as ReserveStock, then restore.
	if _, err := tx.Exec(ctx, `
		SELECT p.id FROM products p
		JOIN reservation_items ri ON ri.product_id = p.id AND ri.order_id = $1
		ORDER BY p.id
		FOR UPDATE OF p`, orderID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE products p
		SET stock = p.stock + ri.quantity, updated_at = now()
		FROM (
			SELECT product_id, quantity FROM reservation_items
			WHERE order_id = $1 ORDER BY product_id
		) ri
		WHERE p.id = ri.product_id`, orderID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE reservations SET status = 'released', updated_at = now() WHERE order_id = $1`, orderID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Confirm marks a reservation as consumed by a paid order. It is idempotent.
func (r *Repository) Confirm(ctx context.Context, orderID string) error {
	tag, err := r.db.Exec(ctx, `
		UPDATE reservations SET status = 'confirmed', updated_at = now()
		WHERE order_id = $1 AND status = 'reserved'`, orderID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var status string
	err = r.db.QueryRow(ctx, `SELECT status FROM reservations WHERE order_id = $1`, orderID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("%w: confirm of unknown order %s", domain.ErrInconsistent, orderID)
	}
	if err != nil {
		return err
	}
	if status == domain.ReservationConfirmed {
		return nil
	}
	return fmt.Errorf("%w: confirm of %s order %s", domain.ErrInconsistent, status, orderID)
}

func reservedItems(ctx context.Context, tx pgx.Tx, orderID string) ([]domain.ReservedItem, int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT product_id, product_name, quantity, unit_price_cents, subtotal_cents
		FROM reservation_items WHERE order_id = $1 ORDER BY product_id`, orderID)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	var items []domain.ReservedItem
	var total int64
	for rows.Next() {
		var item domain.ReservedItem
		if err := rows.Scan(&item.ProductID, &item.ProductName, &item.Quantity, &item.UnitPriceCents, &item.SubtotalCents); err != nil {
			return nil, 0, err
		}
		items = append(items, item)
		total += item.SubtotalCents
	}
	return items, total, rows.Err()
}
