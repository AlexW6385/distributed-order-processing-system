package repository

import (
	"context"
	"fmt"

	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
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

func (r *Repository) ReserveStock(ctx context.Context, orderID string, lines []domain.ReservationLine) ([]domain.ReservedItem, int64, error) {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback(ctx)

	items := make([]domain.ReservedItem, 0, len(lines))
	var total int64
	for _, line := range lines {
		var product domain.Product
		err := tx.QueryRow(ctx, `
			SELECT id, name, description, price_cents, stock
			FROM products
			WHERE id = $1
			FOR UPDATE`, line.ProductID).
			Scan(&product.ID, &product.Name, &product.Description, &product.PriceCents, &product.Stock)
		if err == pgx.ErrNoRows {
			return nil, 0, fmt.Errorf("product %s not found", line.ProductID)
		}
		if err != nil {
			return nil, 0, err
		}
		if product.Stock < line.Quantity {
			return nil, 0, fmt.Errorf("product %s has insufficient stock", line.ProductID)
		}

		subtotal := product.PriceCents * int64(line.Quantity)
		if _, err := tx.Exec(ctx, `UPDATE products SET stock = stock - $1, updated_at = now() WHERE id = $2`, line.Quantity, line.ProductID); err != nil {
			return nil, 0, err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO stock_reservations
				(id, order_id, product_id, quantity, unit_price_cents, subtotal_cents, status)
			VALUES ($1, $2, $3, $4, $5, $6, 'reserved')`,
			uuid.NewString(), orderID, line.ProductID, line.Quantity, product.PriceCents, subtotal); err != nil {
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

	if err := tx.Commit(ctx); err != nil {
		return nil, 0, err
	}
	return items, total, nil
}

func (r *Repository) ReleaseReservation(ctx context.Context, orderID string) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT product_id, quantity
		FROM stock_reservations
		WHERE order_id = $1 AND status = 'reserved'
		FOR UPDATE`, orderID)
	if err != nil {
		return err
	}
	defer rows.Close()

	type row struct {
		productID string
		quantity  int32
	}
	var reserved []row
	for rows.Next() {
		var item row
		if err := rows.Scan(&item.productID, &item.quantity); err != nil {
			return err
		}
		reserved = append(reserved, item)
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, item := range reserved {
		if _, err := tx.Exec(ctx, `UPDATE products SET stock = stock + $1, updated_at = now() WHERE id = $2`, item.quantity, item.productID); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE stock_reservations SET status = 'released', updated_at = now() WHERE order_id = $1 AND status = 'reserved'`, orderID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (r *Repository) ConfirmReservation(ctx context.Context, orderID string) error {
	_, err := r.db.Exec(ctx, `
		UPDATE stock_reservations
		SET status = 'confirmed', updated_at = now()
		WHERE order_id = $1 AND status = 'reserved'`, orderID)
	return err
}
