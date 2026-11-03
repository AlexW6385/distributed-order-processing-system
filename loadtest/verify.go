package main

import (
	"context"
	"fmt"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/reconcile"
)

// verify checks the invariants that must hold across all services once the
// system has settled, over every order ever placed:
//
//  1. no order is stuck mid-saga (pending, payment_pending, refund_pending);
//  2. the outbox is drained and every terminal order has exactly its events;
//  3. each reservation's state matches its order's state;
//  4. payments and the ledger agree with orders (the reconciler's checks);
//  5. every published event produced exactly one notification.
func verify(ctx context.Context, o options) bool {
	orders := mustPool(ctx, o.ordersDB)
	defer orders.Close()
	products := mustPool(ctx, o.productsDB)
	defer products.Close()
	payments := mustPool(ctx, o.paymentsDB)
	defer payments.Close()
	notifications := mustPool(ctx, o.notifyDB)
	defer notifications.Close()

	fmt.Println("verify: waiting for the system to settle...")
	start := time.Now()
	var stuck, pendingEvents int
	for {
		_ = orders.QueryRow(ctx, `SELECT count(*) FROM orders WHERE status IN ('pending', 'payment_pending', 'refund_pending')`).Scan(&stuck)
		_ = orders.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status = 'pending'`).Scan(&pendingEvents)
		if stuck == 0 && pendingEvents == 0 || time.Since(start) > 150*time.Second {
			break
		}
		time.Sleep(time.Second)
	}
	fmt.Printf("  settled in %.0fs\n", time.Since(start).Seconds())

	ok := true
	ok = check(stuck == 0, "no order stuck mid-saga (%d)", stuck) && ok

	var failedEvents, duplicateEvents, missingEvents int
	_ = orders.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status <> 'published'`).Scan(&failedEvents)
	_ = orders.QueryRow(ctx, `SELECT count(*) FROM (SELECT 1 FROM outbox_events GROUP BY aggregate_id, event_type HAVING count(*) > 1) d`).Scan(&duplicateEvents)
	_ = orders.QueryRow(ctx, `
		SELECT count(*) FROM orders o WHERE NOT EXISTS (
			SELECT 1 FROM outbox_events e WHERE e.aggregate_id = o.id AND e.event_type = CASE o.status
				WHEN 'paid' THEN 'order.paid' WHEN 'refunded' THEN 'order.refunded'
				WHEN 'failed' THEN 'order.failed' WHEN 'expired' THEN 'order.expired'
				WHEN 'cancelled' THEN 'order.cancelled' END)
		AND o.status IN ('paid', 'refunded', 'failed', 'expired', 'cancelled')`).Scan(&missingEvents)
	ok = check(failedEvents == 0, "outbox fully published (%d unpublished)", failedEvents) && ok
	ok = check(duplicateEvents == 0 && missingEvents == 0, "one event per terminal transition (%d duplicated, %d missing)", duplicateEvents, missingEvents) && ok

	// Reservations follow orders asynchronously (Kafka); allow a moment to
	// catch up, then compare.
	var mismatched []string
	var reservationCount int
	for deadline := time.Now().Add(60 * time.Second); ; {
		mismatched, reservationCount = compareReservations(ctx, o)
		if len(mismatched) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	ok = check(len(mismatched) == 0, "reservation state matches order state for %d orders%s", reservationCount, sample(mismatched)) && ok

	report := reconcileFromDatabases(ctx, o)
	ok = check(report.OK(), "payments, refunds and ledger reconcile with %d orders%s", report.Orders, sample(report.Mismatches)) && ok

	var published, notified int
	for deadline := time.Now().Add(60 * time.Second); ; {
		_ = orders.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE status = 'published'`).Scan(&published)
		_ = notifications.QueryRow(ctx, `SELECT count(*) FROM notifications`).Scan(&notified)
		if published == notified || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Second)
	}
	ok = check(published == notified, "one notification per event (%d events, %d notifications)", published, notified) && ok
	return ok
}

func compareReservations(ctx context.Context, o options) ([]string, int) {
	orders := mustPool(ctx, o.ordersDB)
	defer orders.Close()
	products := mustPool(ctx, o.productsDB)
	defer products.Close()

	reservation := map[string]string{}
	rows, err := products.Query(ctx, `SELECT order_id, status FROM reservations`)
	if err != nil {
		return []string{err.Error()}, 0
	}
	for rows.Next() {
		var id, status string
		_ = rows.Scan(&id, &status)
		reservation[id] = status
	}
	rows.Close()

	want := map[string]string{
		"stock_reserved": "reserved", "paid": "confirmed",
		"failed": "released", "expired": "released", "cancelled": "released", "refunded": "released",
	}
	var mismatched []string
	n := 0
	rows, err = orders.Query(ctx, `SELECT id, status FROM orders`)
	if err != nil {
		return []string{err.Error()}, 0
	}
	defer rows.Close()
	for rows.Next() {
		var id, status string
		_ = rows.Scan(&id, &status)
		expected, ok := want[status]
		if !ok {
			continue
		}
		n++
		if got := reservation[id]; got != expected {
			mismatched = append(mismatched, fmt.Sprintf("order %s is %s, reservation %q", id, status, got))
		}
	}
	return mismatched, n
}

func reconcileFromDatabases(ctx context.Context, o options) reconcile.Report {
	orders := mustPool(ctx, o.ordersDB)
	defer orders.Close()
	payments := mustPool(ctx, o.paymentsDB)
	defer payments.Close()

	var all []reconcile.Order
	rows, _ := orders.Query(ctx, `SELECT id, status, total_cents FROM orders`)
	for rows.Next() {
		var r reconcile.Order
		_ = rows.Scan(&r.ID, &r.Status, &r.AmountCents)
		all = append(all, r)
	}
	rows.Close()
	var ps []reconcile.Payment
	rows, _ = payments.Query(ctx, `
		SELECT p.order_id, p.status, p.amount_cents, r.order_id IS NOT NULL
		FROM payments p LEFT JOIN refunds r ON r.order_id = p.order_id`)
	for rows.Next() {
		var p reconcile.Payment
		_ = rows.Scan(&p.OrderID, &p.Status, &p.AmountCents, &p.Refunded)
		ps = append(ps, p)
	}
	rows.Close()
	var l reconcile.Ledger
	_ = payments.QueryRow(ctx, `
		SELECT coalesce(sum(l.amount_cents), 0),
		       coalesce(sum(l.amount_cents) FILTER (WHERE l.account = 'cash'), 0),
		       coalesce(sum(l.amount_cents) FILTER (WHERE e.kind = 'charge' AND l.account = 'cash'), 0),
		       coalesce(-sum(l.amount_cents) FILTER (WHERE e.kind = 'refund' AND l.account = 'cash'), 0)
		FROM journal_lines l JOIN journal_entries e ON e.id = l.entry_id`).Scan(&l.TotalCents, &l.CashCents, &l.ChargedCents, &l.RefundedCents)
	return reconcile.Check(all, ps, l)
}

func sample(items []string) string {
	if len(items) == 0 {
		return ""
	}
	if len(items) > 3 {
		return fmt.Sprintf(": %v ... (%d total)", items[:3], len(items))
	}
	return fmt.Sprintf(": %v", items)
}
