// Package reconcile compares what order-service believes happened with what
// payment-service recorded. The two services never share a transaction, so
// this batch check is the backstop that proves the saga kept them consistent.
package reconcile

import (
	"fmt"
	"sort"
)

type Order struct {
	ID          string
	Status      string
	AmountCents int64
}

type Payment struct {
	OrderID     string
	Status      string // succeeded | declined
	AmountCents int64
	Refunded    bool
}

type Ledger struct {
	TotalCents    int64
	CashCents     int64
	ChargedCents  int64
	RefundedCents int64
}

type Report struct {
	Orders, Payments int
	InFlight         []string // orders whose money movement is still settling
	Mismatches       []string
}

func (r Report) OK() bool { return len(r.Mismatches) == 0 }

// Check reconciles orders, payments and the ledger.
func Check(orders []Order, payments []Payment, ledger Ledger) Report {
	r := Report{Orders: len(orders), Payments: len(payments)}
	byOrder := make(map[string]Payment, len(payments))
	for _, p := range payments {
		byOrder[p.OrderID] = p // payment-service enforces one payment per order
	}
	orderStatus := make(map[string]string, len(orders))
	var charged, refunded int64

	for _, o := range orders {
		orderStatus[o.ID] = o.Status
		p, hasPayment := byOrder[o.ID]
		succeeded := hasPayment && p.Status == "succeeded"
		switch o.Status {
		case "paid":
			if !succeeded {
				r.miss("order %s is paid but has no successful payment", o.ID)
			} else if p.Refunded {
				r.miss("order %s is paid but its payment was refunded", o.ID)
			}
		case "refunded":
			if !succeeded || !p.Refunded {
				r.miss("order %s is refunded but payment-service has no refund", o.ID)
			}
		case "payment_pending", "refund_pending":
			r.InFlight = append(r.InFlight, o.ID)
		default: // pending, stock_reserved, failed, expired, cancelled
			if succeeded {
				r.miss("order %s is %s but was charged", o.ID, o.Status)
			}
		}
		if succeeded && p.AmountCents != o.AmountCents && o.Status != "payment_pending" {
			r.miss("order %s total %d differs from payment %d", o.ID, o.AmountCents, p.AmountCents)
		}
	}
	for _, p := range payments {
		if p.Status != "succeeded" {
			continue
		}
		charged += p.AmountCents
		if p.Refunded {
			refunded += p.AmountCents
		}
		if _, ok := orderStatus[p.OrderID]; !ok {
			r.miss("payment for unknown order %s", p.OrderID)
		}
	}

	if ledger.TotalCents != 0 {
		r.miss("ledger does not balance: lines sum to %d", ledger.TotalCents)
	}
	if ledger.ChargedCents != charged || ledger.RefundedCents != refunded {
		r.miss("ledger charged/refunded %d/%d, payments say %d/%d", ledger.ChargedCents, ledger.RefundedCents, charged, refunded)
	}
	if ledger.CashCents != charged-refunded {
		r.miss("cash %d != charges - refunds %d", ledger.CashCents, charged-refunded)
	}
	sort.Strings(r.Mismatches)
	return r
}

func (r *Report) miss(format string, args ...any) {
	r.Mismatches = append(r.Mismatches, fmt.Sprintf(format, args...))
}
