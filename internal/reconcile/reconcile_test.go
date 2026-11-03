package reconcile

import "testing"

func TestConsistentBooks(t *testing.T) {
	orders := []Order{
		{ID: "a", Status: "paid", AmountCents: 100},
		{ID: "b", Status: "refunded", AmountCents: 50},
		{ID: "c", Status: "failed", AmountCents: 70},
		{ID: "d", Status: "payment_pending", AmountCents: 10},
	}
	payments := []Payment{
		{OrderID: "a", Status: "succeeded", AmountCents: 100},
		{OrderID: "b", Status: "succeeded", AmountCents: 50, Refunded: true},
		{OrderID: "c", Status: "declined", AmountCents: 70},
	}
	r := Check(orders, payments, Ledger{TotalCents: 0, CashCents: 100, ChargedCents: 150, RefundedCents: 50})
	if !r.OK() {
		t.Fatalf("unexpected mismatches: %v", r.Mismatches)
	}
	if len(r.InFlight) != 1 {
		t.Fatalf("in flight = %v, want [d]", r.InFlight)
	}
}

func TestMismatches(t *testing.T) {
	cases := map[string]struct {
		orders   []Order
		payments []Payment
		ledger   Ledger
	}{
		"paid without payment": {
			orders: []Order{{ID: "a", Status: "paid", AmountCents: 100}},
		},
		"charged but failed": {
			orders:   []Order{{ID: "a", Status: "failed", AmountCents: 100}},
			payments: []Payment{{OrderID: "a", Status: "succeeded", AmountCents: 100}},
			ledger:   Ledger{CashCents: 100, ChargedCents: 100},
		},
		"refunded without refund": {
			orders:   []Order{{ID: "a", Status: "refunded", AmountCents: 100}},
			payments: []Payment{{OrderID: "a", Status: "succeeded", AmountCents: 100}},
			ledger:   Ledger{CashCents: 100, ChargedCents: 100},
		},
		"orphan payment": {
			payments: []Payment{{OrderID: "x", Status: "succeeded", AmountCents: 100}},
			ledger:   Ledger{CashCents: 100, ChargedCents: 100},
		},
		"amount differs": {
			orders:   []Order{{ID: "a", Status: "paid", AmountCents: 100}},
			payments: []Payment{{OrderID: "a", Status: "succeeded", AmountCents: 90}},
			ledger:   Ledger{CashCents: 90, ChargedCents: 90},
		},
		"unbalanced ledger": {
			ledger: Ledger{TotalCents: 5},
		},
		"ledger disagrees with payments": {
			orders:   []Order{{ID: "a", Status: "paid", AmountCents: 100}},
			payments: []Payment{{OrderID: "a", Status: "succeeded", AmountCents: 100}},
			ledger:   Ledger{CashCents: 200, ChargedCents: 200},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if r := Check(tc.orders, tc.payments, tc.ledger); r.OK() {
				t.Fatal("expected a mismatch")
			}
		})
	}
}
