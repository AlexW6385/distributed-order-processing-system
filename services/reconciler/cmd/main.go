// Command reconciler checks that orders, payments and the ledger agree. It
// runs once and exits non-zero on a mismatch (a Kubernetes CronJob).
package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	"github.com/AlexW6385/distributed-order-processing-system/internal/reconcile"
)

func main() {
	ctx, done := app.Start("reconciler")
	opts := grpcx.ClientOptions{Timeout: 30 * time.Second}
	orderConn, err := grpcx.Dial(config.String("ORDER_GRPC_ADDR", "localhost:50051"), opts)
	app.Must("dial order-service", err)
	paymentConn, err := grpcx.Dial(config.String("PAYMENT_GRPC_ADDR", "localhost:50053"), opts)
	app.Must("dial payment-service", err)
	orders := orderv1.NewOrderServiceClient(orderConn)
	payments := paymentv1.NewPaymentServiceClient(paymentConn)

	report, err := run(ctx, orders, payments)
	orderConn.Close()
	paymentConn.Close()
	done()
	if err != nil {
		slog.Error("reconciliation could not run", "error", err)
		os.Exit(2)
	}
	for _, m := range report.Mismatches {
		slog.Error("mismatch", "detail", m)
	}
	slog.Info("reconciliation finished", "orders", report.Orders, "payments", report.Payments,
		"in_flight", len(report.InFlight), "mismatches", len(report.Mismatches))
	if !report.OK() {
		os.Exit(1)
	}
}

func run(ctx context.Context, orders orderv1.OrderServiceClient, payments paymentv1.PaymentServiceClient) (reconcile.Report, error) {
	// Read the ledger and payments before orders: anything that settles
	// meanwhile shows up as a newer order state, never as money without an
	// order.
	summary, err := payments.GetLedgerSummary(ctx, &paymentv1.GetLedgerSummaryRequest{})
	if err != nil {
		return reconcile.Report{}, err
	}
	var ps []reconcile.Payment
	for token := ""; ; {
		resp, err := payments.ListPayments(ctx, &paymentv1.ListPaymentsRequest{PageToken: token, PageSize: 1000})
		if err != nil {
			return reconcile.Report{}, err
		}
		for _, p := range resp.Payments {
			ps = append(ps, reconcile.Payment{OrderID: p.OrderId, Status: p.Status, AmountCents: p.AmountCents, Refunded: p.Refunded})
		}
		if len(resp.Payments) == 0 {
			break
		}
		token = resp.NextPageToken
	}
	var all []reconcile.Order
	for token := ""; ; {
		resp, err := orders.ListOrders(ctx, &orderv1.ListOrdersRequest{PageToken: token, PageSize: 1000})
		if err != nil {
			return reconcile.Report{}, err
		}
		for _, o := range resp.Orders {
			all = append(all, reconcile.Order{ID: o.Id, Status: o.Status, AmountCents: o.TotalCents})
		}
		if len(resp.Orders) == 0 {
			break
		}
		token = resp.NextPageToken
	}
	return reconcile.Check(all, ps, reconcile.Ledger{
		TotalCents: summary.TotalCents, CashCents: summary.CashCents,
		ChargedCents: summary.ChargedCents, RefundedCents: summary.RefundedCents,
	}), nil
}
