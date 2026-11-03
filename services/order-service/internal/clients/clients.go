// Package clients adapts the product and payment gRPC APIs to the order
// service's domain, translating gRPC status codes into domain errors.
package clients

import (
	"context"
	"fmt"

	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"google.golang.org/grpc/status"
)

type ProductClient struct {
	client productv1.ProductServiceClient
}

func NewProductClient(client productv1.ProductServiceClient) *ProductClient {
	return &ProductClient{client: client}
}

func (c *ProductClient) ReserveStock(ctx context.Context, orderID string, items []domain.ItemInput) ([]domain.Item, int64, error) {
	lines := make([]*productv1.ReservationLine, 0, len(items))
	for _, item := range items {
		lines = append(lines, &productv1.ReservationLine{ProductId: item.ProductID, Quantity: item.Quantity})
	}
	resp, err := c.client.ReserveStock(ctx, &productv1.ReserveStockRequest{OrderId: orderID, Items: lines})
	if err != nil {
		return nil, 0, translate(err)
	}
	reserved := make([]domain.Item, 0, len(resp.Items))
	for _, item := range resp.Items {
		reserved = append(reserved, domain.Item{
			ProductID:      item.ProductId,
			ProductName:    item.ProductName,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPriceCents,
			SubtotalCents:  item.SubtotalCents,
		})
	}
	return reserved, resp.TotalCents, nil
}

type PaymentClient struct {
	client paymentv1.PaymentServiceClient
}

func NewPaymentClient(client paymentv1.PaymentServiceClient) *PaymentClient {
	return &PaymentClient{client: client}
}

func (c *PaymentClient) Charge(ctx context.Context, orderID string, amountCents int64, idempotencyKey string) (domain.PaymentResult, error) {
	resp, err := c.client.Charge(ctx, &paymentv1.ChargeRequest{
		OrderId:        orderID,
		AmountCents:    amountCents,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return domain.PaymentResult{}, translate(err)
	}
	return domain.PaymentResult{
		Succeeded:     resp.Status == "succeeded",
		DeclineReason: resp.DeclineReason,
	}, nil
}

// translate separates definite refusals from calls whose outcome is unknown.
func translate(err error) error {
	msg := status.Convert(err).Message()
	if grpcx.IsUncertain(err) {
		return fmt.Errorf("%w: %s", domain.ErrUnavailable, msg)
	}
	return fmt.Errorf("%w: %s", domain.ErrRejected, msg)
}
