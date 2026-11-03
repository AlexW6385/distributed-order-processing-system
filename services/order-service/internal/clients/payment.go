package clients

import (
	"context"

	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
)

type PaymentClient struct {
	client paymentv1.PaymentServiceClient
}

func NewPaymentClient(client paymentv1.PaymentServiceClient) *PaymentClient {
	return &PaymentClient{client: client}
}

func (c *PaymentClient) PayOrder(ctx context.Context, orderID string, amountCents int64, idempotencyKey string) error {
	_, err := c.client.PayOrder(ctx, &paymentv1.PayOrderRequest{
		OrderId:        orderID,
		AmountCents:    amountCents,
		IdempotencyKey: idempotencyKey,
	})
	return err
}
