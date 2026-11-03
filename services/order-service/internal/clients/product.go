package clients

import (
	"context"

	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
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
		return nil, 0, err
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

func (c *ProductClient) ReleaseReservation(ctx context.Context, orderID string) error {
	_, err := c.client.ReleaseReservation(ctx, &productv1.ReleaseReservationRequest{OrderId: orderID})
	return err
}

func (c *ProductClient) ConfirmReservation(ctx context.Context, orderID string) error {
	_, err := c.client.ConfirmReservation(ctx, &productv1.ConfirmReservationRequest{OrderId: orderID})
	return err
}
