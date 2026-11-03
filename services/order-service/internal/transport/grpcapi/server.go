package grpcapi

import (
	"context"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	orderv1.UnimplementedOrderServiceServer
	service *service.Service
}

func New(service *service.Service) *Server {
	return &Server{service: service}
}

func (s *Server) CreateOrder(ctx context.Context, req *orderv1.CreateOrderRequest) (*orderv1.OrderResponse, error) {
	input := make([]domain.ItemInput, 0, len(req.Items))
	for _, item := range req.Items {
		input = append(input, domain.ItemInput{ProductID: item.ProductId, Quantity: item.Quantity})
	}
	order, err := s.service.CreateOrder(ctx, req.CustomerEmail, input)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &orderv1.OrderResponse{Order: toProto(order)}, nil
}

func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.OrderResponse, error) {
	order, err := s.service.GetOrder(ctx, req.OrderId)
	if err != nil {
		return nil, status.Error(codes.NotFound, err.Error())
	}
	return &orderv1.OrderResponse{Order: toProto(order)}, nil
}

func (s *Server) PayOrder(ctx context.Context, req *orderv1.PayOrderRequest) (*orderv1.OrderResponse, error) {
	order, err := s.service.PayOrder(ctx, req.OrderId, req.IdempotencyKey)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &orderv1.OrderResponse{Order: toProto(order)}, nil
}

func toProto(order *domain.Order) *orderv1.Order {
	items := make([]*orderv1.OrderItem, 0, len(order.Items))
	for _, item := range order.Items {
		items = append(items, &orderv1.OrderItem{
			ProductId:      item.ProductID,
			ProductName:    item.ProductName,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPriceCents,
			SubtotalCents:  item.SubtotalCents,
		})
	}
	return &orderv1.Order{
		Id:            order.ID,
		CustomerEmail: order.CustomerEmail,
		Status:        order.Status,
		TotalCents:    order.TotalCents,
		Items:         items,
		FailureReason: order.FailureReason,
		CreatedAt:     order.CreatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
		UpdatedAt:     order.UpdatedAt.UTC().Format("2006-01-02T15:04:05Z07:00"),
	}
}
