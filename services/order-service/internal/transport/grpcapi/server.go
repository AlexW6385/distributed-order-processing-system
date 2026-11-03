package grpcapi

import (
	"context"
	"errors"
	"time"

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
	order, err := s.service.CreateOrder(ctx, service.CreateInput{
		CustomerEmail:  req.CustomerEmail,
		Items:          input,
		IdempotencyKey: req.IdempotencyKey,
	})
	return respond(order, err)
}

func (s *Server) GetOrder(ctx context.Context, req *orderv1.GetOrderRequest) (*orderv1.OrderResponse, error) {
	return respond(s.service.GetOrder(ctx, req.OrderId))
}

func (s *Server) PayOrder(ctx context.Context, req *orderv1.PayOrderRequest) (*orderv1.OrderResponse, error) {
	return respond(s.service.PayOrder(ctx, req.OrderId, req.IdempotencyKey))
}

func (s *Server) CancelOrder(ctx context.Context, req *orderv1.CancelOrderRequest) (*orderv1.OrderResponse, error) {
	return respond(s.service.CancelOrder(ctx, req.OrderId))
}

func respond(order *domain.Order, err error) (*orderv1.OrderResponse, error) {
	if err != nil {
		return nil, toStatus(err)
	}
	return &orderv1.OrderResponse{Order: toProto(order)}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrConflict), errors.Is(err, domain.ErrRejected):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, domain.ErrUnavailable):
		return status.Error(codes.Unavailable, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
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
	out := &orderv1.Order{
		Id:            order.ID,
		CustomerEmail: order.CustomerEmail,
		Status:        order.Status,
		TotalCents:    order.TotalCents,
		Items:         items,
		FailureReason: order.FailureReason,
		CreatedAt:     order.CreatedAt.UTC().Format(time.RFC3339),
		UpdatedAt:     order.UpdatedAt.UTC().Format(time.RFC3339),
	}
	if order.ExpiresAt != nil {
		out.ExpiresAt = order.ExpiresAt.UTC().Format(time.RFC3339)
	}
	return out
}
