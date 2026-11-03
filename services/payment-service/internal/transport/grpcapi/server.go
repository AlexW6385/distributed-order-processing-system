package grpcapi

import (
	"context"

	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	paymentv1.UnimplementedPaymentServiceServer
	service *service.Service
}

func New(service *service.Service) *Server {
	return &Server{service: service}
}

func (s *Server) PayOrder(ctx context.Context, req *paymentv1.PayOrderRequest) (*paymentv1.PayOrderResponse, error) {
	payment, err := s.service.PayOrder(ctx, req.OrderId, req.AmountCents, req.IdempotencyKey)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &paymentv1.PayOrderResponse{
		PaymentId:   payment.ID,
		OrderId:     payment.OrderID,
		AmountCents: payment.AmountCents,
		Status:      payment.Status,
	}, nil
}
