package grpcapi

import (
	"context"
	"errors"

	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/domain"
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

func (s *Server) Charge(ctx context.Context, req *paymentv1.ChargeRequest) (*paymentv1.ChargeResponse, error) {
	payment, err := s.service.Charge(ctx, req.OrderId, req.AmountCents, req.IdempotencyKey)
	if err != nil {
		switch {
		case errors.Is(err, domain.ErrInvalid):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, domain.ErrMismatch):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return nil, status.Error(codes.DeadlineExceeded, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	return &paymentv1.ChargeResponse{
		PaymentId:     payment.ID,
		OrderId:       payment.OrderID,
		AmountCents:   payment.AmountCents,
		Status:        payment.Status,
		DeclineReason: payment.DeclineReason,
	}, nil
}
