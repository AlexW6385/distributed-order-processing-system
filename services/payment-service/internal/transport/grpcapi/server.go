package grpcapi

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

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
	p, err := s.service.Charge(ctx, req.OrderId, req.AmountCents, req.IdempotencyKey)
	if err != nil {
		return nil, toStatus(err)
	}
	return &paymentv1.ChargeResponse{
		PaymentId:     p.ID,
		OrderId:       p.OrderID,
		AmountCents:   p.AmountCents,
		Status:        p.Status,
		DeclineReason: p.DeclineReason,
	}, nil
}

func (s *Server) Refund(ctx context.Context, req *paymentv1.RefundRequest) (*paymentv1.RefundResponse, error) {
	r, err := s.service.Refund(ctx, req.OrderId, req.AmountCents)
	if err != nil {
		return nil, toStatus(err)
	}
	return &paymentv1.RefundResponse{RefundId: r.ID, OrderId: r.OrderID, AmountCents: r.AmountCents}, nil
}

// ListPayments uses an opaque "<unix nanos>|<id>" page token over (created_at, id).
func (s *Server) ListPayments(ctx context.Context, req *paymentv1.ListPaymentsRequest) (*paymentv1.ListPaymentsResponse, error) {
	after, afterID := time.Time{}, ""
	if req.PageToken != "" {
		nanos, id, ok := strings.Cut(req.PageToken, "|")
		n, err := strconv.ParseInt(nanos, 10, 64)
		if !ok || err != nil {
			return nil, status.Error(codes.InvalidArgument, "bad page token")
		}
		after, afterID = time.Unix(0, n), id
	}
	payments, err := s.service.List(ctx, after, afterID, int(req.PageSize))
	if err != nil {
		return nil, toStatus(err)
	}
	resp := &paymentv1.ListPaymentsResponse{}
	for _, p := range payments {
		resp.Payments = append(resp.Payments, &paymentv1.Payment{
			PaymentId: p.ID, OrderId: p.OrderID, AmountCents: p.AmountCents, Status: p.Status, Refunded: p.Refunded,
		})
	}
	if n := len(payments); n > 0 {
		last := payments[n-1]
		resp.NextPageToken = fmt.Sprintf("%d|%s", last.CreatedAt.UnixNano(), last.ID)
	}
	return resp, nil
}

func (s *Server) GetLedgerSummary(ctx context.Context, _ *paymentv1.GetLedgerSummaryRequest) (*paymentv1.LedgerSummary, error) {
	l, err := s.service.LedgerSummary(ctx)
	if err != nil {
		return nil, toStatus(err)
	}
	return &paymentv1.LedgerSummary{
		TotalCents: l.TotalCents, CashCents: l.CashCents, RevenueCents: l.RevenueCents,
		ChargedCents: l.ChargedCents, RefundedCents: l.RefundedCents,
	}, nil
}

func toStatus(err error) error {
	switch {
	case errors.Is(err, domain.ErrInvalid):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrMismatch):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}
