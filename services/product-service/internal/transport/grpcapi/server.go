package grpcapi

import (
	"context"

	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	productv1.UnimplementedProductServiceServer
	service *service.Service
}

func New(service *service.Service) *Server {
	return &Server{service: service}
}

func (s *Server) ListProducts(ctx context.Context, _ *productv1.ListProductsRequest) (*productv1.ListProductsResponse, error) {
	products, err := s.service.ListProducts(ctx)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	out := make([]*productv1.Product, 0, len(products))
	for _, product := range products {
		out = append(out, &productv1.Product{
			Id:          product.ID,
			Name:        product.Name,
			Description: product.Description,
			PriceCents:  product.PriceCents,
			Stock:       product.Stock,
		})
	}
	return &productv1.ListProductsResponse{Products: out}, nil
}

func (s *Server) ReserveStock(ctx context.Context, req *productv1.ReserveStockRequest) (*productv1.ReserveStockResponse, error) {
	lines := make([]domain.ReservationLine, 0, len(req.Items))
	for _, item := range req.Items {
		lines = append(lines, domain.ReservationLine{ProductID: item.ProductId, Quantity: item.Quantity})
	}
	items, total, err := s.service.ReserveStock(ctx, req.OrderId, lines)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return &productv1.ReserveStockResponse{Items: toProtoItems(items), TotalCents: total}, nil
}

func (s *Server) ReleaseReservation(ctx context.Context, req *productv1.ReleaseReservationRequest) (*productv1.ReleaseReservationResponse, error) {
	if err := s.service.ReleaseReservation(ctx, req.OrderId); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &productv1.ReleaseReservationResponse{}, nil
}

func (s *Server) ConfirmReservation(ctx context.Context, req *productv1.ConfirmReservationRequest) (*productv1.ConfirmReservationResponse, error) {
	if err := s.service.ConfirmReservation(ctx, req.OrderId); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &productv1.ConfirmReservationResponse{}, nil
}

func toProtoItems(items []domain.ReservedItem) []*productv1.ReservedItem {
	out := make([]*productv1.ReservedItem, 0, len(items))
	for _, item := range items {
		out = append(out, &productv1.ReservedItem{
			ProductId:      item.ProductID,
			ProductName:    item.ProductName,
			Quantity:       item.Quantity,
			UnitPriceCents: item.UnitPriceCents,
			SubtotalCents:  item.SubtotalCents,
		})
	}
	return out
}
