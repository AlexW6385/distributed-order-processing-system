package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeOrders struct {
	orderv1.OrderServiceClient
	payStatus string
	err       error
	gotKey    string
}

func (f *fakeOrders) PayOrder(ctx context.Context, in *orderv1.PayOrderRequest, _ ...grpc.CallOption) (*orderv1.OrderResponse, error) {
	f.gotKey = in.IdempotencyKey
	if f.err != nil {
		return nil, f.err
	}
	return &orderv1.OrderResponse{Order: &orderv1.Order{Id: in.OrderId, Status: f.payStatus}}, nil
}

func (f *fakeOrders) CreateOrder(ctx context.Context, in *orderv1.CreateOrderRequest, _ ...grpc.CallOption) (*orderv1.OrderResponse, error) {
	f.gotKey = in.IdempotencyKey
	if f.err != nil {
		return nil, f.err
	}
	return &orderv1.OrderResponse{Order: &orderv1.Order{Id: "o1", Status: "stock_reserved"}}, nil
}

func serve(orders orderv1.OrderServiceClient, method, path, body string, headers map[string]string) *httptest.ResponseRecorder {
	gin.SetMode(gin.TestMode)
	router := gin.New()
	New(orders, productv1.NewProductServiceClient(nil)).Register(router)
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestPayStatusCodes(t *testing.T) {
	key := map[string]string{"Idempotency-Key": "k1"}
	cases := []struct {
		name   string
		orders *fakeOrders
		want   int
	}{
		{"paid", &fakeOrders{payStatus: "paid"}, http.StatusOK},
		{"declined", &fakeOrders{payStatus: "failed"}, http.StatusOK},
		{"outcome unknown", &fakeOrders{payStatus: "payment_pending"}, http.StatusAccepted},
		{"wrong state", &fakeOrders{err: status.Error(codes.FailedPrecondition, "order is cancelled")}, http.StatusConflict},
		{"not found", &fakeOrders{err: status.Error(codes.NotFound, "order not found")}, http.StatusNotFound},
		{"order-service down", &fakeOrders{err: status.Error(codes.Unavailable, "down")}, http.StatusServiceUnavailable},
		{"timeout", &fakeOrders{err: status.Error(codes.DeadlineExceeded, "slow")}, http.StatusGatewayTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := serve(tc.orders, http.MethodPost, "/orders/o1/pay", "", key)
			if rec.Code != tc.want {
				t.Fatalf("status = %d, want %d (body %s)", rec.Code, tc.want, rec.Body)
			}
		})
	}
}

func TestPayRequiresIdempotencyKey(t *testing.T) {
	rec := serve(&fakeOrders{payStatus: "paid"}, http.MethodPost, "/orders/o1/pay", "", nil)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestCreatePassesIdempotencyKey(t *testing.T) {
	orders := &fakeOrders{}
	rec := serve(orders, http.MethodPost, "/orders",
		`{"customer_email":"a@b.co","items":[{"product_id":"p","quantity":1}]}`,
		map[string]string{"Idempotency-Key": "create-1", "Content-Type": "application/json"})
	if rec.Code != http.StatusCreated || orders.gotKey != "create-1" {
		t.Fatalf("status=%d key=%q", rec.Code, orders.gotKey)
	}
}
