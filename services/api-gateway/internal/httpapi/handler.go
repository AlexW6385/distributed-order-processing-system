package httpapi

import (
	"net/http"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Handler struct {
	orders   orderv1.OrderServiceClient
	products productv1.ProductServiceClient
}

func New(orders orderv1.OrderServiceClient, products productv1.ProductServiceClient) *Handler {
	return &Handler{orders: orders, products: products}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/products", h.listProducts)
	router.POST("/orders", h.createOrder)
	router.GET("/orders/:id", h.getOrder)
	router.POST("/orders/:id/pay", h.payOrder)
	router.POST("/orders/:id/cancel", h.cancelOrder)
	router.POST("/orders/:id/refund", h.refundOrder)
	router.POST("/admin/flash-sales", h.startFlashSale)
}

func (h *Handler) listProducts(c *gin.Context) {
	resp, err := h.products.ListProducts(c.Request.Context(), &productv1.ListProductsRequest{})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"products": resp.Products})
}

type createOrderRequest struct {
	CustomerEmail string `json:"customer_email"`
	Items         []struct {
		ProductID string `json:"product_id"`
		Quantity  int32  `json:"quantity"`
	} `json:"items"`
}

func (h *Handler) createOrder(c *gin.Context) {
	var body createOrderRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	items := make([]*orderv1.OrderItemInput, 0, len(body.Items))
	for _, item := range body.Items {
		items = append(items, &orderv1.OrderItemInput{ProductId: item.ProductID, Quantity: item.Quantity})
	}
	resp, err := h.orders.CreateOrder(c.Request.Context(), &orderv1.CreateOrderRequest{
		CustomerEmail:  body.CustomerEmail,
		Items:          items,
		IdempotencyKey: c.GetHeader("Idempotency-Key"),
	})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"order": resp.Order})
}

func (h *Handler) getOrder(c *gin.Context) {
	resp, err := h.orders.GetOrder(c.Request.Context(), &orderv1.GetOrderRequest{OrderId: c.Param("id")})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"order": resp.Order})
}

// payOrder answers 200 when the order is paid or failed, and 202 when the
// payment outcome is still unknown; the client should poll GET /orders/{id}.
func (h *Handler) payOrder(c *gin.Context) {
	key := c.GetHeader("Idempotency-Key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Idempotency-Key header is required"})
		return
	}
	resp, err := h.orders.PayOrder(c.Request.Context(), &orderv1.PayOrderRequest{OrderId: c.Param("id"), IdempotencyKey: key})
	writeOrder(c, resp, err, "payment_pending")
}

func (h *Handler) cancelOrder(c *gin.Context) {
	resp, err := h.orders.CancelOrder(c.Request.Context(), &orderv1.CancelOrderRequest{OrderId: c.Param("id")})
	writeOrder(c, resp, err, "")
}

// refundOrder answers 202 while the refund outcome is unknown.
func (h *Handler) refundOrder(c *gin.Context) {
	resp, err := h.orders.RefundOrder(c.Request.Context(), &orderv1.RefundOrderRequest{OrderId: c.Param("id")})
	writeOrder(c, resp, err, "refund_pending")
}

type flashSaleRequest struct {
	ProductID string `json:"product_id"`
	Stock     int32  `json:"stock"`
}

func (h *Handler) startFlashSale(c *gin.Context) {
	var body flashSaleRequest
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := h.products.StartFlashSale(c.Request.Context(), &productv1.StartFlashSaleRequest{ProductId: body.ProductID, Stock: body.Stock})
	if err != nil {
		writeError(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"product_id": resp.ProductId, "stock": resp.Stock})
}

func writeOrder(c *gin.Context, resp *orderv1.OrderResponse, err error, pendingStatus string) {
	if err != nil {
		writeError(c, err)
		return
	}
	code := http.StatusOK
	if pendingStatus != "" && resp.Order.Status == pendingStatus {
		code = http.StatusAccepted
	}
	c.JSON(code, gin.H{"order": resp.Order})
}

// writeError maps a gRPC status to an HTTP status and a JSON error body.
func writeError(c *gin.Context, err error) {
	st := status.Convert(err)
	c.JSON(httpStatus(st.Code()), gin.H{"error": st.Message()})
}

func httpStatus(code codes.Code) int {
	switch code {
	case codes.InvalidArgument, codes.OutOfRange:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.FailedPrecondition, codes.Aborted:
		return http.StatusConflict
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	default:
		return http.StatusInternalServerError
	}
}
