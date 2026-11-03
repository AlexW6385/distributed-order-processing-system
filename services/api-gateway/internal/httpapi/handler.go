package httpapi

import (
	"net/http"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/httpjson"
	"github.com/gin-gonic/gin"
)

type Handler struct {
	orders   orderv1.OrderServiceClient
	products productv1.ProductServiceClient
}

func New(orders orderv1.OrderServiceClient, products productv1.ProductServiceClient) *Handler {
	return &Handler{orders: orders, products: products}
}

func (h *Handler) Register(router gin.IRouter) {
	router.GET("/health", h.health)
	router.GET("/products", h.listProducts)
	router.POST("/orders", h.createOrder)
	router.GET("/orders/:id", h.getOrder)
	router.POST("/orders/:id/pay", h.payOrder)
	router.POST("/orders/:id/cancel", h.cancelOrder)
}

func (h *Handler) health(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok"})
}

func (h *Handler) listProducts(c *gin.Context) {
	resp, err := h.products.ListProducts(c.Request.Context(), &productv1.ListProductsRequest{})
	if err != nil {
		httpjson.Error(c, err)
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
		httpjson.Error(c, err)
		return
	}
	c.JSON(http.StatusCreated, gin.H{"order": resp.Order})
}

func (h *Handler) getOrder(c *gin.Context) {
	resp, err := h.orders.GetOrder(c.Request.Context(), &orderv1.GetOrderRequest{OrderId: c.Param("id")})
	if err != nil {
		httpjson.Error(c, err)
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
	resp, err := h.orders.PayOrder(c.Request.Context(), &orderv1.PayOrderRequest{
		OrderId:        c.Param("id"),
		IdempotencyKey: key,
	})
	if err != nil {
		httpjson.Error(c, err)
		return
	}
	code := http.StatusOK
	if resp.Order.Status == "payment_pending" {
		code = http.StatusAccepted
	}
	c.JSON(code, gin.H{"order": resp.Order})
}

func (h *Handler) cancelOrder(c *gin.Context) {
	resp, err := h.orders.CancelOrder(c.Request.Context(), &orderv1.CancelOrderRequest{OrderId: c.Param("id")})
	if err != nil {
		httpjson.Error(c, err)
		return
	}
	c.JSON(http.StatusOK, gin.H{"order": resp.Order})
}
