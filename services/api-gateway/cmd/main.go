package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	"github.com/AlexW6385/distributed-order-processing-system/services/api-gateway/internal/httpapi"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
)

func main() {
	ctx, done := app.Start("api-gateway")
	defer done()

	// The gateway's deadline covers the whole saga step behind it, so it is
	// longer than the per-hop timeouts inside order-service.
	opts := grpcx.ClientOptions{Timeout: config.Duration("GATEWAY_GRPC_TIMEOUT", 5*time.Second)}
	orderConn, err := grpcx.Dial(config.String("ORDER_GRPC_ADDR", "localhost:50051"), opts)
	app.Must("dial order-service", err)
	defer orderConn.Close()
	productConn, err := grpcx.Dial(config.String("PRODUCT_GRPC_ADDR", "localhost:50052"), opts)
	app.Must("dial product-service", err)
	defer productConn.Close()

	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.GET("/healthz", func(c *gin.Context) { c.Status(http.StatusOK) })
	api := router.Group("/", gin.Recovery(), otelgin.Middleware("api-gateway"), httpapi.Observe(),
		httpapi.RateLimit(rdb, config.Int("RATE_LIMIT_PER_MINUTE", 600), time.Minute))
	httpapi.New(orderv1.NewOrderServiceClient(orderConn), productv1.NewProductServiceClient(productConn)).Register(api)

	server := &http.Server{
		Addr:              ":" + config.String("API_GATEWAY_PORT", "8080"),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		time.Sleep(config.Duration("SHUTDOWN_DELAY", 0))
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	slog.Info("http server started", "addr", server.Addr)
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("http server stopped", "error", err)
	}
}
