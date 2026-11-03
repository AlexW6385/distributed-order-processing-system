package main

import (
	"context"
	"errors"
	"net/http"
	"time"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/api-gateway/internal/httpapi"
	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis/v8"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	log := logging.New("api-gateway")

	// The gateway's deadline covers the whole saga step behind it, so it is
	// longer than the per-hop timeout inside order-service.
	timeout := config.Duration("GATEWAY_GRPC_TIMEOUT", 5*time.Second)
	orderConn, err := grpcx.Dial(config.String("ORDER_GRPC_ADDR", "localhost:50051"), timeout)
	app.Must(log, "dial order-service", err)
	defer orderConn.Close()
	productConn, err := grpcx.Dial(config.String("PRODUCT_GRPC_ADDR", "localhost:50052"), timeout)
	app.Must(log, "dial product-service", err)
	defer productConn.Close()

	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	router.Use(gin.Recovery(), httpapi.RequestID(), httpapi.AccessLog(log),
		httpapi.RateLimit(rdb, config.Int("RATE_LIMIT_PER_MINUTE", 600), time.Minute, log))
	httpapi.New(orderv1.NewOrderServiceClient(orderConn), productv1.NewProductServiceClient(productConn)).Register(router)

	server := &http.Server{
		Addr:              ":" + config.String("API_GATEWAY_PORT", "8080"),
		Handler:           router,
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      30 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	log.Info("http server started", map[string]any{"addr": server.Addr})
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("http server stopped", err, nil)
	}
}
