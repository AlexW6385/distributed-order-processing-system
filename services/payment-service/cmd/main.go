package main

import (
	"context"
	"fmt"
	"net"

	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	platformdb "github.com/AlexW6385/distributed-order-processing-system/internal/platform/db"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/transport/grpcapi"
	"github.com/go-redis/redis/v8"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	log := logging.New("payment-service")

	db, err := platformdb.Open(ctx, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"))
	if err != nil {
		log.Error("database connection failed", err, nil)
		panic(err)
	}
	defer db.Close()

	redisClient := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	if err := redisClient.Ping(ctx).Err(); err != nil {
		log.Error("redis connection failed", err, nil)
		panic(err)
	}
	defer redisClient.Close()

	port := config.String("PAYMENT_GRPC_PORT", "50053")
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Error("listen failed", err, nil)
		panic(err)
	}

	server := grpc.NewServer()
	paymentv1.RegisterPaymentServiceServer(server, grpcapi.New(service.New(repository.New(db), redisClient)))
	log.Info("grpc server started", map[string]any{"addr": fmt.Sprintf(":%s", port)})
	if err := server.Serve(listener); err != nil {
		log.Error("grpc server stopped", err, nil)
	}
}
