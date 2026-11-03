package main

import (
	"context"
	"fmt"
	"net"

	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	platformdb "github.com/AlexW6385/distributed-order-processing-system/internal/platform/db"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/transport/grpcapi"
	"google.golang.org/grpc"
)

func main() {
	ctx := context.Background()
	log := logging.New("product-service")

	db, err := platformdb.Open(ctx, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"))
	if err != nil {
		log.Error("database connection failed", err, nil)
		panic(err)
	}
	defer db.Close()

	port := config.String("PRODUCT_GRPC_PORT", "50052")
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Error("listen failed", err, nil)
		panic(err)
	}

	server := grpc.NewServer()
	productv1.RegisterProductServiceServer(server, grpcapi.New(service.New(repository.New(db))))
	log.Info("grpc server started", map[string]any{"addr": fmt.Sprintf(":%s", port)})
	if err := server.Serve(listener); err != nil {
		log.Error("grpc server stopped", err, nil)
	}
}
