package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	platformdb "github.com/AlexW6385/distributed-order-processing-system/internal/platform/db"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/clients"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/outbox"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/repository"
	orderservice "github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/transport/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := logging.New("order-service")

	db, err := platformdb.Open(ctx, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"))
	if err != nil {
		log.Error("database connection failed", err, nil)
		panic(err)
	}
	defer db.Close()

	productConn, err := grpc.NewClient(config.String("PRODUCT_GRPC_ADDR", "localhost:50052"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("product grpc connection failed", err, nil)
		panic(err)
	}
	defer productConn.Close()

	paymentConn, err := grpc.NewClient(config.String("PAYMENT_GRPC_ADDR", "localhost:50053"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Error("payment grpc connection failed", err, nil)
		panic(err)
	}
	defer paymentConn.Close()

	repo := repository.New(db)
	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	topic := config.String("ORDER_PAID_TOPIC", "order.paid")
	if err := platformkafka.EnsureTopic(ctx, brokers[0], topic); err != nil {
		log.Error("kafka topic setup failed", err, map[string]any{"topic": topic})
		panic(err)
	}
	writer := platformkafka.NewWriter(brokers, topic)
	defer writer.Close()
	go outbox.New(repo, writer, log).Run(ctx)

	svc := orderservice.New(
		repo,
		clients.NewProductClient(productv1.NewProductServiceClient(productConn)),
		clients.NewPaymentClient(paymentv1.NewPaymentServiceClient(paymentConn)),
		log,
	)

	port := config.String("ORDER_GRPC_PORT", "50051")
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Error("listen failed", err, nil)
		panic(err)
	}

	server := grpc.NewServer()
	orderv1.RegisterOrderServiceServer(server, grpcapi.New(svc))
	go func() {
		<-ctx.Done()
		server.GracefulStop()
	}()

	log.Info("grpc server started", map[string]any{"addr": fmt.Sprintf(":%s", port)})
	if err := server.Serve(listener); err != nil {
		log.Error("grpc server stopped", err, nil)
	}
}
