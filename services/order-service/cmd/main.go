package main

import (
	"time"

	orderv1 "github.com/AlexW6385/distributed-order-processing-system/gen/order/v1"
	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/clients"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/outbox"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/recovery"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/repository"
	orderservice "github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/transport/grpcapi"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/migrations"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	log := logging.New("order-service")

	db := app.OpenDB(ctx, log, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"), migrations.FS)
	defer db.Close()

	callTimeout := config.Duration("GRPC_CALL_TIMEOUT", 2*time.Second)
	productConn, err := grpcx.Dial(config.String("PRODUCT_GRPC_ADDR", "localhost:50052"), callTimeout)
	app.Must(log, "dial product-service", err)
	defer productConn.Close()
	paymentConn, err := grpcx.Dial(config.String("PAYMENT_GRPC_ADDR", "localhost:50053"), callTimeout)
	app.Must(log, "dial payment-service", err)
	defer paymentConn.Close()

	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	app.Must(log, "ensure kafka topic", platformkafka.EnsureTopic(ctx, brokers, events.Topic, config.Int("ORDER_EVENTS_PARTITIONS", 3)))
	writer := platformkafka.NewWriter(brokers, events.Topic)
	defer writer.Close()

	repo := repository.New(db)
	svc := orderservice.New(
		repo,
		clients.NewProductClient(productv1.NewProductServiceClient(productConn)),
		clients.NewPaymentClient(paymentv1.NewPaymentServiceClient(paymentConn)),
		orderservice.Config{PaymentWindow: config.Duration("ORDER_PAYMENT_WINDOW", 15*time.Minute)},
		log,
	)

	go outbox.New(repo, writer, outbox.Config{}, log).Run(ctx)
	go recovery.New(repo, svc, recovery.Config{
		Interval:          config.Duration("RECOVERY_INTERVAL", 5*time.Second),
		PendingTimeout:    config.Duration("RECOVERY_PENDING_TIMEOUT", time.Minute),
		PaymentRetryAfter: config.Duration("RECOVERY_PAYMENT_RETRY_AFTER", 10*time.Second),
	}, log).Run(ctx)

	server := grpcx.NewServer(log)
	orderv1.RegisterOrderServiceServer(server, grpcapi.New(svc))
	app.ServeGRPC(ctx, log, server, config.String("ORDER_GRPC_PORT", "50051"))
}
