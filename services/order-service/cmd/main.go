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
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/clients"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/outbox"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/recovery"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/repository"
	orderservice "github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/transport/grpcapi"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/migrations"
)

func main() {
	ctx, done := app.Start("order-service")
	defer done()

	db := app.OpenDB(ctx, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/orders?sslmode=disable"), migrations.FS)
	defer db.Close()

	timeout := config.Duration("GRPC_CALL_TIMEOUT", 2*time.Second)
	breaker := func(name string) *grpcx.Breaker {
		return grpcx.NewBreaker(name, config.Int("BREAKER_THRESHOLD", 5), config.Duration("BREAKER_COOLDOWN", 5*time.Second))
	}
	productConn, err := grpcx.Dial(config.String("PRODUCT_GRPC_ADDR", "localhost:50052"), grpcx.ClientOptions{
		Timeout: timeout, Breaker: breaker("product-service"),
		RetryMethods: []string{productv1.ProductService_ReserveStock_FullMethodName},
	})
	app.Must("dial product-service", err)
	defer productConn.Close()
	paymentConn, err := grpcx.Dial(config.String("PAYMENT_GRPC_ADDR", "localhost:50053"), grpcx.ClientOptions{
		Timeout: timeout, Breaker: breaker("payment-service"),
		RetryMethods: []string{paymentv1.PaymentService_Charge_FullMethodName, paymentv1.PaymentService_Refund_FullMethodName},
	})
	app.Must("dial payment-service", err)
	defer paymentConn.Close()

	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	app.Must("ensure kafka topic", platformkafka.EnsureTopic(ctx, brokers, events.Topic, config.Int("ORDER_EVENTS_PARTITIONS", 3)))
	writer := platformkafka.NewWriter(brokers, events.Topic)
	defer writer.Close()

	repo := repository.New(db)
	svc := orderservice.New(repo,
		clients.NewProductClient(productv1.NewProductServiceClient(productConn)),
		clients.NewPaymentClient(paymentv1.NewPaymentServiceClient(paymentConn)),
		orderservice.Config{PaymentWindow: config.Duration("ORDER_PAYMENT_WINDOW", 15*time.Minute)},
	)

	go outbox.New(repo, writer, outbox.Config{
		Interval:        config.Duration("OUTBOX_INTERVAL", 100*time.Millisecond),
		BatchSize:       config.Int("OUTBOX_BATCH_SIZE", 100),
		NoDrain:         config.String("OUTBOX_DRAIN", "true") == "false",
		WritePerMessage: config.String("OUTBOX_BATCH_WRITES", "true") == "false",
	}).Run(ctx)
	go recovery.New(repo, svc, recovery.Config{
		Interval:          config.Duration("RECOVERY_INTERVAL", 5*time.Second),
		PendingTimeout:    config.Duration("RECOVERY_PENDING_TIMEOUT", time.Minute),
		PaymentRetryAfter: config.Duration("RECOVERY_PAYMENT_RETRY_AFTER", 10*time.Second),
	}).Run(ctx)

	server, health := grpcx.NewServer()
	orderv1.RegisterOrderServiceServer(server, grpcapi.New(svc))
	app.ServeGRPC(ctx, server, health, config.String("ORDER_GRPC_PORT", "50051"))
}
