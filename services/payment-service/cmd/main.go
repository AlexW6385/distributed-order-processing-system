package main

import (
	"time"

	paymentv1 "github.com/AlexW6385/distributed-order-processing-system/gen/payment/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/service"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/internal/transport/grpcapi"
	"github.com/AlexW6385/distributed-order-processing-system/services/payment-service/migrations"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	log := logging.New("payment-service")

	db := app.OpenDB(ctx, log, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/payments?sslmode=disable"), migrations.FS)
	defer db.Close()

	svc := service.New(repository.New(db), service.Config{
		DeclineAboveCents: int64(config.Int("PAYMENT_DECLINE_ABOVE_CENTS", 100_000)),
		FaultRate:         config.Float("PAYMENT_FAULT_RATE", 0),
		FaultDelay:        config.Duration("PAYMENT_FAULT_DELAY", 5*time.Second),
	})

	server := grpcx.NewServer(log)
	paymentv1.RegisterPaymentServiceServer(server, grpcapi.New(svc))
	app.ServeGRPC(ctx, log, server, config.String("PAYMENT_GRPC_PORT", "50053"))
}
