package main

import (
	"context"
	"errors"
	"time"

	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/cache"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/service"
	eventsapi "github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/transport/events"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/transport/grpcapi"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/migrations"
	"github.com/go-redis/redis/v8"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	log := logging.New("product-service")

	db := app.OpenDB(ctx, log, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/products?sslmode=disable"), migrations.FS)
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()
	svc := service.New(repository.New(db), cache.NewRedis(rdb, config.Duration("PRODUCT_CACHE_TTL", 5*time.Second)), log)

	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	partitions := config.Int("ORDER_EVENTS_PARTITIONS", 3)
	app.Must(log, "ensure kafka topic", platformkafka.EnsureTopic(ctx, brokers, events.Topic, partitions))
	app.Must(log, "ensure dlq topic", platformkafka.EnsureTopic(ctx, brokers, events.DeadLetterTopic, 1))
	reader := platformkafka.NewReader(brokers, events.Topic, config.String("KAFKA_GROUP_ID", "product-service"))
	defer reader.Close()
	dlq := platformkafka.NewWriter(brokers, events.DeadLetterTopic)
	defer dlq.Close()

	go func() {
		err := platformkafka.Consume(ctx, reader, eventsapi.Handler(svc), platformkafka.ConsumerConfig{DeadLetter: dlq}, log)
		if err != nil && !errors.Is(err, context.Canceled) {
			log.Error("order event consumer stopped", err, nil)
			stop()
		}
	}()

	server := grpcx.NewServer(log)
	productv1.RegisterProductServiceServer(server, grpcapi.New(svc))
	app.ServeGRPC(ctx, log, server, config.String("PRODUCT_GRPC_PORT", "50052"))
}
