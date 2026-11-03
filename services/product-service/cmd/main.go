package main

import (
	"context"
	"errors"
	"log/slog"
	"time"

	productv1 "github.com/AlexW6385/distributed-order-processing-system/gen/product/v1"
	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/grpcx"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/cache"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/flashsale"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/service"
	eventsapi "github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/transport/events"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/internal/transport/grpcapi"
	"github.com/AlexW6385/distributed-order-processing-system/services/product-service/migrations"
	"github.com/go-redis/redis/v8"
)

func main() {
	ctx, done := app.Start("product-service")
	defer done()

	db := app.OpenDB(ctx, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/products?sslmode=disable"), migrations.FS)
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: config.String("REDIS_ADDR", "localhost:6379")})
	defer rdb.Close()

	// PRODUCT_CACHE_TTL=0 turns the catalog cache off.
	var catalog service.Cache
	if ttl := config.Duration("PRODUCT_CACHE_TTL", 5*time.Second); ttl > 0 {
		catalog = cache.NewRedis(rdb, ttl)
	}
	svc := service.New(repository.New(db), catalog, flashsale.New(rdb))

	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	app.Must("ensure kafka topic", platformkafka.EnsureTopic(ctx, brokers, events.Topic, config.Int("ORDER_EVENTS_PARTITIONS", 3)))
	app.Must("ensure dlq topic", platformkafka.EnsureTopic(ctx, brokers, events.DeadLetterTopic, 1))
	reader := platformkafka.NewReader(brokers, events.Topic, config.String("KAFKA_GROUP_ID", "product-service"))
	defer reader.Close()
	dlq := platformkafka.NewWriter(brokers, events.DeadLetterTopic)
	defer dlq.Close()
	go func() {
		err := platformkafka.Consume(ctx, reader, eventsapi.Handler(svc), platformkafka.ConsumerConfig{DeadLetter: dlq})
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("order event consumer stopped", "error", err)
		}
	}()

	server, health := grpcx.NewServer()
	productv1.RegisterProductServiceServer(server, grpcapi.New(svc))
	app.ServeGRPC(ctx, server, health, config.String("PRODUCT_GRPC_PORT", "50052"))
}
