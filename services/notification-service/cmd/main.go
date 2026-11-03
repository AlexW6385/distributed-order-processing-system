package main

import (
	"context"
	"errors"
	"log/slog"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/services/notification-service/internal/inbox"
	"github.com/AlexW6385/distributed-order-processing-system/services/notification-service/migrations"
)

func main() {
	ctx, done := app.Start("notification-service")
	defer done()

	db := app.OpenDB(ctx, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/notifications?sslmode=disable"), migrations.FS)
	defer db.Close()

	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	app.Must("ensure kafka topic", platformkafka.EnsureTopic(ctx, brokers, events.Topic, config.Int("ORDER_EVENTS_PARTITIONS", 3)))
	app.Must("ensure dlq topic", platformkafka.EnsureTopic(ctx, brokers, events.DeadLetterTopic, 1))
	reader := platformkafka.NewReader(brokers, events.Topic, config.String("KAFKA_GROUP_ID", "notification-service"))
	defer reader.Close()
	dlq := platformkafka.NewWriter(brokers, events.DeadLetterTopic)
	defer dlq.Close()

	slog.Info("consumer started")
	err := platformkafka.Consume(ctx, reader, inbox.New(db).Handler(), platformkafka.ConsumerConfig{DeadLetter: dlq})
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("consumer stopped", "error", err)
	}
}
