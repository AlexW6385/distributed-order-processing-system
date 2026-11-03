package main

import (
	"context"
	"errors"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/app"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/notification-service/internal/inbox"
	"github.com/AlexW6385/distributed-order-processing-system/services/notification-service/migrations"
)

func main() {
	ctx, stop := app.SignalContext()
	defer stop()
	log := logging.New("notification-service")

	db := app.OpenDB(ctx, log, config.String("DATABASE_URL", "postgres://orders:orders@localhost:5432/notifications?sslmode=disable"), migrations.FS)
	defer db.Close()

	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	app.Must(log, "ensure kafka topic", platformkafka.EnsureTopic(ctx, brokers, events.Topic, config.Int("ORDER_EVENTS_PARTITIONS", 3)))
	app.Must(log, "ensure dlq topic", platformkafka.EnsureTopic(ctx, brokers, events.DeadLetterTopic, 1))
	reader := platformkafka.NewReader(brokers, events.Topic, config.String("KAFKA_GROUP_ID", "notification-service"))
	defer reader.Close()
	dlq := platformkafka.NewWriter(brokers, events.DeadLetterTopic)
	defer dlq.Close()

	log.Info("consumer started", nil)
	err := platformkafka.Consume(ctx, reader, inbox.New(db, log).Handler(), platformkafka.ConsumerConfig{DeadLetter: dlq}, log)
	if err != nil && !errors.Is(err, context.Canceled) {
		log.Error("consumer stopped", err, nil)
	}
}
