package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/config"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/services/notification-service/internal/consumer"
	"github.com/segmentio/kafka-go"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log := logging.New("notification-service")
	brokers := config.CSV("KAFKA_BROKERS", "localhost:9094")
	topic := config.String("ORDER_PAID_TOPIC", "order.paid")
	if err := platformkafka.EnsureTopic(ctx, brokers[0], topic); err != nil {
		log.Error("kafka topic setup failed", err, map[string]any{"topic": topic})
		panic(err)
	}
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     config.String("NOTIFICATION_GROUP_ID", "notification-service"),
		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	log.Info("consumer started", nil)
	if err := consumer.New(reader, log).Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("consumer stopped", err, nil)
	}
}
