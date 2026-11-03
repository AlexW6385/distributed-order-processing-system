package kafka

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// NewWriter returns a synchronous writer that hashes message keys to
// partitions, so messages with the same key keep their relative order, and
// waits for all in-sync replicas before reporting success.
func NewWriter(brokers []string, topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
	}
}

// EnsureTopic creates topic if it does not exist. It retries for a while
// because the broker may still be starting when a service boots.
func EnsureTopic(ctx context.Context, brokers []string, topic string, partitions int) error {
	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		lastErr = createTopic(ctx, brokers[0], topic, partitions)
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
	return lastErr
}

func createTopic(ctx context.Context, broker, topic string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", broker)
	if err != nil {
		return err
	}
	defer conn.Close()

	// Topic creation must go to the controller broker.
	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	ctrlConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer ctrlConn.Close()

	err = ctrlConn.CreateTopics(kafka.TopicConfig{
		Topic:             topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
	if err != nil && !errors.Is(err, kafka.TopicAlreadyExists) {
		return err
	}
	return nil
}
