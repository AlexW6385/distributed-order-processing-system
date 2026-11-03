package consumer

import (
	"context"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
	logger *logging.Logger
}

func New(reader *kafka.Reader, logger *logging.Logger) *Consumer {
	return &Consumer{reader: reader, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			return err
		}
		c.logger.Info("notification event consumed", map[string]any{
			"topic": string(msg.Topic),
			"key":   string(msg.Key),
			"value": string(msg.Value),
		})
		if err := c.reader.CommitMessages(ctx, msg); err != nil {
			c.logger.Error("commit notification message failed", err, map[string]any{"key": string(msg.Key)})
		}
	}
}
