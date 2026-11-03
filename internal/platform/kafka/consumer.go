package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/segmentio/kafka-go"
)

// ErrPermanent marks a handler error that retrying cannot fix, such as a
// payload that does not decode. Wrap it with fmt.Errorf("...: %w", ErrPermanent).
var ErrPermanent = errors.New("permanent failure")

// Handler processes one message. Handlers must be idempotent: delivery is
// at-least-once, so the same message can arrive more than once.
type Handler func(ctx context.Context, msg kafka.Message) error

// ConsumerConfig controls retry behaviour for Consume.
type ConsumerConfig struct {
	MaxAttempts int           // attempts before a message goes to the dead-letter topic
	Backoff     time.Duration // initial delay between attempts, doubled each time
	DeadLetter  *kafka.Writer // optional; nil means failed messages are only logged
}

// Consume reads messages one at a time and commits the offset only after the
// handler succeeds or the message has been dead-lettered. A failing message
// blocks its partition while it is retried, which preserves per-key ordering.
func Consume(ctx context.Context, reader *kafka.Reader, handle Handler, cfg ConsumerConfig, log *logging.Logger) error {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 200 * time.Millisecond
	}
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Group rebalances and broker restarts surface here; the reader
			// rejoins the group on the next fetch.
			log.Error("fetch message failed, retrying", err, nil)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		if err := processWithRetry(ctx, msg, handle, cfg, log); err != nil {
			return err
		}
		if err := reader.CommitMessages(ctx, msg); err != nil {
			// The message will be redelivered; handlers are idempotent.
			log.Error("commit offset failed", err, fields(msg))
		}
	}
}

func processWithRetry(ctx context.Context, msg kafka.Message, handle Handler, cfg ConsumerConfig, log *logging.Logger) error {
	delay := cfg.Backoff
	var err error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		err = handle(ctx, msg)
		if err == nil {
			return nil
		}
		f := fields(msg)
		f["attempt"] = attempt
		log.Error("handle message failed", err, f)
		if errors.Is(err, ErrPermanent) {
			break
		}
		if attempt < cfg.MaxAttempts {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
	}
	return deadLetter(ctx, msg, err, cfg, log)
}

func deadLetter(ctx context.Context, msg kafka.Message, cause error, cfg ConsumerConfig, log *logging.Logger) error {
	if cfg.DeadLetter == nil {
		log.Error("dropping message after retries", cause, fields(msg))
		return nil
	}
	headers := append([]kafka.Header{}, msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "dlq_error", Value: []byte(cause.Error())},
		kafka.Header{Key: "dlq_source_topic", Value: []byte(msg.Topic)},
	)
	if err := cfg.DeadLetter.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value, Headers: headers}); err != nil {
		// Do not commit: the message stays on the source topic and is retried.
		return err
	}
	log.Error("message moved to dead-letter topic", cause, fields(msg))
	return nil
}

func fields(msg kafka.Message) map[string]any {
	return map[string]any{
		"topic":     msg.Topic,
		"partition": msg.Partition,
		"offset":    msg.Offset,
		"key":       string(msg.Key),
	}
}

// Header returns the value of the named header, or "".
func Header(msg kafka.Message, key string) string {
	for _, h := range msg.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// NewReader returns a consumer-group reader that starts from the earliest
// offset when the group has no committed position yet.
func NewReader(brokers []string, topic, groupID string) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset,
		MinBytes:       1,
		MaxBytes:       1 << 20,
		MaxWait:        500 * time.Millisecond,
		CommitInterval: 0, // synchronous commits
	})
}
