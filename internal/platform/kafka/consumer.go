package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/obs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
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

var (
	consumed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_messages_consumed_total",
		Help: "Messages handled by a consumer group, by outcome (ok, dead_lettered).",
	}, []string{"group", "outcome"})
	handleFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "kafka_handler_failures_total",
		Help: "Failed handler attempts, including ones that were retried.",
	}, []string{"group"})
	consumerLag = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kafka_consumer_lag",
		Help: "Messages between the consumer's position and the end of the partition.",
	}, []string{"group"})
)

// Consume reads messages one at a time and commits the offset only after the
// handler succeeds or the message has been dead-lettered. A failing message
// blocks its partition while it is retried, which preserves per-key ordering.
// Each message is handled in a span that continues the producer's trace.
func Consume(ctx context.Context, reader *kafka.Reader, handle Handler, cfg ConsumerConfig) error {
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = 200 * time.Millisecond
	}
	group := reader.Config().GroupID
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// Group rebalances and broker restarts surface here; the reader
			// rejoins the group on the next fetch.
			slog.Error("fetch message failed, retrying", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
			continue
		}
		consumerLag.WithLabelValues(group).Set(float64(reader.Stats().Lag))

		msgCtx := otel.GetTextMapPropagator().Extract(ctx, headerCarrier(msg.Headers))
		msgCtx, span := obs.Tracer().Start(msgCtx, "consume "+msg.Topic, trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(attribute.String("messaging.kafka.consumer.group", group),
				attribute.String("messaging.kafka.message.key", string(msg.Key)),
				attribute.String("event.type", Header(msg, "event_type"))))
		outcome, err := processWithRetry(msgCtx, msg, handle, cfg, group)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
		if err != nil && outcome == "" {
			return err // could not dead-letter; leave uncommitted
		}
		consumed.WithLabelValues(group, outcome).Inc()
		if err := reader.CommitMessages(ctx, msg); err != nil {
			// The message will be redelivered; handlers are idempotent.
			slog.ErrorContext(msgCtx, "commit offset failed", "error", err)
		}
	}
}

func processWithRetry(ctx context.Context, msg kafka.Message, handle Handler, cfg ConsumerConfig, group string) (string, error) {
	delay := cfg.Backoff
	var err error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		if err = handle(ctx, msg); err == nil {
			return "ok", nil
		}
		handleFailures.WithLabelValues(group).Inc()
		slog.ErrorContext(ctx, "handle message failed", "error", err, "attempt", attempt, "key", string(msg.Key))
		if errors.Is(err, ErrPermanent) {
			break
		}
		if attempt < cfg.MaxAttempts {
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}
	}
	if dlqErr := deadLetter(ctx, msg, err, cfg); dlqErr != nil {
		return "", dlqErr
	}
	return "dead_lettered", err
}

func deadLetter(ctx context.Context, msg kafka.Message, cause error, cfg ConsumerConfig) error {
	if cfg.DeadLetter == nil {
		slog.ErrorContext(ctx, "dropping message after retries", "error", cause, "key", string(msg.Key))
		return nil
	}
	headers := append([]kafka.Header{}, msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: "dlq_error", Value: []byte(cause.Error())},
		kafka.Header{Key: "dlq_source_topic", Value: []byte(msg.Topic)},
	)
	return cfg.DeadLetter.WriteMessages(ctx, kafka.Message{Key: msg.Key, Value: msg.Value, Headers: headers})
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

// headerCarrier lets the OpenTelemetry propagator read trace context from
// Kafka headers.
type headerCarrier []kafka.Header

var _ propagation.TextMapCarrier = headerCarrier(nil)

func (c headerCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(string, string) {}

func (c headerCarrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
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
