package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/segmentio/kafka-go"
)

func TestTransientErrorsAreRetried(t *testing.T) {
	calls := 0
	handle := func(ctx context.Context, msg kafka.Message) error {
		calls++
		if calls < 3 {
			return errors.New("database busy")
		}
		return nil
	}
	cfg := ConsumerConfig{MaxAttempts: 5, Backoff: time.Millisecond}
	if err := processWithRetry(context.Background(), kafka.Message{}, handle, cfg, logging.New("test")); err != nil {
		t.Fatal(err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3", calls)
	}
}

func TestPermanentErrorsAreNotRetried(t *testing.T) {
	calls := 0
	handle := func(ctx context.Context, msg kafka.Message) error {
		calls++
		return fmt.Errorf("bad payload: %w", ErrPermanent)
	}
	cfg := ConsumerConfig{MaxAttempts: 5, Backoff: time.Millisecond}
	if err := processWithRetry(context.Background(), kafka.Message{}, handle, cfg, logging.New("test")); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
