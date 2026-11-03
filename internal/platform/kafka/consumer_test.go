package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

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
	if outcome, err := processWithRetry(context.Background(), kafka.Message{}, handle, cfg, "g"); err != nil || outcome != "ok" {
		t.Fatal(outcome, err)
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
	if outcome, _ := processWithRetry(context.Background(), kafka.Message{}, handle, cfg, "g"); outcome != "dead_lettered" {
		t.Fatal(outcome)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}
