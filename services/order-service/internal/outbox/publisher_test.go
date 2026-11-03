package outbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/logging"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/testdb"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/domain"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/outbox"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/internal/repository"
	"github.com/AlexW6385/distributed-order-processing-system/services/order-service/migrations"
	"github.com/segmentio/kafka-go"
)

type recordingWriter struct {
	mu   sync.Mutex
	msgs []kafka.Message
	fail error
}

func (w *recordingWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.fail != nil {
		return w.fail
	}
	w.msgs = append(w.msgs, msgs...)
	return nil
}

func seed(t *testing.T, repo *repository.Repository, n int) {
	ctx := context.Background()
	for i := 0; i < n; i++ {
		order, _, err := repo.CreatePending(ctx, fmt.Sprintf("u%d@example.com", i), "")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := repo.Close(ctx, order.ID, []string{domain.StatusPending}, domain.StatusFailed, "test"); err != nil {
			t.Fatal(err)
		}
	}
}

// Several publisher replicas draining one outbox must not publish an event
// twice: FOR UPDATE SKIP LOCKED hands each row to exactly one of them.
func TestConcurrentPublishersDeliverEachEventOnce(t *testing.T) {
	db := testdb.New(t, migrations.FS)
	repo := repository.New(db)
	seed(t, repo, 200)

	writer := &recordingWriter{}
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		p := outbox.New(repo, writer, outbox.Config{BatchSize: 7}, logging.New("test"))
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				n, err := p.PublishOnce(context.Background())
				if err != nil {
					t.Error(err)
					return
				}
				if n == 0 {
					return
				}
			}
		}()
	}
	wg.Wait()

	seen := map[string]int{}
	for _, m := range writer.msgs {
		for _, h := range m.Headers {
			if h.Key == "event_id" {
				seen[string(h.Value)]++
			}
		}
	}
	if len(seen) != 200 {
		t.Fatalf("published %d distinct events, want 200", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("event %s published %d times", id, n)
		}
	}
	var pending int
	_ = db.QueryRow(context.Background(), `SELECT count(*) FROM outbox_events WHERE status <> 'published'`).Scan(&pending)
	if pending != 0 {
		t.Fatalf("%d events left unpublished", pending)
	}
}

func TestFailedPublishIsRetriedLater(t *testing.T) {
	db := testdb.New(t, migrations.FS)
	repo := repository.New(db)
	seed(t, repo, 3)

	writer := &recordingWriter{fail: errors.New("broker down")}
	p := outbox.New(repo, writer, outbox.Config{}, logging.New("test"))
	if _, err := p.PublishOnce(context.Background()); err == nil {
		t.Fatal("expected publish error")
	}

	var attempts, pending int
	_ = db.QueryRow(context.Background(), `
		SELECT min(attempts), count(*) FILTER (WHERE status = 'pending' AND next_attempt_at > now())
		FROM outbox_events`).Scan(&attempts, &pending)
	if attempts != 1 || pending != 3 {
		t.Fatalf("attempts=%d backed-off=%d, want 1 and 3", attempts, pending)
	}

	// Backed-off events are not picked up again immediately.
	writer.fail = nil
	if n, _ := p.PublishOnce(context.Background()); n != 0 {
		t.Fatalf("published %d events during backoff, want 0", n)
	}
	_, _ = db.Exec(context.Background(), `UPDATE outbox_events SET next_attempt_at = now()`)
	if n, err := p.PublishOnce(context.Background()); err != nil || n != 3 {
		t.Fatalf("after backoff: n=%d err=%v, want 3", n, err)
	}
}

func TestMessagesAreKeyedByOrder(t *testing.T) {
	db := testdb.New(t, migrations.FS)
	repo := repository.New(db)
	seed(t, repo, 1)
	writer := &recordingWriter{}
	if _, err := outbox.New(repo, writer, outbox.Config{}, logging.New("test")).PublishOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	var orderID string
	_ = db.QueryRow(context.Background(), `SELECT id FROM orders`).Scan(&orderID)
	if string(writer.msgs[0].Key) != orderID {
		t.Fatalf("key = %q, want order id %q", writer.msgs[0].Key, orderID)
	}
}
