package inbox_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/AlexW6385/distributed-order-processing-system/internal/contracts/events"
	platformkafka "github.com/AlexW6385/distributed-order-processing-system/internal/platform/kafka"
	"github.com/AlexW6385/distributed-order-processing-system/internal/platform/testdb"
	"github.com/AlexW6385/distributed-order-processing-system/services/notification-service/internal/inbox"
	"github.com/AlexW6385/distributed-order-processing-system/services/notification-service/migrations"
	"github.com/segmentio/kafka-go"
)

func TestRedeliveredEventNotifiesOnce(t *testing.T) {
	db := testdb.New(t, migrations.FS)
	handle := inbox.New(db).Handler()

	payload, _ := json.Marshal(events.OrderEvent{
		EventID: "evt-1", EventType: events.OrderPaid, OrderID: "order-1",
		CustomerEmail: "alex@example.com", TotalCents: 1234, OccurredAt: time.Now(),
	})
	for i := 0; i < 3; i++ {
		if err := handle(context.Background(), kafka.Message{Value: payload}); err != nil {
			t.Fatal(err)
		}
	}
	var n int
	_ = db.QueryRow(context.Background(), `SELECT count(*) FROM notifications`).Scan(&n)
	if n != 1 {
		t.Fatalf("%d notifications, want 1", n)
	}
}

func TestUndecodableEventIsPermanent(t *testing.T) {
	db := testdb.New(t, migrations.FS)
	handle := inbox.New(db).Handler()
	err := handle(context.Background(), kafka.Message{Value: []byte("not json")})
	if !errors.Is(err, platformkafka.ErrPermanent) {
		t.Fatalf("err = %v, want ErrPermanent", err)
	}
}
