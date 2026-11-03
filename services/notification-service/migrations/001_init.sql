-- Inbox: IDs of events already handled. Kafka delivers at least once, so the
-- consumer records the event ID in the same transaction as its side effect and
-- skips any event it has seen.
CREATE TABLE processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stand-in for an email provider: each row is one message "sent".
CREATE TABLE notifications (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL UNIQUE REFERENCES processed_events(event_id),
    order_id TEXT NOT NULL,
    recipient TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_order_id ON notifications (order_id);
