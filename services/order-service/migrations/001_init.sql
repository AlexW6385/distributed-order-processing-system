CREATE TABLE orders (
    id TEXT PRIMARY KEY,
    customer_email TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN (
        'pending', 'stock_reserved', 'payment_pending', 'paid', 'failed', 'expired', 'cancelled'
    )),
    total_cents BIGINT NOT NULL DEFAULT 0 CHECK (total_cents >= 0),
    failure_reason TEXT NOT NULL DEFAULT '',
    -- Client-supplied key for CreateOrder; replays return the same order.
    create_key TEXT UNIQUE,
    -- Client-supplied key for PayOrder, passed through to payment-service.
    payment_key TEXT NOT NULL DEFAULT '',
    -- Payment deadline for a reserved order.
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Background workers scan non-terminal orders by status and age.
CREATE INDEX idx_orders_open_status
    ON orders (status, updated_at)
    WHERE status IN ('pending', 'stock_reserved', 'payment_pending');

CREATE TABLE order_items (
    order_id TEXT NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    product_id TEXT NOT NULL,
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents > 0),
    subtotal_cents BIGINT NOT NULL CHECK (subtotal_cents > 0),
    PRIMARY KEY (order_id, product_id)
);

CREATE TABLE outbox_events (
    id TEXT PRIMARY KEY,
    aggregate_type TEXT NOT NULL,
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'published', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT NOT NULL DEFAULT '',
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
);

CREATE INDEX idx_outbox_events_pending
    ON outbox_events (next_attempt_at)
    WHERE status = 'pending';
