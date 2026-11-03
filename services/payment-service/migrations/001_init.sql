-- One row per order: order_id is the idempotency key for charges. Declines are
-- stored too, so a retried charge gets the same answer instead of a new one.
CREATE TABLE payments (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE,
    idempotency_key TEXT NOT NULL,
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    status TEXT NOT NULL CHECK (status IN ('succeeded', 'declined')),
    decline_reason TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_payments_created ON payments (created_at, id);

-- Full refunds only, at most one per order.
CREATE TABLE refunds (
    id TEXT PRIMARY KEY,
    order_id TEXT NOT NULL UNIQUE REFERENCES payments (order_id),
    amount_cents BIGINT NOT NULL CHECK (amount_cents > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Double-entry ledger. Every money movement is a journal entry whose lines
-- sum to zero; amounts are signed (debit positive, credit negative).
CREATE TABLE ledger_accounts (
    code TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('asset', 'revenue'))
);

INSERT INTO ledger_accounts (code, kind) VALUES ('cash', 'asset'), ('revenue', 'revenue');

CREATE TABLE journal_entries (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('charge', 'refund')),
    order_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (kind, order_id)
);

CREATE TABLE journal_lines (
    entry_id TEXT NOT NULL REFERENCES journal_entries (id),
    account TEXT NOT NULL REFERENCES ledger_accounts (code),
    amount_cents BIGINT NOT NULL CHECK (amount_cents <> 0),
    PRIMARY KEY (entry_id, account)
);

-- Checked at commit: a transaction cannot leave an entry unbalanced, whatever
-- the application code does.
CREATE FUNCTION check_entry_balanced() RETURNS trigger AS $$
BEGIN
    IF (SELECT sum(amount_cents) FROM journal_lines WHERE entry_id = NEW.entry_id) <> 0 THEN
        RAISE EXCEPTION 'journal entry % is not balanced', NEW.entry_id;
    END IF;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE CONSTRAINT TRIGGER journal_entry_balanced
    AFTER INSERT OR UPDATE ON journal_lines
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION check_entry_balanced();
