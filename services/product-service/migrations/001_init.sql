CREATE TABLE products (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT NOT NULL,
    price_cents BIGINT NOT NULL CHECK (price_cents > 0),
    -- Stock that can still be reserved. The CHECK makes overselling impossible
    -- even if application logic is wrong.
    stock INTEGER NOT NULL CHECK (stock >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per order. A row in status 'released' with no items is a tombstone:
-- the order was released before its reservation arrived, so a late
-- ReserveStock call for that order must be rejected.
CREATE TABLE reservations (
    order_id TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('reserved', 'confirmed', 'released')),
    total_cents BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE reservation_items (
    order_id TEXT NOT NULL REFERENCES reservations(order_id) ON DELETE CASCADE,
    product_id TEXT NOT NULL REFERENCES products(id),
    product_name TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price_cents BIGINT NOT NULL CHECK (unit_price_cents > 0),
    subtotal_cents BIGINT NOT NULL CHECK (subtotal_cents > 0),
    PRIMARY KEY (order_id, product_id)
);

INSERT INTO products (id, name, description, price_cents, stock) VALUES
('prod-coffee-001', 'Single Origin Coffee', '12 oz bag of medium roast beans', 1599, 50),
('prod-tea-002', 'Jasmine Green Tea', 'Loose leaf green tea with jasmine aroma', 1299, 40),
('prod-mug-003', 'Ceramic Mug', 'Matte white 12 oz mug', 1099, 80),
('prod-bottle-004', 'Steel Water Bottle', 'Insulated 20 oz bottle', 2499, 35),
('prod-notebook-005', 'Dot Grid Notebook', 'A5 notebook with 160 pages', 899, 100),
('prod-pen-006', 'Gel Pen Set', 'Six black gel pens', 699, 120),
('prod-keyboard-007', 'Compact Keyboard', 'Wireless 75 percent keyboard', 5999, 20),
('prod-mouse-008', 'Ergonomic Mouse', 'Bluetooth ergonomic mouse', 3499, 30),
('prod-lamp-009', 'Desk Lamp', 'LED lamp with warm and cool modes', 4299, 18),
('prod-stand-010', 'Laptop Stand', 'Aluminum adjustable stand', 3999, 25),
('prod-charger-011', 'USB-C Charger', '65W compact charger', 2999, 45),
('prod-cable-012', 'Braided USB-C Cable', '6 ft charging cable', 999, 90),
('prod-backpack-013', 'Daily Backpack', 'Minimal 20L commuter backpack', 7499, 15),
('prod-headphones-014', 'Wireless Headphones', 'Over-ear headphones with case', 8999, 12),
('prod-speaker-015', 'Portable Speaker', 'Water-resistant Bluetooth speaker', 4999, 22)
ON CONFLICT (id) DO NOTHING;
