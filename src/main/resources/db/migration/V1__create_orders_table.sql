CREATE TABLE IF NOT EXISTS orders (
    order_id       VARCHAR(36)    NOT NULL,
    customer_id    VARCHAR(100)   NOT NULL,
    product_id     VARCHAR(100)   NOT NULL,
    quantity       INTEGER        NOT NULL CHECK (quantity > 0),
    total_amount   NUMERIC(12, 2) NOT NULL CHECK (total_amount > 0),
    status         VARCHAR(30)    NOT NULL DEFAULT 'PENDING',
    failure_reason VARCHAR(500),
    retry_count    INTEGER        NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
    version        BIGINT         NOT NULL DEFAULT 0,

    CONSTRAINT pk_orders PRIMARY KEY (order_id)
);

CREATE INDEX IF NOT EXISTS idx_order_status    ON orders (status);
CREATE INDEX IF NOT EXISTS idx_order_customer  ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_order_created   ON orders (created_at DESC);

COMMENT ON TABLE orders IS 'Core order entity for the event-driven pipeline';
