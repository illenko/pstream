-- Downstream schema: same columns as the source, but a plain table with
-- payment_id as the primary key (the Kafka key).

CREATE TABLE payments (
    tenant_id         text          NOT NULL,
    payment_id        uuid          NOT NULL PRIMARY KEY,
    order_id          text          NOT NULL,
    merchant_id       text          NOT NULL,
    amount            numeric(18,2) NOT NULL,
    currency          char(3)       NOT NULL,
    fee_amount        numeric(18,2),
    method            text          NOT NULL,
    status            text          NOT NULL,
    description       text,
    auth_code         text,
    error_description text,
    created_at        timestamptz   NOT NULL,
    finalized_at      timestamptz,
    updated_at        timestamptz   NOT NULL
);
