-- Dimensional model: one fact at order-line grain, three conformed dimensions.
--
-- Surrogate keys throughout, because the facts must keep pointing at the version
-- of the dimension that was live when the event happened. A fact joined on the
-- business key silently restates itself every time an attribute changes.

CREATE TABLE IF NOT EXISTS dim_customer (
    sk              BIGINT PRIMARY KEY,
    customer_id     VARCHAR NOT NULL,      -- business key
    customer_name   VARCHAR,
    segment         VARCHAR,               -- tracked: drives segment reporting
    country         VARCHAR,               -- tracked: drives geography reporting
    credit_rating   VARCHAR,               -- tracked
    valid_from      TIMESTAMP NOT NULL,
    valid_to        TIMESTAMP NOT NULL,
    is_current      BOOLEAN NOT NULL
);

-- Type 1: a corrected product name is a correction, not history worth keeping.
CREATE TABLE IF NOT EXISTS dim_product (
    product_id      VARCHAR PRIMARY KEY,
    product_name    VARCHAR,
    category        VARCHAR,
    unit_cost       DECIMAL(18,4),
    updated_at      TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        INTEGER PRIMARY KEY,   -- yyyymmdd
    full_date       DATE NOT NULL,
    year            INTEGER,
    quarter         INTEGER,
    month           INTEGER,
    month_name      VARCHAR,
    day_of_month    INTEGER,
    day_of_week     INTEGER,
    day_name        VARCHAR,
    is_weekend      BOOLEAN
);

CREATE TABLE IF NOT EXISTS fact_order_line (
    order_line_id   VARCHAR PRIMARY KEY,   -- degenerate dimension; also the idempotency key
    order_id        VARCHAR NOT NULL,
    order_date_key  INTEGER NOT NULL,
    customer_sk     BIGINT NOT NULL,
    product_id      VARCHAR NOT NULL,
    quantity        INTEGER NOT NULL,
    unit_price      DECIMAL(18,4) NOT NULL,
    discount_pct    DECIMAL(9,4) NOT NULL DEFAULT 0,
    gross_amount    DECIMAL(18,4) NOT NULL,
    net_amount      DECIMAL(18,4) NOT NULL,
    source_updated_at TIMESTAMP NOT NULL,
    loaded_at       TIMESTAMP NOT NULL
);
