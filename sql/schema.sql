-- ============================================================
-- E-Commerce Analytics Platform — Database Schema
-- Medallion Architecture: Bronze → Silver → Gold
-- ============================================================

-- ─────────────────────────────────────────
-- SCHEMAS
-- ─────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;


-- ─────────────────────────────────────────
-- BRONZE LAYER — Raw Events (no transformation)
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bronze.raw_events (
    id              BIGSERIAL PRIMARY KEY,
    event_id        VARCHAR(64) UNIQUE NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    raw_payload     JSONB NOT NULL,
    kafka_topic     VARCHAR(100),
    kafka_partition INT,
    kafka_offset    BIGINT,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_bronze_event_type ON bronze.raw_events(event_type);
CREATE INDEX IF NOT EXISTS idx_bronze_ingested_at ON bronze.raw_events(ingested_at);


-- ─────────────────────────────────────────
-- SILVER LAYER — Cleaned & Validated Records
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS silver.orders (
    order_id            VARCHAR(64) PRIMARY KEY,
    event_id            VARCHAR(64) REFERENCES bronze.raw_events(event_id),
    user_id             VARCHAR(64) NOT NULL,
    session_id          VARCHAR(64),
    product_id          VARCHAR(64) NOT NULL,
    product_category    VARCHAR(100),
    quantity            INT CHECK (quantity > 0),
    unit_price          DECIMAL(10,2) CHECK (unit_price >= 0),
    total_amount        DECIMAL(12,2) CHECK (total_amount >= 0),
    payment_method      VARCHAR(50),
    city                VARCHAR(100),
    state               VARCHAR(100),
    device              VARCHAR(50),
    order_status        VARCHAR(50) DEFAULT 'placed',
    event_time          TIMESTAMP NOT NULL,
    processed_at        TIMESTAMP DEFAULT NOW(),
    is_valid            BOOLEAN DEFAULT TRUE
);

CREATE INDEX IF NOT EXISTS idx_silver_user_id    ON silver.orders(user_id);
CREATE INDEX IF NOT EXISTS idx_silver_event_time ON silver.orders(event_time);
CREATE INDEX IF NOT EXISTS idx_silver_status     ON silver.orders(order_status);
CREATE INDEX IF NOT EXISTS idx_silver_category   ON silver.orders(product_category);

CREATE TABLE IF NOT EXISTS silver.payments (
    payment_id          VARCHAR(64) PRIMARY KEY,
    order_id            VARCHAR(64),
    user_id             VARCHAR(64) NOT NULL,
    amount              DECIMAL(12,2) CHECK (amount >= 0),
    payment_method      VARCHAR(50),
    payment_status      VARCHAR(50),   -- success / failed / pending
    failure_reason      VARCHAR(200),
    event_time          TIMESTAMP NOT NULL,
    processed_at        TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS silver.deliveries (
    delivery_id         VARCHAR(64) PRIMARY KEY,
    order_id            VARCHAR(64),
    user_id             VARCHAR(64) NOT NULL,
    delivery_status     VARCHAR(50),   -- shipped / delivered / cancelled
    city                VARCHAR(100),
    estimated_days      INT,
    actual_days         INT,
    is_delayed          BOOLEAN GENERATED ALWAYS AS (actual_days > estimated_days) STORED,
    event_time          TIMESTAMP NOT NULL,
    processed_at        TIMESTAMP DEFAULT NOW()
);


-- ─────────────────────────────────────────
-- GOLD LAYER — Business KPIs (Dashboard Ready)
-- ─────────────────────────────────────────

-- Hourly revenue by category
CREATE TABLE IF NOT EXISTS gold.hourly_revenue (
    id                  BIGSERIAL PRIMARY KEY,
    hour_bucket         TIMESTAMP NOT NULL,
    product_category    VARCHAR(100),
    total_revenue       DECIMAL(14,2) DEFAULT 0,
    order_count         INT DEFAULT 0,
    avg_order_value     DECIMAL(10,2) DEFAULT 0,
    refreshed_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (hour_bucket, product_category)
);

-- Daily revenue summary
CREATE TABLE IF NOT EXISTS gold.daily_revenue (
    date_bucket         DATE PRIMARY KEY,
    total_revenue       DECIMAL(14,2) DEFAULT 0,
    order_count         INT DEFAULT 0,
    unique_customers    INT DEFAULT 0,
    avg_order_value     DECIMAL(10,2) DEFAULT 0,
    top_category        VARCHAR(100),
    refreshed_at        TIMESTAMP DEFAULT NOW()
);

-- Revenue by city
CREATE TABLE IF NOT EXISTS gold.revenue_by_city (
    id                  BIGSERIAL PRIMARY KEY,
    date_bucket         DATE NOT NULL,
    city                VARCHAR(100) NOT NULL,
    total_revenue       DECIMAL(14,2) DEFAULT 0,
    order_count         INT DEFAULT 0,
    refreshed_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (date_bucket, city)
);

-- Payment failure metrics
CREATE TABLE IF NOT EXISTS gold.payment_failure_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    hour_bucket         TIMESTAMP NOT NULL,
    payment_method      VARCHAR(50),
    total_attempts      INT DEFAULT 0,
    failed_count        INT DEFAULT 0,
    failure_rate        DECIMAL(5,2) DEFAULT 0,
    refreshed_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (hour_bucket, payment_method)
);

-- Customer lifetime metrics (for churn analysis)
CREATE TABLE IF NOT EXISTS gold.customer_metrics (
    user_id                 VARCHAR(64) PRIMARY KEY,
    total_orders            INT DEFAULT 0,
    total_spend             DECIMAL(14,2) DEFAULT 0,
    avg_order_value         DECIMAL(10,2) DEFAULT 0,
    first_order_date        DATE,
    last_order_date         DATE,
    days_since_last_order   INT,
    favourite_category      VARCHAR(100),
    favourite_payment       VARCHAR(50),
    churn_risk_flag         BOOLEAN DEFAULT FALSE,  -- TRUE if inactive 30+ days
    refreshed_at            TIMESTAMP DEFAULT NOW()
);

-- Delayed shipment tracker
CREATE TABLE IF NOT EXISTS gold.delayed_shipments (
    id                  BIGSERIAL PRIMARY KEY,
    date_bucket         DATE NOT NULL,
    city                VARCHAR(100),
    delayed_count       INT DEFAULT 0,
    total_shipments     INT DEFAULT 0,
    delay_rate          DECIMAL(5,2) DEFAULT 0,
    refreshed_at        TIMESTAMP DEFAULT NOW(),
    UNIQUE (date_bucket, city)
);

-- ─────────────────────────────────────────
-- DATA QUALITY LOG
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.dq_run_log (
    id              BIGSERIAL PRIMARY KEY,
    run_id          VARCHAR(64),
    layer           VARCHAR(20),   -- bronze / silver / gold
    table_name      VARCHAR(100),
    check_name      VARCHAR(200),
    status          VARCHAR(20),   -- passed / failed / warning
    records_checked INT,
    records_failed  INT,
    run_at          TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────
-- SEED: Product catalogue
-- ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS public.products (
    product_id      VARCHAR(64) PRIMARY KEY,
    product_name    VARCHAR(200),
    category        VARCHAR(100),
    base_price      DECIMAL(10,2)
);

INSERT INTO public.products VALUES
    ('PROD-001', 'Samsung Galaxy S24',       'Electronics',   55000.00),
    ('PROD-002', 'Apple AirPods Pro',         'Electronics',   22000.00),
    ('PROD-003', 'Nike Air Max 270',          'Footwear',       8500.00),
    ('PROD-004', 'Adidas Running T-Shirt',    'Clothing',       1299.00),
    ('PROD-005', 'Prestige Pressure Cooker',  'Kitchen',        2500.00),
    ('PROD-006', 'boAt Rockerz 450',          'Electronics',    1999.00),
    ('PROD-007', 'Levi Jeans 511',            'Clothing',       3499.00),
    ('PROD-008', 'Instant Pot Duo',           'Kitchen',        7500.00),
    ('PROD-009', 'Puma Sports Shoes',         'Footwear',       4500.00),
    ('PROD-010', 'Himalaya Face Wash',        'Beauty',          299.00),
    ('PROD-011', 'Kindle Paperwhite',         'Electronics',   14000.00),
    ('PROD-012', 'Wildcraft Backpack 45L',    'Accessories',    3200.00)
ON CONFLICT DO NOTHING;
