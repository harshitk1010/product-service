-- =============================================================================
-- Star Schema — ANALYTICS_DB
-- Dimensions with SCD Type 2, Fact tables for transactional analysis
-- =============================================================================

USE DATABASE ANALYTICS_DB;
USE SCHEMA MARTS;
USE WAREHOUSE ANALYTICS_TRANSFORM_WH;


-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- ── dim_date ─────────────────────────────────────────────────────────────────
-- Populated by dbt seed + macro; never loaded from events
CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INT         NOT NULL PRIMARY KEY,   -- YYYYMMDD
    full_date           DATE        NOT NULL,
    year                SMALLINT    NOT NULL,
    quarter             SMALLINT    NOT NULL,
    month               SMALLINT    NOT NULL,
    month_name          VARCHAR(10) NOT NULL,
    week_of_year        SMALLINT    NOT NULL,
    day_of_week         SMALLINT    NOT NULL,
    day_name            VARCHAR(10) NOT NULL,
    is_weekend          BOOLEAN     NOT NULL,
    is_holiday          BOOLEAN     DEFAULT FALSE,
    fiscal_quarter      SMALLINT,
    fiscal_year         SMALLINT
);


-- ── dim_user — SCD Type 2 ─────────────────────────────────────────────────────
-- Tracks historical changes to user attributes.
-- When a user changes email/address, a new row is inserted with new
-- dw_effective_from and the old row's dw_effective_to is updated.
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk             INT         NOT NULL AUTOINCREMENT PRIMARY KEY,  -- Surrogate key
    user_id             VARCHAR(32) NOT NULL,                            -- Natural key
    email               VARCHAR(255),
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    full_name           VARCHAR(200),
    date_of_birth       DATE,
    age_bucket          VARCHAR(20),
    gender              VARCHAR(10),
    country             VARCHAR(3),
    city                VARCHAR(100),
    referral_source     VARCHAR(100),
    marketing_opt_in    BOOLEAN,
    customer_segment    VARCHAR(50),   -- 'new', 'returning', 'vip', 'churned'
    -- SCD Type 2 audit columns
    dw_effective_from   TIMESTAMP_NTZ NOT NULL,
    dw_effective_to     TIMESTAMP_NTZ,            -- NULL = current record
    dw_is_current       BOOLEAN       NOT NULL DEFAULT TRUE,
    dw_created_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    dw_updated_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (user_id, dw_is_current);


-- ── dim_product — SCD Type 2 ──────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_product (
    product_sk          INT         NOT NULL AUTOINCREMENT PRIMARY KEY,
    product_id          VARCHAR(20) NOT NULL,
    product_name        VARCHAR(500),
    category            VARCHAR(100),
    subcategory         VARCHAR(100),
    brand               VARCHAR(200),
    price_tier          VARCHAR(20),   -- 'budget' <25, 'mid' 25-100, 'premium' >100
    is_active           BOOLEAN     DEFAULT TRUE,
    -- SCD Type 2
    dw_effective_from   TIMESTAMP_NTZ NOT NULL,
    dw_effective_to     TIMESTAMP_NTZ,
    dw_is_current       BOOLEAN     NOT NULL DEFAULT TRUE,
    dw_created_at       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (product_id, dw_is_current);


-- ── dim_geography ─────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_geography (
    geo_sk          INT         NOT NULL AUTOINCREMENT PRIMARY KEY,
    country_code    VARCHAR(3)  NOT NULL,
    country_name    VARCHAR(100),
    region          VARCHAR(50),
    subregion       VARCHAR(50),
    currency_code   VARCHAR(3),
    timezone        VARCHAR(50)
);


-- ── dim_payment_method ────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_payment_method (
    payment_sk          INT         NOT NULL AUTOINCREMENT PRIMARY KEY,
    payment_method      VARCHAR(50) NOT NULL,
    payment_category    VARCHAR(30),  -- 'card', 'digital_wallet', 'bnpl', 'crypto'
    is_digital          BOOLEAN
);


-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- ── fact_orders ──────────────────────────────────────────────────────────────
-- Grain: one row per order
CREATE TABLE IF NOT EXISTS fact_orders (
    order_sk            BIGINT      NOT NULL AUTOINCREMENT PRIMARY KEY,
    -- Foreign keys
    order_id            VARCHAR(50) NOT NULL,
    user_sk             INT         REFERENCES dim_user(user_sk),
    date_sk             INT         REFERENCES dim_date(date_key),
    geo_sk              INT         REFERENCES dim_geography(geo_sk),
    payment_sk          INT         REFERENCES dim_payment_method(payment_sk),
    -- Degenerate dimensions
    session_id          VARCHAR(64),
    device_type         VARCHAR(20),
    coupon_code         VARCHAR(50),
    -- Measures
    item_count          SMALLINT,
    subtotal            NUMBER(12,2),
    tax_amount          NUMBER(10,2),
    shipping_amount     NUMBER(10,2),
    discount_amount     NUMBER(10,2),
    total_amount        NUMBER(12,2),
    -- Derived measures
    gross_margin_pct    NUMBER(5,2),
    -- Flags
    is_first_order      BOOLEAN,
    has_coupon          BOOLEAN,
    -- Timestamps
    order_timestamp     TIMESTAMP_NTZ NOT NULL,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(order_timestamp), geo_sk);


-- ── fact_order_items ──────────────────────────────────────────────────────────
-- Grain: one row per order line item
CREATE TABLE IF NOT EXISTS fact_order_items (
    order_item_sk       BIGINT  NOT NULL AUTOINCREMENT PRIMARY KEY,
    order_sk            BIGINT  REFERENCES fact_orders(order_sk),
    product_sk          INT     REFERENCES dim_product(product_sk),
    user_sk             INT     REFERENCES dim_user(user_sk),
    date_sk             INT     REFERENCES dim_date(date_key),
    -- Measures
    quantity            SMALLINT,
    unit_price          NUMBER(10,2),
    discount_amount     NUMBER(10,2),
    line_total          NUMBER(12,2),
    -- Timestamps
    order_timestamp     TIMESTAMP_NTZ NOT NULL,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(order_timestamp));


-- ── fact_events (behavioral) ─────────────────────────────────────────────────
-- Grain: one row per user event (views, searches, cart actions)
-- Partitioned aggressively on date to keep query costs low
CREATE TABLE IF NOT EXISTS fact_events (
    event_sk            BIGINT      NOT NULL AUTOINCREMENT PRIMARY KEY,
    event_id            VARCHAR(36) NOT NULL,
    event_type          VARCHAR(50) NOT NULL,
    -- Foreign keys
    user_sk             INT         REFERENCES dim_user(user_sk),
    product_sk          INT,        -- Nullable (not all events have a product)
    date_sk             INT         REFERENCES dim_date(date_key),
    geo_sk              INT         REFERENCES dim_geography(geo_sk),
    -- Degenerate dimensions
    session_id          VARCHAR(64),
    device_type         VARCHAR(20),
    -- Optional measures
    price               NUMBER(10,2),
    view_duration_sec   INT,
    search_results_count INT,
    cart_total          NUMBER(12,2),
    -- Timestamps
    event_timestamp     TIMESTAMP_NTZ NOT NULL,
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(event_timestamp), event_type);


-- ── fact_sessions ─────────────────────────────────────────────────────────────
-- Grain: one row per session (sessionized from fact_events)
CREATE TABLE IF NOT EXISTS fact_sessions (
    session_sk          BIGINT      NOT NULL AUTOINCREMENT PRIMARY KEY,
    session_id          VARCHAR(64) NOT NULL,
    user_sk             INT         REFERENCES dim_user(user_sk),
    date_sk             INT         REFERENCES dim_date(date_key),
    geo_sk              INT         REFERENCES dim_geography(geo_sk),
    -- Session metrics
    session_start       TIMESTAMP_NTZ,
    session_end         TIMESTAMP_NTZ,
    duration_seconds    INT,
    event_count         INT,
    page_depth          INT,
    unique_products_viewed INT,
    -- Funnel flags
    did_search          BOOLEAN DEFAULT FALSE,
    did_add_to_cart     BOOLEAN DEFAULT FALSE,
    did_checkout        BOOLEAN DEFAULT FALSE,
    did_purchase        BOOLEAN DEFAULT FALSE,
    -- Session value
    order_amount        NUMBER(12,2),
    device_type         VARCHAR(20),
    entry_page          VARCHAR(200),
    exit_page           VARCHAR(200),
    _loaded_at          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(session_start));
