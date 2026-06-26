-- =============================================================================
-- RAW Layer Tables — minimal typing, preserve source data exactly as received
-- =============================================================================

USE DATABASE RAW_DB;
USE SCHEMA EVENTS;

CREATE TABLE IF NOT EXISTS USER_EVENTS (
    event_id        VARCHAR(36)     NOT NULL,
    event_type      VARCHAR(50)     NOT NULL,
    timestamp       TIMESTAMP_NTZ   NOT NULL,
    session_id      VARCHAR(64),
    user_id         VARCHAR(32)     NOT NULL,
    device_type     VARCHAR(20),
    ip_address      VARCHAR(45),
    country         VARCHAR(3),
    city            VARCHAR(100),
    platform        VARCHAR(20),
    email           VARCHAR(255),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    date_of_birth   DATE,
    gender          VARCHAR(10),
    referral_source VARCHAR(100),
    marketing_opt_in BOOLEAN,
    _kafka_topic    VARCHAR(100),
    _kafka_partition SMALLINT,
    _kafka_offset   BIGINT,
    _loaded_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(timestamp), country);


CREATE TABLE IF NOT EXISTS PRODUCT_EVENTS (
    event_id                VARCHAR(36)     NOT NULL,
    event_type              VARCHAR(50)     NOT NULL,
    timestamp               TIMESTAMP_NTZ   NOT NULL,
    session_id              VARCHAR(64),
    user_id                 VARCHAR(32)     NOT NULL,
    device_type             VARCHAR(20),
    country                 VARCHAR(3),
    city                    VARCHAR(100),
    product_id              VARCHAR(20),
    product_name            VARCHAR(500),
    category                VARCHAR(100),
    subcategory             VARCHAR(100),
    price                   NUMBER(10,2),
    currency                VARCHAR(3),
    brand                   VARCHAR(200),
    view_duration_seconds   INT,
    image_clicked           BOOLEAN,
    recommendation_source   VARCHAR(100),
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(timestamp), category);


CREATE TABLE IF NOT EXISTS ORDER_EVENTS (
    event_id                VARCHAR(36)     NOT NULL,
    event_type              VARCHAR(50)     NOT NULL,
    timestamp               TIMESTAMP_NTZ   NOT NULL,
    session_id              VARCHAR(64),
    user_id                 VARCHAR(32)     NOT NULL,
    device_type             VARCHAR(20),
    country                 VARCHAR(3),
    order_id                VARCHAR(50)     NOT NULL,
    subtotal                NUMBER(12,2),
    tax_amount              NUMBER(10,2),
    shipping_amount         NUMBER(10,2),
    discount_amount         NUMBER(10,2),
    total_amount            NUMBER(12,2),
    currency                VARCHAR(3),
    payment_method          VARCHAR(50),
    coupon_code             VARCHAR(50),
    is_first_order          BOOLEAN,
    items_json              VARIANT,        -- Array of order items as JSON
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(timestamp), country);


CREATE TABLE IF NOT EXISTS SEARCH_EVENTS (
    event_id                VARCHAR(36)     NOT NULL,
    event_type              VARCHAR(50)     NOT NULL,
    timestamp               TIMESTAMP_NTZ   NOT NULL,
    session_id              VARCHAR(64),
    user_id                 VARCHAR(32)     NOT NULL,
    country                 VARCHAR(3),
    query                   VARCHAR(500),
    results_count           INT,
    clicked_result_position SMALLINT,
    clicked_product_id      VARCHAR(20),
    search_source           VARCHAR(50),
    filters_json            VARIANT,
    _loaded_at              TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(timestamp));


CREATE TABLE IF NOT EXISTS CART_EVENTS (
    event_id        VARCHAR(36)     NOT NULL,
    event_type      VARCHAR(50)     NOT NULL,
    timestamp       TIMESTAMP_NTZ   NOT NULL,
    session_id      VARCHAR(64),
    user_id         VARCHAR(32)     NOT NULL,
    device_type     VARCHAR(20),
    country         VARCHAR(3),
    product_id      VARCHAR(20),
    product_name    VARCHAR(500),
    quantity        SMALLINT,
    unit_price      NUMBER(10,2),
    cart_total      NUMBER(12,2),
    cart_item_count SMALLINT,
    _loaded_at      TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
) CLUSTER BY (DATE(timestamp));
