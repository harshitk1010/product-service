-- =============================================================================
-- Snowflake Warehouse Architecture
-- Three-tier database design: RAW → ANALYTICS → REPORTING
-- =============================================================================

-- ── Virtual Warehouses ────────────────────────────────────────────────────────
-- Separate warehouses isolate workloads and prevent resource contention.

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_LOAD_WH
    WAREHOUSE_SIZE = 'MEDIUM'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'Used exclusively for data loading (COPY INTO commands)';

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_TRANSFORM_WH
    WAREHOUSE_SIZE = 'LARGE'
    AUTO_SUSPEND = 120
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MAX_CLUSTER_COUNT = 3
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY = 'ECONOMY'
    COMMENT = 'Multi-cluster WH for dbt transformations';

CREATE WAREHOUSE IF NOT EXISTS ANALYTICS_QUERY_WH
    WAREHOUSE_SIZE = 'SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    MAX_CLUSTER_COUNT = 5
    MIN_CLUSTER_COUNT = 1
    SCALING_POLICY = 'STANDARD'
    COMMENT = 'Used for BI tool queries (Power BI, Looker). Multi-cluster for concurrency.';


-- ── Databases ─────────────────────────────────────────────────────────────────

CREATE DATABASE IF NOT EXISTS RAW_DB
    DATA_RETENTION_TIME_IN_DAYS = 7
    COMMENT = 'Landing zone: raw events from S3 via COPY INTO';

CREATE DATABASE IF NOT EXISTS ANALYTICS_DB
    DATA_RETENTION_TIME_IN_DAYS = 30
    COMMENT = 'dbt transformation layer: staging, intermediate, marts';

CREATE DATABASE IF NOT EXISTS REPORTING_DB
    DATA_RETENTION_TIME_IN_DAYS = 90
    COMMENT = 'Final reporting layer: Power BI reads from here';


-- ── Schemas — RAW_DB ──────────────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS RAW_DB.EVENTS;
CREATE SCHEMA IF NOT EXISTS RAW_DB.STAGES;


-- ── Schemas — ANALYTICS_DB ───────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS ANALYTICS_DB.MARTS;


-- ── Schemas — REPORTING_DB ───────────────────────────────────────────────────

CREATE SCHEMA IF NOT EXISTS REPORTING_DB.EXECUTIVE;
CREATE SCHEMA IF NOT EXISTS REPORTING_DB.REVENUE;
CREATE SCHEMA IF NOT EXISTS REPORTING_DB.CUSTOMER;
CREATE SCHEMA IF NOT EXISTS REPORTING_DB.PRODUCT;


-- ── S3 External Stage ─────────────────────────────────────────────────────────

CREATE OR REPLACE STAGE RAW_DB.STAGES.S3_SILVER_STAGE
    URL = 's3://ecommerce-analytics-lake/silver/'
    CREDENTIALS = (
        AWS_ROLE = 'arn:aws:iam::ACCOUNT_ID:role/SnowflakeS3AccessRole'
    )
    FILE_FORMAT = (
        TYPE = PARQUET
        SNAPPY_COMPRESSION = TRUE
    )
    COMMENT = 'S3 Silver layer stage for COPY INTO operations';


-- ── File Format ───────────────────────────────────────────────────────────────

CREATE OR REPLACE FILE FORMAT RAW_DB.STAGES.PARQUET_FORMAT
    TYPE = PARQUET
    SNAPPY_COMPRESSION = TRUE
    BINARY_AS_TEXT = FALSE;
