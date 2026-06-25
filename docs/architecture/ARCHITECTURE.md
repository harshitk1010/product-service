# Real-Time E-Commerce Analytics Platform — System Architecture

## Overview

This platform processes **millions of e-commerce events per day** in real time,
providing sub-second analytics for business intelligence, fraud detection, and
personalization. Modeled after systems used at Amazon, Netflix, and Uber.

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                          E-COMMERCE EVENT SOURCES                                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │  Web App     │  │  Mobile App  │  │  Backend API  │  │  Partner     │           │
│  │  (React/JS)  │  │  (iOS/Andr)  │  │  Services    │  │  Webhooks    │           │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘           │
└─────────┼─────────────────┼─────────────────┼─────────────────┼─────────────────────┘
          │                 │                 │                 │
          ▼                 ▼                 ▼                 ▼
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         EVENT INGESTION LAYER                                        │
│                    ┌─────────────────────────────┐                                  │
│                    │     Kafka Producers          │                                  │
│                    │  (Python / FastAPI Gateway)  │                                  │
│                    └─────────────┬───────────────┘                                  │
└──────────────────────────────────┼──────────────────────────────────────────────────┘
                                   │
          ┌────────────────────────▼─────────────────────────┐
          │              APACHE KAFKA CLUSTER                 │
          │  ┌──────────────┐  ┌──────────────┐              │
          │  │user-events   │  │product-events│              │
          │  │(8 partitions)│  │(8 partitions)│              │
          │  └──────────────┘  └──────────────┘              │
          │  ┌──────────────┐  ┌──────────────┐              │
          │  │order-events  │  │search-events │              │
          │  │(8 partitions)│  │(4 partitions)│              │
          │  └──────────────┘  └──────────────┘              │
          │  ┌──────────────┐  ┌──────────────┐              │
          │  │cart-events   │  │dlq-events    │              │
          │  │(4 partitions)│  │(2 partitions)│              │
          │  └──────────────┘  └──────────────┘              │
          └──────┬───────────────────────┬────────────────────┘
                 │                       │
    ┌────────────▼───────┐   ┌───────────▼──────────────────┐
    │  SPARK STREAMING   │   │        DATA LAKE (S3)         │
    │  (PySpark 3.5)     │   │  ┌──────────────────────────┐│
    │  ┌─────────────┐   │   │  │ raw/   (Kafka offsets)   ││
    │  │ Window Aggs │   │   │  │ bronze/(Parquet, raw)    ││
    │  │ Session     │   │   │  │ silver/(Cleaned/Enriched)││
    │  │ Analytics   │   │   │  │ gold/  (Aggregated)      ││
    │  │ Funnel Calc │   │   │  └──────────────────────────┘│
    │  └──────┬──────┘   │   └───────────┬──────────────────┘
    └─────────┼──────────┘               │
              │                          │
    ┌─────────▼──────────────────────────▼──────────┐
    │           SNOWFLAKE DATA WAREHOUSE              │
    │  ┌─────────────────────────────────────────┐   │
    │  │ RAW_DB → ANALYTICS_DB → REPORTING_DB    │   │
    │  │ Fact Tables: fact_orders, fact_events    │   │
    │  │ Dim Tables: dim_user, dim_product, date  │   │
    │  │ SCD Type 2 for dim_user, dim_product     │   │
    │  └─────────────────────────────────────────┘   │
    └──────────────────┬─────────────────────────────┘
                       │
    ┌──────────────────▼────────────────────────────┐
    │                  dbt LAYER                     │
    │  staging → intermediate → marts → reports     │
    │  Data quality tests, documentation, lineage   │
    └──────────────────┬────────────────────────────┘
                       │
    ┌──────────────────▼────────────────────────────┐
    │            ORCHESTRATION (Airflow)             │
    │  DAGs: ingest, transform, load, quality_check  │
    └──────────────────┬────────────────────────────┘
                       │
    ┌──────────────────▼────────────────────────────┐
    │           BI LAYER (Power BI / Grafana)        │
    │  Revenue | Conversion | Customer | Executive   │
    └───────────────────────────────────────────────┘

CROSS-CUTTING CONCERNS:
    PostgreSQL (operational metadata + audit logs)
    Prometheus + Grafana (infrastructure + pipeline metrics)
    GitHub Actions CI/CD (automated test + deploy)
    Kubernetes + Helm (container orchestration)
    Vault / AWS Secrets Manager (secrets management)
```

---

## Component Decisions & Trade-offs

### Apache Kafka — Event Streaming Backbone
**Why**: Kafka handles 1M+ events/sec with microsecond latency. Its durable log
enables replay, exactly-once semantics, and fan-out to multiple consumers.

**Trade-off vs. Kinesis**: Kafka gives more control (partition count, retention
policy, consumer group semantics). Kinesis is simpler but limits you to 1MB/s
per shard. At Amazon-scale, Kafka is universally preferred for internal streaming.

**Partition Strategy**:
- `user-events`: partition by `user_id` → ensures per-user ordering
- `order-events`: partition by `order_id` → ensures order lifecycle ordering
- `product-events`: partition by `product_id` → ensures product event ordering
- Replication factor: 3 (survives 2 broker failures)

### Apache Spark Streaming — Real-time Processing
**Why**: Structured Streaming gives micro-batch semantics with SQL API. At
Netflix/Uber scale, Spark handles exactly-once processing with checkpointing.

**Trade-off vs. Flink**: Flink has true event-time processing and lower latency,
but Spark Structured Streaming is sufficient for 100ms micro-batches and
integrates better with the existing Spark ecosystem (MLlib, Delta Lake).

### Snowflake — Cloud Data Warehouse
**Why**: Snowflake separates compute from storage, allowing elastic scaling.
Auto-suspend/resume means you pay only for what you use. Time-travel supports
data auditing. Works natively with S3/dbt.

**Trade-off vs. Redshift**: Snowflake's virtual warehouse model scales faster
and requires less tuning. Redshift is cheaper for steady workloads but Snowflake
wins for variable analytics workloads.

### dbt — Transformation Layer
**Why**: dbt brings software engineering to SQL — version control, testing,
documentation, lineage. Every model is testable; every column can be documented.

### Airflow — Orchestration
**Why**: Industry-standard DAG orchestration. Supports complex dependencies,
SLAs, retry logic, and has native connectors for every tool in this stack.

### S3 Data Lake — Multi-Zone Architecture
**Why**: Raw + Bronze + Silver + Gold follows the medallion architecture used
at Databricks/Netflix. Raw is immutable (audit). Bronze is parsed. Silver is
cleaned and enriched. Gold is aggregated for consumption.

---

## Data Flow

```
EVENT → Kafka Producer → Kafka Topic → Spark Consumer
      ↓                                     ↓
  PostgreSQL                         S3 Bronze Layer
  (audit log)                              ↓
                                    Spark Transforms
                                           ↓
                                    S3 Silver Layer
                                           ↓
                                    Snowflake COPY
                                           ↓
                                       dbt Models
                                           ↓
                                    Power BI / Grafana
```

---

## Scalability Design

| Component | Current | Scale-out Strategy |
|-----------|---------|-------------------|
| Kafka | 3 brokers, 8 partitions | Add brokers, rebalance partitions |
| Spark | 3 workers, 8 cores | Add worker nodes via K8s HPA |
| Snowflake | Medium WH (8 credits/hr) | Multi-cluster WH for concurrency |
| PostgreSQL | 1 primary | Read replicas via PgBouncer |
| Airflow | 1 scheduler, 3 workers | Kubernetes executor + auto-scaling |

---

## Failure Modes & Recovery

| Failure | Detection | Recovery |
|---------|-----------|----------|
| Kafka broker down | Prometheus alert | Automatic leader election (RF=3) |
| Spark job failure | Airflow SLA miss | Checkpoint-based resume |
| Snowflake query timeout | dbt test failure | Retry with exponential backoff |
| S3 write failure | CloudWatch + Prometheus | Dead-letter queue + replay |
| Airflow scheduler crash | PagerDuty alert | K8s restart policy |
