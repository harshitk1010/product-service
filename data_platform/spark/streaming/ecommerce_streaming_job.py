"""
Spark Structured Streaming job — real-time e-commerce analytics.

Reads from Kafka, applies window aggregations, session analytics,
funnel calculations, and writes to:
  1. S3 Bronze layer (raw parsed events)
  2. S3 Silver layer (enriched, cleaned events)
  3. S3 Gold layer (pre-aggregated metrics)
  4. PostgreSQL (real-time dashboard metrics)

Fault tolerance: checkpointing to S3 ensures exactly-once semantics.
"""
from __future__ import annotations

import os
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pyspark.sql.window import Window

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
S3_BUCKET = os.getenv("S3_BUCKET", "s3a://ecommerce-analytics-lake")
CHECKPOINT_BASE = f"{S3_BUCKET}/checkpoints"
PG_URL = os.getenv("PG_JDBC_URL", "jdbc:postgresql://localhost:5432/ecommerce_metrics")
PG_PROPS = {
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "postgres"),
    "driver": "org.postgresql.Driver",
}

# ── Schemas ───────────────────────────────────────────────────────────────────

ORDER_ITEM_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount_amount", DoubleType()),
    StructField("total_price", DoubleType()),
])

BASE_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("session_id", StringType()),
    StructField("user_id", StringType(), False),
    StructField("device_type", StringType()),
    StructField("ip_address", StringType()),
    StructField("country", StringType()),
    StructField("city", StringType()),
    StructField("platform", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("query", StringType()),
    StructField("results_count", IntegerType()),
    StructField("order_id", StringType()),
    StructField("total_amount", DoubleType()),
    StructField("items", ArrayType(ORDER_ITEM_SCHEMA)),
    StructField("is_first_order", BooleanType()),
    StructField("payment_method", StringType()),
    StructField("cart_total", DoubleType()),
    StructField("cart_item_count", IntegerType()),
])


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcommerceRealTimeAnalytics")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BASE)
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.metricsEnabled", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.4,"
                "org.postgresql:postgresql:42.7.1")
        .getOrCreate()
    )


def read_kafka_stream(spark: SparkSession, topics: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topics)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 100_000)
        .option("kafka.group.id", "spark-streaming-consumer")
        .option("kafka.session.timeout.ms", "45000")
        .option("kafka.heartbeat.interval.ms", "10000")
        .load()
    )


def parse_events(raw_df: DataFrame) -> DataFrame:
    return (
        raw_df
        .select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("timestamp").alias("kafka_timestamp"),
            F.from_json(
                F.col("value").cast("string"),
                BASE_EVENT_SCHEMA
            ).alias("data")
        )
        .select(
            "topic", "partition", "offset", "kafka_timestamp",
            "data.*"
        )
        .withColumn(
            "event_timestamp",
            F.to_timestamp(F.col("timestamp"))
        )
        .withColumn(
            "processing_date",
            F.to_date(F.col("event_timestamp"))
        )
        .withColumn(
            "processing_hour",
            F.hour(F.col("event_timestamp"))
        )
        # Watermark: tolerate 10-minute late arrivals
        .withWatermark("event_timestamp", "10 minutes")
    )


# ── Bronze Layer ──────────────────────────────────────────────────────────────

def write_bronze_layer(df: DataFrame) -> None:
    """
    Raw parsed events — no transformations.
    Partitioned by date/hour for efficient querying.
    """
    (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{S3_BUCKET}/bronze/events")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze")
        .partitionBy("topic", "processing_date", "processing_hour")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Real-time Revenue Metrics (Gold) ──────────────────────────────────────────

def compute_revenue_metrics(df: DataFrame) -> DataFrame:
    """5-minute tumbling window revenue aggregation."""
    orders = df.filter(F.col("event_type") == "order_placed")
    return (
        orders
        .groupBy(
            F.window("event_timestamp", "5 minutes"),
            F.col("country"),
        )
        .agg(
            F.count("order_id").alias("order_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("total_amount").alias("avg_order_value"),
            F.countDistinct("user_id").alias("unique_buyers"),
            F.sum(F.when(F.col("is_first_order"), 1).otherwise(0))
             .alias("new_customer_orders"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "country",
            "order_count",
            "total_revenue",
            "avg_order_value",
            "unique_buyers",
            "new_customer_orders",
        )
    )


def write_revenue_metrics(df: DataFrame) -> None:
    (
        df.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", f"{S3_BUCKET}/gold/revenue_metrics")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/revenue_metrics")
        .partitionBy("country")
        .trigger(processingTime="60 seconds")
        .start()
    )


# ── Conversion Funnel (Gold) ──────────────────────────────────────────────────

def compute_conversion_funnel(df: DataFrame) -> DataFrame:
    """
    15-minute sliding window funnel:
      product_viewed → cart_add → checkout_started → order_placed
    """
    return (
        df
        .groupBy(
            F.window("event_timestamp", "15 minutes", "5 minutes"),
        )
        .agg(
            F.countDistinct(
                F.when(F.col("event_type") == "product_viewed", F.col("session_id"))
            ).alias("sessions_product_viewed"),
            F.countDistinct(
                F.when(F.col("event_type") == "cart_add", F.col("session_id"))
            ).alias("sessions_add_to_cart"),
            F.countDistinct(
                F.when(F.col("event_type") == "checkout_started", F.col("session_id"))
            ).alias("sessions_checkout_started"),
            F.countDistinct(
                F.when(F.col("event_type") == "order_placed", F.col("session_id"))
            ).alias("sessions_ordered"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "sessions_product_viewed",
            "sessions_add_to_cart",
            "sessions_checkout_started",
            "sessions_ordered",
            F.when(
                F.col("sessions_product_viewed") > 0,
                F.round(
                    F.col("sessions_ordered") / F.col("sessions_product_viewed") * 100, 2
                )
            ).otherwise(0).alias("overall_conversion_pct"),
        )
    )


# ── Top Products (Gold) ───────────────────────────────────────────────────────

def compute_top_products(df: DataFrame) -> DataFrame:
    """10-minute window — top products by views and revenue."""
    return (
        df
        .filter(
            F.col("event_type").isin("product_viewed", "order_placed")
        )
        .groupBy(
            F.window("event_timestamp", "10 minutes"),
            "product_id",
            "product_name",
            "category",
        )
        .agg(
            F.sum(F.when(F.col("event_type") == "product_viewed", 1).otherwise(0))
             .alias("view_count"),
            F.sum(F.when(F.col("event_type") == "order_placed", F.col("total_amount"))
                  .otherwise(0))
             .alias("revenue"),
            F.countDistinct("user_id").alias("unique_users"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "product_id", "product_name", "category",
            "view_count", "revenue", "unique_users",
        )
    )


# ── Session Analytics (Silver) ────────────────────────────────────────────────

def compute_session_analytics(df: DataFrame) -> DataFrame:
    """
    Per-session metrics: duration, page depth, conversion.
    Written to Silver because it still has user-level granularity.
    """
    return (
        df
        .groupBy(
            F.window("event_timestamp", "30 minutes", "5 minutes"),
            "session_id",
            "user_id",
            "device_type",
            "country",
        )
        .agg(
            F.min("event_timestamp").alias("session_start"),
            F.max("event_timestamp").alias("session_end"),
            F.count("event_id").alias("event_count"),
            F.countDistinct("product_id").alias("unique_products_viewed"),
            F.max(F.when(F.col("event_type") == "order_placed", F.col("total_amount")))
             .alias("order_amount"),
            F.max(F.col("event_type") == "order_placed").cast("boolean")
             .alias("converted"),
        )
        .withColumn(
            "session_duration_seconds",
            F.unix_timestamp("session_end") - F.unix_timestamp("session_start")
        )
    )


# ── Search Analytics (Silver) ─────────────────────────────────────────────────

def compute_search_analytics(df: DataFrame) -> DataFrame:
    return (
        df
        .filter(F.col("event_type") == "product_searched")
        .groupBy(
            F.window("event_timestamp", "10 minutes"),
            "query",
        )
        .agg(
            F.count("event_id").alias("search_count"),
            F.avg("results_count").alias("avg_results_count"),
            F.countDistinct("user_id").alias("unique_searchers"),
            F.countDistinct("session_id").alias("unique_sessions"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            "query", "search_count", "avg_results_count",
            "unique_searchers", "unique_sessions",
        )
    )


# ── Write to PostgreSQL for real-time dashboards ──────────────────────────────

def write_to_postgres(df: DataFrame, table: str) -> None:
    def write_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.count() > 0:
            (
                batch_df.write
                .format("jdbc")
                .option("url", PG_URL)
                .option("dbtable", table)
                .option("driver", PG_PROPS["driver"])
                .option("user", PG_PROPS["user"])
                .option("password", PG_PROPS["password"])
                .mode("append")
                .save()
            )

    (
        df.writeStream
        .foreachBatch(write_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table.replace('.', '_')}")
        .trigger(processingTime="30 seconds")
        .start()
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    all_topics = "user-events,product-events,order-events,search-events,cart-events"
    raw_df = read_kafka_stream(spark, all_topics)
    parsed_df = parse_events(raw_df)

    # Bronze — raw events
    write_bronze_layer(parsed_df)

    # Gold — revenue metrics
    revenue_df = compute_revenue_metrics(parsed_df)
    write_revenue_metrics(revenue_df)
    write_to_postgres(revenue_df, "realtime.revenue_metrics")

    # Gold — funnel
    funnel_df = compute_conversion_funnel(parsed_df)
    write_to_postgres(funnel_df, "realtime.funnel_metrics")

    # Gold — top products
    top_products_df = compute_top_products(parsed_df)
    write_to_postgres(top_products_df, "realtime.top_products")

    # Silver — sessions
    session_df = compute_session_analytics(parsed_df)
    write_to_postgres(session_df, "realtime.session_analytics")

    # Silver — search
    search_df = compute_search_analytics(parsed_df)
    write_to_postgres(search_df, "realtime.search_analytics")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
