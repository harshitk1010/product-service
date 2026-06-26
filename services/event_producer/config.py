"""
Centralised configuration via environment variables.
All secrets come from AWS Secrets Manager / Vault in production.
"""
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    security_protocol: str = os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT")
    sasl_mechanism: str = os.getenv("KAFKA_SASL_MECHANISM", "PLAIN")
    sasl_username: str = os.getenv("KAFKA_SASL_USERNAME", "")
    sasl_password: str = os.getenv("KAFKA_SASL_PASSWORD", "")
    schema_registry_url: str = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    acks: str = "all"
    retries: int = 3
    linger_ms: int = 5
    batch_size: int = 16384
    compression_type: str = "snappy"
    enable_idempotence: bool = True
    max_in_flight_requests_per_connection: int = 5


@dataclass(frozen=True)
class TopicConfig:
    user_events: str = "user-events"
    product_events: str = "product-events"
    order_events: str = "order-events"
    search_events: str = "search-events"
    cart_events: str = "cart-events"
    dlq: str = "dead-letter-queue"
    session_events: str = "session-events"


@dataclass(frozen=True)
class DatabaseConfig:
    host: str = os.getenv("PG_HOST", "localhost")
    port: int = int(os.getenv("PG_PORT", "5432"))
    database: str = os.getenv("PG_DATABASE", "ecommerce_audit")
    user: str = os.getenv("PG_USER", "postgres")
    password: str = os.getenv("PG_PASSWORD", "postgres")


@dataclass(frozen=True)
class S3Config:
    bucket: str = os.getenv("S3_BUCKET", "ecommerce-analytics-lake")
    region: str = os.getenv("AWS_REGION", "us-east-1")
    raw_prefix: str = "raw"
    bronze_prefix: str = "bronze"
    silver_prefix: str = "silver"
    gold_prefix: str = "gold"


KAFKA = KafkaConfig()
TOPICS = TopicConfig()
DATABASE = DatabaseConfig()
S3 = S3Config()
