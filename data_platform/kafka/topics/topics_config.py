"""
Kafka topic admin — creates/updates topics with production-grade configs.
Run once during infrastructure bootstrap or via CI/CD pipeline.

Topic design principles:
  - Partitions = parallelism. Set based on peak throughput / consumer throughput.
  - Replication factor = 3 for production (survives 2 broker failures).
  - Retention is event-type driven: orders kept 7 days, raw events 3 days.
  - Compaction enabled for state topics (user profiles).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic


@dataclass
class TopicSpec:
    name: str
    num_partitions: int
    replication_factor: int = 3
    retention_ms: int = 259_200_000  # 3 days default
    cleanup_policy: str = "delete"
    compression_type: str = "snappy"
    min_insync_replicas: int = 2
    segment_ms: int = 3_600_000  # 1 hour segments for faster retention enforcement
    max_message_bytes: int = 1_048_576  # 1 MB
    extra_configs: Dict[str, str] = field(default_factory=dict)

    def to_new_topic(self) -> NewTopic:
        configs = {
            "retention.ms": str(self.retention_ms),
            "cleanup.policy": self.cleanup_policy,
            "compression.type": self.compression_type,
            "min.insync.replicas": str(self.min_insync_replicas),
            "segment.ms": str(self.segment_ms),
            "max.message.bytes": str(self.max_message_bytes),
            **self.extra_configs,
        }
        return NewTopic(
            self.name,
            num_partitions=self.num_partitions,
            replication_factor=self.replication_factor,
            config=configs,
        )


TOPIC_SPECS: List[TopicSpec] = [
    TopicSpec(
        name="user-events",
        num_partitions=8,
        retention_ms=604_800_000,  # 7 days — needed for user profile building
        extra_configs={"message.timestamp.type": "LogAppendTime"},
    ),
    TopicSpec(
        name="product-events",
        num_partitions=8,
        retention_ms=259_200_000,  # 3 days
    ),
    TopicSpec(
        name="order-events",
        num_partitions=8,
        retention_ms=604_800_000,  # 7 days — financial events need longer retention
        extra_configs={
            "min.insync.replicas": "3",  # Extra safety for financial data
        },
    ),
    TopicSpec(
        name="search-events",
        num_partitions=4,
        retention_ms=86_400_000,  # 1 day
    ),
    TopicSpec(
        name="cart-events",
        num_partitions=4,
        retention_ms=172_800_000,  # 2 days
    ),
    TopicSpec(
        name="session-events",
        num_partitions=4,
        cleanup_policy="compact",  # Keep latest session state
        retention_ms=86_400_000,
    ),
    TopicSpec(
        name="dead-letter-queue",
        num_partitions=2,
        retention_ms=604_800_000,  # 7 days for investigation
    ),
]


def create_topics(bootstrap_servers: str = "localhost:9092") -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())

    to_create = [
        spec.to_new_topic()
        for spec in TOPIC_SPECS
        if spec.name not in existing
    ]

    if not to_create:
        print("All topics already exist.")
        return

    results = admin.create_topics(to_create)
    for name, future in results.items():
        try:
            future.result()
            print(f"Created topic: {name}")
        except Exception as exc:
            print(f"Failed to create {name}: {exc}")


if __name__ == "__main__":
    import os
    create_topics(os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
