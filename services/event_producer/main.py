"""
Event Producer Service — FastAPI gateway that accepts HTTP events and
publishes to Kafka. In production, this runs as a K8s Deployment with
HPA based on CPU and queue depth.
"""
from __future__ import annotations

import asyncio
import signal
import sys
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List

import structlog
import uvicorn
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from config import KAFKA, TOPICS
from kafka_producer import EcommerceKafkaProducer, TopicRouter
from schemas import (
    CartEvent,
    OrderEvent,
    ProductViewedEvent,
    SearchEvent,
    UserRegisteredEvent,
)
from synthetic_data_generator import simulate_user_session

logger = structlog.get_logger(__name__)
producer: EcommerceKafkaProducer = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global producer
    logger.info("starting_event_producer_service")
    producer = EcommerceKafkaProducer()
    yield
    logger.info("shutting_down_event_producer_service")
    if producer:
        producer.close()


app = FastAPI(
    title="E-Commerce Event Producer",
    version="1.0.0",
    description="Real-time event ingestion API — publishes to Kafka",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["POST", "GET"],
    allow_headers=["*"],
)

# Mount Prometheus metrics endpoint
metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


def publish_event(event_dict: Dict[str, Any]) -> None:
    topic = TopicRouter.get_topic(event_dict.get("event_type", ""))
    producer.produce(
        topic=topic,
        event=event_dict,
        key=event_dict.get("user_id"),
        headers={"trace-id": event_dict.get("event_id", "")},
    )


@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "event-producer", "timestamp": time.time()}


@app.post("/events/user", status_code=status.HTTP_202_ACCEPTED)
async def ingest_user_event(
    event: UserRegisteredEvent, background_tasks: BackgroundTasks
):
    background_tasks.add_task(publish_event, event.to_dict())
    return {"event_id": event.event_id, "status": "accepted"}


@app.post("/events/product", status_code=status.HTTP_202_ACCEPTED)
async def ingest_product_event(
    event: ProductViewedEvent, background_tasks: BackgroundTasks
):
    background_tasks.add_task(publish_event, event.to_dict())
    return {"event_id": event.event_id, "status": "accepted"}


@app.post("/events/search", status_code=status.HTTP_202_ACCEPTED)
async def ingest_search_event(
    event: SearchEvent, background_tasks: BackgroundTasks
):
    background_tasks.add_task(publish_event, event.to_dict())
    return {"event_id": event.event_id, "status": "accepted"}


@app.post("/events/cart", status_code=status.HTTP_202_ACCEPTED)
async def ingest_cart_event(event: CartEvent, background_tasks: BackgroundTasks):
    background_tasks.add_task(publish_event, event.to_dict())
    return {"event_id": event.event_id, "status": "accepted"}


@app.post("/events/order", status_code=status.HTTP_202_ACCEPTED)
async def ingest_order_event(event: OrderEvent, background_tasks: BackgroundTasks):
    background_tasks.add_task(publish_event, event.to_dict())
    return {"event_id": event.event_id, "status": "accepted"}


@app.post("/simulate/sessions", status_code=status.HTTP_200_OK)
async def simulate_sessions(n_sessions: int = 100):
    """Load-test endpoint: generates n synthetic sessions and publishes all events."""
    if n_sessions > 10000:
        raise HTTPException(status_code=400, detail="Max 10000 sessions per request")

    total = 0
    for _ in range(n_sessions):
        for event_envelope in simulate_user_session():
            publish_event(event_envelope["event"])
            total += 1

    producer.flush(timeout=30)
    return {"sessions_simulated": n_sessions, "events_published": total}


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        workers=4,
        log_config=None,
    )
