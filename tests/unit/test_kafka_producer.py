"""Unit tests for Kafka producer — mock confluent-kafka to avoid broker dependency."""
import json
import pytest
from unittest.mock import MagicMock, patch, call
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../services/event_producer'))


class TestTopicRouter:
    def test_order_routes_to_order_events(self):
        from kafka_producer import TopicRouter
        assert TopicRouter.get_topic("order_placed") == "order-events"
        assert TopicRouter.get_topic("order_paid") == "order-events"
        assert TopicRouter.get_topic("checkout_started") == "order-events"

    def test_product_routes_to_product_events(self):
        from kafka_producer import TopicRouter
        assert TopicRouter.get_topic("product_viewed") == "product-events"

    def test_search_routes_to_search_events(self):
        from kafka_producer import TopicRouter
        assert TopicRouter.get_topic("product_searched") == "search-events"

    def test_cart_routes_to_cart_events(self):
        from kafka_producer import TopicRouter
        assert TopicRouter.get_topic("cart_add") == "cart-events"
        assert TopicRouter.get_topic("cart_remove") == "cart-events"

    def test_unknown_routes_to_user_events(self):
        from kafka_producer import TopicRouter
        assert TopicRouter.get_topic("unknown_event_type") == "user-events"


class TestEcommerceKafkaProducer:
    @patch('kafka_producer.Producer')
    def test_produce_calls_kafka(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        from kafka_producer import EcommerceKafkaProducer
        producer = EcommerceKafkaProducer()

        test_event = {
            "event_id": "test-id",
            "event_type": "order_placed",
            "user_id": "U123",
        }
        producer.produce(topic="order-events", event=test_event, key="U123")

        mock_producer.produce.assert_called_once()
        call_kwargs = mock_producer.produce.call_args
        assert call_kwargs.kwargs["topic"] == "order-events"

    @patch('kafka_producer.Producer')
    def test_dlq_routing_on_kafka_exception(self, mock_producer_class):
        from confluent_kafka import KafkaException
        mock_producer = MagicMock()
        mock_producer.produce.side_effect = KafkaException()
        mock_producer_class.return_value = mock_producer

        from kafka_producer import EcommerceKafkaProducer
        producer = EcommerceKafkaProducer()

        # Should not raise — routes to DLQ instead
        producer.produce(
            topic="order-events",
            event={"event_type": "order_placed", "user_id": "U123"},
        )

    @patch('kafka_producer.Producer')
    def test_produce_serializes_event_to_json(self, mock_producer_class):
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        from kafka_producer import EcommerceKafkaProducer
        producer = EcommerceKafkaProducer()

        test_event = {"event_id": "abc", "user_id": "U1", "amount": 99.99}
        producer.produce(topic="test-topic", event=test_event)

        call_kwargs = mock_producer.produce.call_args.kwargs
        payload = json.loads(call_kwargs["value"].decode())
        assert payload["amount"] == 99.99
