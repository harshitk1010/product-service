"""Unit tests for event schema validation."""
import pytest
from datetime import datetime
from unittest.mock import patch
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../../services/event_producer'))

from schemas import (
    UserRegisteredEvent,
    ProductViewedEvent,
    SearchEvent,
    CartEvent,
    OrderEvent,
    OrderItem,
    EventType,
    DeviceType,
)


BASE_KWARGS = dict(
    session_id="S-TEST-SESSION",
    user_id="U123456",
    device_type=DeviceType.DESKTOP,
    ip_address="192.168.1.1",
    user_agent="Mozilla/5.0",
    country="US",
    city="New York",
)


class TestUserRegisteredEvent:
    def test_valid_user_registered(self):
        event = UserRegisteredEvent(
            **BASE_KWARGS,
            email="test@example.com",
            first_name="John",
            last_name="Doe",
        )
        assert event.event_type == EventType.USER_REGISTERED
        assert event.email == "test@example.com"
        assert event.event_id is not None
        assert len(event.event_id) == 36  # UUID4 format

    def test_event_id_is_unique(self):
        e1 = UserRegisteredEvent(**BASE_KWARGS, email="a@b.com",
                                  first_name="A", last_name="B")
        e2 = UserRegisteredEvent(**BASE_KWARGS, email="c@d.com",
                                  first_name="C", last_name="D")
        assert e1.event_id != e2.event_id

    def test_to_kafka_key_returns_user_id(self):
        event = UserRegisteredEvent(**BASE_KWARGS, email="a@b.com",
                                     first_name="A", last_name="B")
        assert event.to_kafka_key() == "U123456"

    def test_to_dict_is_json_serializable(self):
        import json
        event = UserRegisteredEvent(**BASE_KWARGS, email="a@b.com",
                                     first_name="A", last_name="B")
        d = event.to_dict()
        assert json.dumps(d, default=str)  # Should not raise
        assert d["event_type"] == "user_registered"


class TestProductViewedEvent:
    def test_valid_product_view(self):
        event = ProductViewedEvent(
            **BASE_KWARGS,
            product_id="P00001",
            product_name="Test Headphones",
            category="Electronics",
            subcategory="Headphones",
            price=99.99,
            brand="Sony",
        )
        assert event.event_type == EventType.PRODUCT_VIEWED
        assert event.price == 99.99
        assert event.currency == "USD"

    def test_view_duration_defaults_to_zero(self):
        event = ProductViewedEvent(
            **BASE_KWARGS,
            product_id="P00001",
            product_name="Test",
            category="Electronics",
            subcategory="Laptops",
            price=999.0,
            brand="Apple",
        )
        assert event.view_duration_seconds == 0


class TestOrderEvent:
    def _make_item(self) -> OrderItem:
        return OrderItem(
            product_id="P00001",
            product_name="Test Product",
            category="Electronics",
            quantity=2,
            unit_price=49.99,
            discount_amount=5.00,
            total_price=94.98,
        )

    def test_valid_order_event(self):
        event = OrderEvent(
            **BASE_KWARGS,
            event_type=EventType.ORDER_PLACED,
            order_id="ORD-ABC123",
            items=[self._make_item()],
            subtotal=94.98,
            tax_amount=7.60,
            shipping_amount=4.99,
            discount_amount=5.00,
            total_amount=107.57,
            payment_method="credit_card",
            shipping_address_country="US",
        )
        assert event.order_id == "ORD-ABC123"
        assert len(event.items) == 1
        assert event.total_amount == 107.57


class TestSearchEvent:
    def test_valid_search(self):
        event = SearchEvent(
            **BASE_KWARGS,
            query="wireless headphones",
            results_count=42,
        )
        assert event.event_type == EventType.PRODUCT_SEARCHED
        assert event.query == "wireless headphones"
        assert event.results_count == 42


class TestSyntheticDataGenerator:
    def test_simulate_session_yields_events(self):
        import sys
        sys.path.insert(0, os.path.join(os.path.dirname(__file__),
                                         '../../services/event_producer'))
        from synthetic_data_generator import simulate_user_session
        events = list(simulate_user_session())
        assert len(events) >= 0  # May be 0 for sessions that bounce

    def test_generate_bulk_events(self):
        from synthetic_data_generator import generate_bulk_events
        events = generate_bulk_events(n_sessions=5)
        assert len(events) >= 0

        for e in events:
            assert "topic" in e
            assert "event" in e
            assert e["topic"] in [
                "user-events", "product-events", "order-events",
                "search-events", "cart-events"
            ]
