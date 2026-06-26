"""
Pydantic event schemas — enforce contract at the producer boundary.
These mirror Avro schemas registered in Schema Registry for consumer validation.
"""
from __future__ import annotations

import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    USER_REGISTERED = "user_registered"
    USER_LOGIN = "user_login"
    USER_LOGOUT = "user_logout"
    PRODUCT_VIEWED = "product_viewed"
    PRODUCT_SEARCHED = "product_searched"
    CART_ADD = "cart_add"
    CART_REMOVE = "cart_remove"
    CHECKOUT_STARTED = "checkout_started"
    ORDER_PLACED = "order_placed"
    ORDER_PAID = "order_paid"
    ORDER_SHIPPED = "order_shipped"
    ORDER_DELIVERED = "order_delivered"
    ORDER_CANCELLED = "order_cancelled"
    REVIEW_SUBMITTED = "review_submitted"
    WISHLIST_ADD = "wishlist_add"


class DeviceType(str, Enum):
    MOBILE = "mobile"
    DESKTOP = "desktop"
    TABLET = "tablet"
    APP = "app"


class BaseEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    session_id: str
    user_id: str
    device_type: DeviceType
    ip_address: str
    user_agent: str
    country: str
    city: str
    platform: str = "web"
    app_version: str = "2.4.1"
    metadata: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp", mode="before")
    @classmethod
    def parse_timestamp(cls, v):
        if isinstance(v, str):
            return datetime.fromisoformat(v)
        return v

    def to_kafka_key(self) -> str:
        return self.user_id

    def to_dict(self) -> Dict[str, Any]:
        return self.model_dump(mode="json")


class UserRegisteredEvent(BaseEvent):
    event_type: EventType = EventType.USER_REGISTERED
    email: str
    first_name: str
    last_name: str
    date_of_birth: Optional[str] = None
    gender: Optional[str] = None
    referral_source: Optional[str] = None
    marketing_opt_in: bool = False


class ProductViewedEvent(BaseEvent):
    event_type: EventType = EventType.PRODUCT_VIEWED
    product_id: str
    product_name: str
    category: str
    subcategory: str
    price: float
    currency: str = "USD"
    brand: str
    view_duration_seconds: int = 0
    image_clicked: bool = False
    recommendation_source: Optional[str] = None


class SearchEvent(BaseEvent):
    event_type: EventType = EventType.PRODUCT_SEARCHED
    query: str
    filters_applied: Dict[str, Any] = Field(default_factory=dict)
    results_count: int = 0
    clicked_result_position: Optional[int] = None
    clicked_product_id: Optional[str] = None
    search_source: str = "search_bar"


class CartEvent(BaseEvent):
    event_type: EventType
    product_id: str
    product_name: str
    quantity: int
    unit_price: float
    currency: str = "USD"
    cart_total: float
    cart_item_count: int


class OrderItem(BaseModel):
    product_id: str
    product_name: str
    category: str
    quantity: int
    unit_price: float
    discount_amount: float = 0.0
    total_price: float


class OrderEvent(BaseEvent):
    event_type: EventType
    order_id: str
    items: List[OrderItem]
    subtotal: float
    tax_amount: float
    shipping_amount: float
    discount_amount: float = 0.0
    total_amount: float
    currency: str = "USD"
    payment_method: str
    shipping_address_country: str
    coupon_code: Optional[str] = None
    is_first_order: bool = False
