"""
Synthetic e-commerce data generator.
Produces realistic event streams with proper statistical distributions
matching what you'd see in a real e-commerce platform.
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from typing import Generator, List, Optional

from faker import Faker

from schemas import (
    CartEvent,
    DeviceType,
    EventType,
    OrderEvent,
    OrderItem,
    ProductViewedEvent,
    SearchEvent,
    UserRegisteredEvent,
)

fake = Faker()
Faker.seed(42)

# ── Product catalogue ─────────────────────────────────────────────────────────

PRODUCT_CATALOGUE = [
    {"id": f"P{i:05d}", "name": fake.catch_phrase(), "category": cat,
     "subcategory": sub, "price": round(random.uniform(9.99, 499.99), 2),
     "brand": fake.company()}
    for i, (cat, sub) in enumerate([
        ("Electronics", "Smartphones"), ("Electronics", "Laptops"),
        ("Electronics", "Headphones"), ("Electronics", "Tablets"),
        ("Clothing", "Men's T-Shirts"), ("Clothing", "Women's Dresses"),
        ("Clothing", "Shoes"), ("Clothing", "Accessories"),
        ("Home & Garden", "Furniture"), ("Home & Garden", "Kitchen"),
        ("Home & Garden", "Bedding"), ("Books", "Fiction"),
        ("Books", "Non-Fiction"), ("Books", "Technical"),
        ("Sports", "Running"), ("Sports", "Gym Equipment"),
        ("Beauty", "Skincare"), ("Beauty", "Makeup"),
        ("Toys", "Board Games"), ("Toys", "Action Figures"),
    ] * 50)
]

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay",
                   "google_pay", "buy_now_pay_later", "crypto"]

REFERRAL_SOURCES = ["google_organic", "google_paid", "facebook_ad",
                    "instagram_ad", "email_campaign", "direct", "affiliate",
                    "influencer", "referral_program"]

COUNTRIES = [("US", "New York"), ("US", "Los Angeles"), ("US", "Chicago"),
             ("GB", "London"), ("DE", "Berlin"), ("FR", "Paris"),
             ("CA", "Toronto"), ("AU", "Sydney"), ("IN", "Mumbai"),
             ("JP", "Tokyo"), ("BR", "São Paulo"), ("MX", "Mexico City")]

SEARCH_QUERIES = [
    "wireless headphones", "running shoes size 10", "laptop under 1000",
    "organic skincare", "best coffee maker", "gaming keyboard mechanical",
    "yoga mat thick", "air fryer 5qt", "standing desk", "noise cancelling",
    "waterproof jacket", "vitamin c serum", "bluetooth speaker portable",
    "hiking boots", "instant pot 8qt", "mechanical keyboard",
]


def random_user_id() -> str:
    return f"U{random.randint(100000, 999999)}"


def random_session_id() -> str:
    return f"S{uuid.uuid4().hex[:16].upper()}"


def random_device() -> DeviceType:
    return random.choices(
        list(DeviceType),
        weights=[0.55, 0.30, 0.10, 0.05],
        k=1
    )[0]


def random_location() -> tuple[str, str]:
    return random.choice(COUNTRIES)


def base_event_kwargs() -> dict:
    country, city = random_location()
    return dict(
        session_id=random_session_id(),
        user_id=random_user_id(),
        device_type=random_device(),
        ip_address=fake.ipv4_public(),
        user_agent=fake.user_agent(),
        country=country,
        city=city,
    )


# ── Individual event generators ───────────────────────────────────────────────

def generate_user_registered() -> UserRegisteredEvent:
    return UserRegisteredEvent(
        **base_event_kwargs(),
        email=fake.email(),
        first_name=fake.first_name(),
        last_name=fake.last_name(),
        date_of_birth=fake.date_of_birth(minimum_age=18, maximum_age=75).isoformat(),
        gender=random.choice(["M", "F", "NB", None]),
        referral_source=random.choice(REFERRAL_SOURCES),
        marketing_opt_in=random.random() > 0.4,
    )


def generate_product_viewed(user_id: Optional[str] = None,
                             session_id: Optional[str] = None) -> ProductViewedEvent:
    product = random.choice(PRODUCT_CATALOGUE)
    kwargs = base_event_kwargs()
    if user_id:
        kwargs["user_id"] = user_id
    if session_id:
        kwargs["session_id"] = session_id
    return ProductViewedEvent(
        **kwargs,
        product_id=product["id"],
        product_name=product["name"],
        category=product["category"],
        subcategory=product["subcategory"],
        price=product["price"],
        brand=product["brand"],
        view_duration_seconds=random.randint(5, 300),
        image_clicked=random.random() > 0.6,
        recommendation_source=random.choice(
            ["homepage", "pdp_related", "cart_upsell", "email", None, None, None]
        ),
    )


def generate_search_event(user_id: Optional[str] = None) -> SearchEvent:
    kwargs = base_event_kwargs()
    if user_id:
        kwargs["user_id"] = user_id
    results_count = random.randint(0, 500)
    clicked = results_count > 0 and random.random() > 0.4
    return SearchEvent(
        **kwargs,
        query=random.choice(SEARCH_QUERIES),
        filters_applied={
            "price_min": random.choice([None, 10, 50, 100]),
            "price_max": random.choice([None, 100, 500, 1000]),
            "brand": random.choice([None, fake.company()]),
            "rating": random.choice([None, 3, 4, 4.5]),
        },
        results_count=results_count,
        clicked_result_position=random.randint(1, 20) if clicked else None,
        clicked_product_id=random.choice(PRODUCT_CATALOGUE)["id"] if clicked else None,
    )


def generate_cart_event(event_type: EventType,
                         user_id: Optional[str] = None,
                         session_id: Optional[str] = None) -> CartEvent:
    product = random.choice(PRODUCT_CATALOGUE)
    quantity = random.randint(1, 5)
    kwargs = base_event_kwargs()
    if user_id:
        kwargs["user_id"] = user_id
    if session_id:
        kwargs["session_id"] = session_id
    return CartEvent(
        **kwargs,
        event_type=event_type,
        product_id=product["id"],
        product_name=product["name"],
        quantity=quantity,
        unit_price=product["price"],
        cart_total=round(product["price"] * quantity * random.uniform(1, 3), 2),
        cart_item_count=random.randint(1, 10),
    )


def generate_order_event(user_id: Optional[str] = None,
                          session_id: Optional[str] = None) -> OrderEvent:
    kwargs = base_event_kwargs()
    if user_id:
        kwargs["user_id"] = user_id
    if session_id:
        kwargs["session_id"] = session_id

    n_items = random.randint(1, 8)
    items = []
    subtotal = 0.0
    for _ in range(n_items):
        product = random.choice(PRODUCT_CATALOGUE)
        qty = random.randint(1, 3)
        price = product["price"]
        discount = round(price * random.choice([0, 0, 0, 0.05, 0.10, 0.20]), 2)
        total = round((price - discount) * qty, 2)
        subtotal += total
        items.append(OrderItem(
            product_id=product["id"],
            product_name=product["name"],
            category=product["category"],
            quantity=qty,
            unit_price=price,
            discount_amount=discount,
            total_price=total,
        ))

    subtotal = round(subtotal, 2)
    tax = round(subtotal * 0.08, 2)
    shipping = round(random.choice([0, 4.99, 7.99, 12.99]), 2)
    coupon_discount = round(subtotal * random.choice([0, 0, 0, 0.05, 0.10]), 2)
    total = round(subtotal + tax + shipping - coupon_discount, 2)

    return OrderEvent(
        **kwargs,
        event_type=EventType.ORDER_PLACED,
        order_id=f"ORD-{uuid.uuid4().hex[:12].upper()}",
        items=items,
        subtotal=subtotal,
        tax_amount=tax,
        shipping_amount=shipping,
        discount_amount=coupon_discount,
        total_amount=total,
        payment_method=random.choice(PAYMENT_METHODS),
        shipping_address_country=kwargs["country"],
        coupon_code=f"SAVE{random.randint(5, 30)}" if coupon_discount > 0 else None,
        is_first_order=random.random() > 0.75,
    )


# ── Session simulator ─────────────────────────────────────────────────────────

def simulate_user_session() -> Generator[dict, None, None]:
    """
    Simulates a realistic user session following a conversion funnel:
      login → search → product_view (x1-5) → add_to_cart (x0-3) → order (x0-1)
    Conversion rates are calibrated to real e-commerce benchmarks:
      - 60% of sessions include a search
      - 80% of sessions include at least one product view
      - 30% add something to cart
      - 3% complete a purchase (industry average)
    """
    user_id = random_user_id()
    session_id = random_session_id()

    if random.random() > 0.95:
        event = generate_user_registered()
        event = event.model_copy(update={"user_id": user_id, "session_id": session_id})
        yield {"topic": "user-events", "event": event.to_dict()}

    if random.random() < 0.60:
        event = generate_search_event(user_id=user_id)
        event = event.model_copy(update={"session_id": session_id})
        yield {"topic": "search-events", "event": event.to_dict()}

    n_views = random.randint(1, 8) if random.random() < 0.80 else 0
    for _ in range(n_views):
        event = generate_product_viewed(user_id=user_id, session_id=session_id)
        yield {"topic": "product-events", "event": event.to_dict()}

    if random.random() < 0.30:
        n_cart = random.randint(1, 3)
        for _ in range(n_cart):
            event = generate_cart_event(
                EventType.CART_ADD, user_id=user_id, session_id=session_id
            )
            yield {"topic": "cart-events", "event": event.to_dict()}

        if random.random() < 0.10:
            event = generate_order_event(user_id=user_id, session_id=session_id)
            yield {"topic": "order-events", "event": event.to_dict()}


def generate_bulk_events(n_sessions: int = 1000) -> List[dict]:
    """Generate n_sessions worth of events for batch testing."""
    events = []
    for _ in range(n_sessions):
        for event in simulate_user_session():
            events.append(event)
    return events


if __name__ == "__main__":
    import json
    events = generate_bulk_events(10)
    for e in events[:5]:
        print(json.dumps(e, indent=2, default=str))
    print(f"\nGenerated {len(events)} events from 10 sessions")
