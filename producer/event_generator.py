"""
event_generator.py
Generates realistic fake e-commerce events for the streaming pipeline.
Simulates Indian e-commerce behavior (cities, payment methods, products).
"""

import uuid
import random
import json
from datetime import datetime, timezone
from faker import Faker

fake = Faker("en_IN")  # Indian locale for realistic data

# ─── Static Reference Data ───────────────────────────────────────────────────

PRODUCTS = [
    {"product_id": "PROD-001", "name": "Samsung Galaxy S24",      "category": "Electronics", "price": 55000.00},
    {"product_id": "PROD-002", "name": "Apple AirPods Pro",        "category": "Electronics", "price": 22000.00},
    {"product_id": "PROD-003", "name": "Nike Air Max 270",         "category": "Footwear",    "price": 8500.00},
    {"product_id": "PROD-004", "name": "Adidas Running T-Shirt",   "category": "Clothing",    "price": 1299.00},
    {"product_id": "PROD-005", "name": "Prestige Pressure Cooker", "category": "Kitchen",     "price": 2500.00},
    {"product_id": "PROD-006", "name": "boAt Rockerz 450",         "category": "Electronics", "price": 1999.00},
    {"product_id": "PROD-007", "name": "Levi Jeans 511",           "category": "Clothing",    "price": 3499.00},
    {"product_id": "PROD-008", "name": "Instant Pot Duo",          "category": "Kitchen",     "price": 7500.00},
    {"product_id": "PROD-009", "name": "Puma Sports Shoes",        "category": "Footwear",    "price": 4500.00},
    {"product_id": "PROD-010", "name": "Himalaya Face Wash",       "category": "Beauty",      "price": 299.00},
    {"product_id": "PROD-011", "name": "Kindle Paperwhite",        "category": "Electronics", "price": 14000.00},
    {"product_id": "PROD-012", "name": "Wildcraft Backpack 45L",   "category": "Accessories", "price": 3200.00},
]

INDIAN_CITIES = [
    ("Mumbai",     "Maharashtra"),
    ("Delhi",      "Delhi"),
    ("Bengaluru",  "Karnataka"),
    ("Hyderabad",  "Telangana"),
    ("Chennai",    "Tamil Nadu"),
    ("Kolkata",    "West Bengal"),
    ("Jaipur",     "Rajasthan"),
    ("Pune",       "Maharashtra"),
    ("Ahmedabad",  "Gujarat"),
    ("Lucknow",    "Uttar Pradesh"),
    ("Surat",      "Gujarat"),
    ("Indore",     "Madhya Pradesh"),
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Net Banking", "Cash on Delivery", "EMI"]

# Weighted: UPI is most popular in India
PAYMENT_WEIGHTS = [40, 20, 15, 10, 10, 5]

DEVICES = ["Android", "iOS", "Desktop", "Tablet"]
DEVICE_WEIGHTS = [55, 25, 15, 5]

EVENT_TYPES = [
    "order_placed",
    "payment_success",
    "payment_failed",
    "order_shipped",
    "order_delivered",
    "order_cancelled",
    "cart_abandoned",
]

# ─── Helper: stable user pool (so same users appear repeatedly) ───────────────

USER_POOL = [f"USR-{str(i).zfill(5)}" for i in range(1, 501)]  # 500 users


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:10].upper()}"


# ─── Event Generators ─────────────────────────────────────────────────────────

def generate_order_placed() -> dict:
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    city, state = random.choice(INDIAN_CITIES)
    unit_price = round(product["price"] * random.uniform(0.85, 1.10), 2)  # slight price variation

    return {
        "event_id":         _new_id("EVT"),
        "event_type":       "order_placed",
        "timestamp":        _now_iso(),
        "user_id":          random.choice(USER_POOL),
        "session_id":       _new_id("SESS"),
        "order_id":         _new_id("ORD"),
        "product_id":       product["product_id"],
        "product_name":     product["name"],
        "product_category": product["category"],
        "quantity":         quantity,
        "unit_price":       unit_price,
        "total_amount":     round(unit_price * quantity, 2),
        "payment_method":   random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0],
        "city":             city,
        "state":            state,
        "device":           random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
        "pincode":          fake.postcode(),
    }


def generate_payment_event(order: dict = None) -> dict:
    """Generate a payment event, optionally tied to an existing order."""
    is_success = random.random() > 0.12  # 88% success rate
    method = order["payment_method"] if order else random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS)[0]

    failure_reasons = [
        "Insufficient balance",
        "Card declined by bank",
        "OTP timeout",
        "Network error",
        "Daily limit exceeded",
    ]

    return {
        "event_id":         _new_id("EVT"),
        "event_type":       "payment_success" if is_success else "payment_failed",
        "timestamp":        _now_iso(),
        "payment_id":       _new_id("PAY"),
        "order_id":         order["order_id"] if order else _new_id("ORD"),
        "user_id":          order["user_id"] if order else random.choice(USER_POOL),
        "amount":           order["total_amount"] if order else round(random.uniform(299, 50000), 2),
        "payment_method":   method,
        "payment_status":   "success" if is_success else "failed",
        "failure_reason":   None if is_success else random.choice(failure_reasons),
    }


def generate_shipping_event(order: dict = None) -> dict:
    estimated_days = random.randint(2, 7)
    # 20% chance of delay
    actual_days = estimated_days + (random.randint(1, 4) if random.random() < 0.20 else 0)
    city, state = random.choice(INDIAN_CITIES)

    return {
        "event_id":         _new_id("EVT"),
        "event_type":       "order_shipped",
        "timestamp":        _now_iso(),
        "delivery_id":      _new_id("DEL"),
        "order_id":         order["order_id"] if order else _new_id("ORD"),
        "user_id":          order["user_id"] if order else random.choice(USER_POOL),
        "delivery_status":  "shipped",
        "city":             order.get("city", city) if order else city,
        "state":            order.get("state", state) if order else state,
        "estimated_days":   estimated_days,
        "actual_days":      actual_days,
        "courier_partner":  random.choice(["Delhivery", "BlueDart", "DTDC", "Ekart", "XpressBees"]),
    }


def generate_delivery_event(shipping: dict = None) -> dict:
    return {
        "event_id":         _new_id("EVT"),
        "event_type":       "order_delivered",
        "timestamp":        _now_iso(),
        "delivery_id":      shipping["delivery_id"] if shipping else _new_id("DEL"),
        "order_id":         shipping["order_id"] if shipping else _new_id("ORD"),
        "user_id":          shipping["user_id"] if shipping else random.choice(USER_POOL),
        "delivery_status":  "delivered",
        "city":             shipping["city"] if shipping else random.choice(INDIAN_CITIES)[0],
        "estimated_days":   shipping["estimated_days"] if shipping else 5,
        "actual_days":      shipping["actual_days"] if shipping else 5,
    }


def generate_cancellation_event(order: dict = None) -> dict:
    cancel_reasons = [
        "Changed my mind",
        "Found better price elsewhere",
        "Ordered by mistake",
        "Delivery taking too long",
        "Payment issues",
    ]
    return {
        "event_id":         _new_id("EVT"),
        "event_type":       "order_cancelled",
        "timestamp":        _now_iso(),
        "order_id":         order["order_id"] if order else _new_id("ORD"),
        "user_id":          order["user_id"] if order else random.choice(USER_POOL),
        "cancel_reason":    random.choice(cancel_reasons),
        "refund_amount":    order["total_amount"] if order else 0,
        "refund_method":    "Original payment method",
    }


def generate_cart_abandoned_event() -> dict:
    product = random.choice(PRODUCTS)
    city, state = random.choice(INDIAN_CITIES)
    return {
        "event_id":         _new_id("EVT"),
        "event_type":       "cart_abandoned",
        "timestamp":        _now_iso(),
        "user_id":          random.choice(USER_POOL),
        "session_id":       _new_id("SESS"),
        "product_id":       product["product_id"],
        "product_category": product["category"],
        "cart_value":       round(product["price"] * random.randint(1, 3), 2),
        "city":             city,
        "device":           random.choices(DEVICES, weights=DEVICE_WEIGHTS)[0],
        "abandonment_reason": random.choice([
            "Price too high", "Just browsing", "Distracted", "Payment failed", "Shipping cost"
        ]),
    }


# ─── Main: Generate a single random event ────────────────────────────────────

def generate_random_event() -> dict:
    """
    Generates a random event. Order-placed events are most common.
    Returns (topic, event_dict) tuple.
    """
    roll = random.random()

    if roll < 0.35:
        # 35% — new order placed
        order = generate_order_placed()
        return "ecom.orders", order

    elif roll < 0.55:
        # 20% — payment event (linked to a fresh order)
        order = generate_order_placed()
        payment = generate_payment_event(order)
        return "ecom.payments", payment

    elif roll < 0.65:
        # 10% — cart abandoned
        return "ecom.orders", generate_cart_abandoned_event()

    elif roll < 0.78:
        # 13% — shipment
        order = generate_order_placed()
        shipping = generate_shipping_event(order)
        return "ecom.deliveries", shipping

    elif roll < 0.88:
        # 10% — delivery
        shipping = generate_shipping_event()
        delivery = generate_delivery_event(shipping)
        return "ecom.deliveries", delivery

    else:
        # 12% — cancellation
        order = generate_order_placed()
        cancel = generate_cancellation_event(order)
        return "ecom.orders", cancel


# ─── Quick test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=== Sample Events ===\n")
    for _ in range(5):
        topic, event = generate_random_event()
        print(f"Topic: {topic}")
        print(json.dumps(event, indent=2))
        print("-" * 60)
