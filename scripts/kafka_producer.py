import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker('en_IN')

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

PRODUCTS = [
    {"id": "P001", "name": "iPhone 15", "price": 79999, "category": "Electronics"},
    {"id": "P002", "name": "Nike Shoes", "price": 4999, "category": "Fashion"},
    {"id": "P003", "name": "Yoga Mat", "price": 999, "category": "Sports"},
    {"id": "P004", "name": "Laptop Stand", "price": 1499, "category": "Accessories"},
    {"id": "P005", "name": "Coffee Maker", "price": 2999, "category": "Kitchen"},
]

CITIES = ["Mumbai", "Delhi", "Bangalore", "Jaipur", "Pune", "Hyderabad", "Chennai"]

def generate_order():
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 3)
    return {
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "customer_name": fake.name(),
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "quantity": quantity,
        "price": product["price"],
        "total_amount": product["price"] * quantity,
        "city": random.choice(CITIES),
        "status": random.choice(["placed", "confirmed", "processing"]),
        "timestamp": datetime.now().isoformat()
    }

def generate_payment(order):
    return {
        "payment_id": f"PAY-{random.randint(10000, 99999)}",
        "order_id": order["order_id"],
        "amount": order["total_amount"],
        "method": random.choice(["UPI", "Credit Card", "Debit Card", "Net Banking", "COD"]),
        "status": random.choice(["success", "success", "success", "failed"]),
        "timestamp": datetime.now().isoformat()
    }

def generate_delivery(order):
    return {
        "delivery_id": f"DEL-{random.randint(10000, 99999)}",
        "order_id": order["order_id"],
        "city": order["city"],
        "status": random.choice(["dispatched", "in_transit", "out_for_delivery"]),
        "estimated_days": random.randint(1, 7),
        "timestamp": datetime.now().isoformat()
    }

print("🚀 Kafka Producer started! Sending data every 2 seconds...")
print("Press Ctrl+C to stop\n")

count = 0
while True:
    order = generate_order()
    payment = generate_payment(order)
    delivery = generate_delivery(order)

    producer.send('ecom.orders', order)
    producer.send('ecom.payments', payment)
    producer.send('ecom.deliveries', delivery)
    producer.flush()

    count += 1
    print(f"[{count}] Sent → Order: {order['order_id']} | {order['product_name']} | ₹{order['total_amount']} | {order['city']}")
    time.sleep(2)
