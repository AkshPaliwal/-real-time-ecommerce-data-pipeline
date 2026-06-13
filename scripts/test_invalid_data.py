import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Invalid orders
invalid_orders = [
    {"order_id": "", "customer_id": "CUST-1", "product_name": "iPhone", "price": 999, "total_amount": 999, "quantity": 1, "status": "placed"},  # empty order_id
    {"order_id": "ORD-BAD1", "customer_id": "CUST-1", "product_name": "iPhone", "price": -500, "total_amount": -500, "quantity": 1, "status": "placed"},  # negative price
    {"order_id": "ORD-BAD2", "customer_id": "CUST-1", "product_name": "iPhone", "price": 999, "total_amount": 999, "quantity": 0, "status": "placed"},  # zero quantity
    {"order_id": "ORD-BAD3", "customer_id": "CUST-1", "product_name": "iPhone", "price": 999, "total_amount": 999, "quantity": 1, "status": "shipped"},  # invalid status
]

for order in invalid_orders:
    producer.send('ecom.orders', order)
    print(f"Sent invalid order: {order.get('order_id') or 'EMPTY'}")

producer.flush()
print("\n✅ Invalid test data sent!")
