import json
import psycopg2
from kafka import KafkaConsumer
from threading import Thread

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecom_analytics",
    user="ecom_user",
    password="ecom_pass123"
)
conn.autocommit = True
cursor = conn.cursor()

print("✅ PostgreSQL connected!")

def consume_orders():
    consumer = KafkaConsumer(
        'ecom.orders',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='orders-consumer'
    )
    print("📦 Orders consumer started...")
    for msg in consumer:
        data = msg.value
        try:
            cursor.execute("""
                INSERT INTO orders 
                (order_id, customer_id, customer_name, product_id, product_name, 
                category, quantity, price, total_amount, city, status, timestamp)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (order_id) DO NOTHING
            """, (
                data['order_id'], data['customer_id'], data['customer_name'],
                data['product_id'], data['product_name'], data['category'],
                data['quantity'], data['price'], data['total_amount'],
                data['city'], data['status'], data['timestamp']
            ))
            print(f"  ✅ Order saved: {data['order_id']} | {data['product_name']} | ₹{data['total_amount']}")
        except Exception as e:
            print(f"  ❌ Order error: {e}")

def consume_payments():
    consumer = KafkaConsumer(
        'ecom.payments',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='payments-consumer'
    )
    print("💳 Payments consumer started...")
    for msg in consumer:
        data = msg.value
        try:
            cursor.execute("""
                INSERT INTO payments 
                (payment_id, order_id, amount, method, status, timestamp)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (payment_id) DO NOTHING
            """, (
                data['payment_id'], data['order_id'], data['amount'],
                data['method'], data['status'], data['timestamp']
            ))
            print(f"  ✅ Payment saved: {data['payment_id']} | {data['method']} | {data['status']}")
        except Exception as e:
            print(f"  ❌ Payment error: {e}")

def consume_deliveries():
    consumer = KafkaConsumer(
        'ecom.deliveries',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='earliest',
        group_id='deliveries-consumer'
    )
    print("🚚 Deliveries consumer started...")
    for msg in consumer:
        data = msg.value
        try:
            cursor.execute("""
                INSERT INTO deliveries 
                (delivery_id, order_id, city, status, estimated_days, timestamp)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (delivery_id) DO NOTHING
            """, (
                data['delivery_id'], data['order_id'], data['city'],
                data['status'], data['estimated_days'], data['timestamp']
            ))
            print(f"  ✅ Delivery saved: {data['delivery_id']} | {data['city']} | {data['status']}")
        except Exception as e:
            print(f"  ❌ Delivery error: {e}")

# Start all 3 consumers in parallel
threads = [
    Thread(target=consume_orders),
    Thread(target=consume_payments),
    Thread(target=consume_deliveries)
]

for t in threads:
    t.daemon = True
    t.start()

print("\n🚀 All consumers running! Press Ctrl+C to stop\n")

for t in threads:
    t.join()
