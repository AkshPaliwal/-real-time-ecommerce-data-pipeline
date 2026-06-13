import json
import psycopg2
from kafka import KafkaConsumer
from threading import Thread
from datetime import datetime

# PostgreSQL connection
conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecom_analytics",
    user="ecom_user",
    password="ecom123"
)
conn.autocommit = True
cursor = conn.cursor()

print("✅ PostgreSQL connected!")

# ─── Validation Functions ───────────────────────────────────────

def save_rejected(topic, data, reason):
    try:
        cursor.execute("""
            INSERT INTO rejected_records (topic, record_data, rejection_reason)
            VALUES (%s, %s, %s)
        """, (topic, json.dumps(data), reason))
        print(f"  ⛔ REJECTED [{topic}] — {reason}")
    except Exception as e:
        print(f"  ❌ Error saving rejected record: {e}")

def validate_order(data):
    if not data.get('order_id'):
        return False, "Missing order_id"
    if not data.get('customer_id'):
        return False, "Missing customer_id"
    if not data.get('product_name'):
        return False, "Missing product_name"
    if float(data.get('price', 0)) <= 0:
        return False, "Invalid price — must be > 0"
    if float(data.get('total_amount', 0)) <= 0:
        return False, "Invalid total_amount — must be > 0"
    if int(data.get('quantity', 0)) <= 0:
        return False, "Invalid quantity — must be > 0"
    if data.get('status') not in ['placed', 'confirmed', 'processing']:
        return False, f"Invalid status: {data.get('status')}"
    return True, "OK"

def validate_payment(data):
    if not data.get('payment_id'):
        return False, "Missing payment_id"
    if not data.get('order_id'):
        return False, "Missing order_id"
    if float(data.get('amount', 0)) <= 0:
        return False, "Invalid amount — must be > 0"
    if data.get('method') not in ['UPI', 'Credit Card', 'Debit Card', 'Net Banking', 'COD']:
        return False, f"Invalid payment method: {data.get('method')}"
    if data.get('status') not in ['success', 'failed']:
        return False, f"Invalid payment status: {data.get('status')}"
    return True, "OK"

def validate_delivery(data):
    if not data.get('delivery_id'):
        return False, "Missing delivery_id"
    if not data.get('order_id'):
        return False, "Missing order_id"
    if not data.get('city'):
        return False, "Missing city"
    if int(data.get('estimated_days', 0)) <= 0:
        return False, "Invalid estimated_days — must be > 0"
    if data.get('status') not in ['dispatched', 'in_transit', 'out_for_delivery']:
        return False, f"Invalid delivery status: {data.get('status')}"
    return True, "OK"

# ─── Consumers ──────────────────────────────────────────────────

def consume_orders():
    consumer = KafkaConsumer(
        'ecom.orders',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='orders-validated-consumer'
    )
    print("📦 Orders validated consumer started...")
    for msg in consumer:
        data = msg.value
        is_valid, reason = validate_order(data)
        if not is_valid:
            save_rejected('ecom.orders', data, reason)
            continue
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
            print(f"  ❌ DB Error: {e}")

def consume_payments():
    consumer = KafkaConsumer(
        'ecom.payments',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='payments-validated-consumer'
    )
    print("💳 Payments validated consumer started...")
    for msg in consumer:
        data = msg.value
        is_valid, reason = validate_payment(data)
        if not is_valid:
            save_rejected('ecom.payments', data, reason)
            continue
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
            print(f"  ❌ DB Error: {e}")

def consume_deliveries():
    consumer = KafkaConsumer(
        'ecom.deliveries',
        bootstrap_servers='localhost:9092',
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='deliveries-validated-consumer'
    )
    print("🚚 Deliveries validated consumer started...")
    for msg in consumer:
        data = msg.value
        is_valid, reason = validate_delivery(data)
        if not is_valid:
            save_rejected('ecom.deliveries', data, reason)
            continue
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
            print(f"  ❌ DB Error: {e}")

# ─── Start All ──────────────────────────────────────────────────

threads = [
    Thread(target=consume_orders),
    Thread(target=consume_payments),
    Thread(target=consume_deliveries)
]

for t in threads:
    t.daemon = True
    t.start()

print("\n🚀 Validated consumers running! Press Ctrl+C to stop\n")

for t in threads:
    t.join()
