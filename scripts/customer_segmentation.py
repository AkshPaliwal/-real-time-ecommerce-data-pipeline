import psycopg2
from datetime import datetime

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    database="ecom_analytics",
    user="ecom_user",
    password="ecom123"
)
conn.autocommit = True
cursor = conn.cursor()

print("🔍 Calculating customer segments...")

# Step 1: Calculate RFM for each customer
cursor.execute("""
    INSERT INTO customer_segments 
    (customer_id, total_orders, total_spent, avg_order_value, 
    last_order_date, days_since_last_order, segment)
    
    SELECT 
        customer_id,
        COUNT(*) as total_orders,
        SUM(total_amount) as total_spent,
        AVG(total_amount) as avg_order_value,
        MAX(timestamp) as last_order_date,
        EXTRACT(DAY FROM NOW() - MAX(timestamp)) as days_since_last_order,
        CASE
            WHEN COUNT(*) >= 5 AND SUM(total_amount) >= 100000 THEN 'Champion'
            WHEN COUNT(*) >= 3 AND SUM(total_amount) >= 50000 THEN 'Loyal'
            WHEN COUNT(*) >= 2 AND EXTRACT(DAY FROM NOW() - MAX(timestamp)) <= 1 THEN 'Promising'
            WHEN COUNT(*) = 1 AND EXTRACT(DAY FROM NOW() - MAX(timestamp)) <= 1 THEN 'New'
            WHEN EXTRACT(DAY FROM NOW() - MAX(timestamp)) > 2 THEN 'At Risk'
            ELSE 'Regular'
        END as segment
    FROM orders
    GROUP BY customer_id
    
    ON CONFLICT (customer_id) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_spent = EXCLUDED.total_spent,
        avg_order_value = EXCLUDED.avg_order_value,
        last_order_date = EXCLUDED.last_order_date,
        days_since_last_order = EXCLUDED.days_since_last_order,
        segment = EXCLUDED.segment,
        created_at = NOW()
""")

# Step 2: Show segment summary
cursor.execute("""
    SELECT 
        segment,
        COUNT(*) as customer_count,
        ROUND(AVG(total_spent)::numeric, 2) as avg_spent,
        ROUND(SUM(total_spent)::numeric, 2) as total_revenue
    FROM customer_segments
    GROUP BY segment
    ORDER BY total_revenue DESC
""")

results = cursor.fetchall()

print("\n📊 Customer Segmentation Results:")
print("-" * 60)
print(f"{'Segment':<15} {'Customers':<12} {'Avg Spent':<15} {'Total Revenue'}")
print("-" * 60)
for row in results:
    print(f"{row[0]:<15} {row[1]:<12} ₹{row[2]:<14} ₹{row[3]}")

# Step 3: Total customers
cursor.execute("SELECT COUNT(*) FROM customer_segments")
total = cursor.fetchone()[0]
print(f"\n✅ Total customers segmented: {total}")

conn.close()
