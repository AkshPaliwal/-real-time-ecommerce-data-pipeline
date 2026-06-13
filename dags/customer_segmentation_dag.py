from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'ecom-analytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecom_analytics",
        user="ecom_user",
        password="ecom123"
    )

def run_segmentation(**context):
    conn = get_conn()
    conn.autocommit = True
    cursor = conn.cursor()

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

    print("✅ Segmentation done!")
    conn.close()

def print_summary(**context):
    conn = get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT segment, COUNT(*) as customers,
        ROUND(AVG(total_spent)::numeric, 2) as avg_spent,
        ROUND(SUM(total_spent)::numeric, 2) as total_revenue
        FROM customer_segments
        GROUP BY segment
        ORDER BY total_revenue DESC
    """)
    results = cursor.fetchall()
    conn.close()

    print("\n📊 Customer Segments:")
    print("-" * 60)
    for row in results:
        print(f"{row[0]:<15} Customers: {row[1]:<8} Avg: ₹{row[2]:<12} Revenue: ₹{row[3]}")

with DAG(
    dag_id='customer_segmentation',
    default_args=default_args,
    description='Daily customer segmentation',
    schedule_interval='0 3 * * *',  # Har raat 3 baje
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['customers', 'segmentation', 'daily']
) as dag:

    t1 = PythonOperator(task_id='run_segmentation', python_callable=run_segmentation)
    t2 = PythonOperator(task_id='print_summary', python_callable=print_summary)

    t1 >> t2
