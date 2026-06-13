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

def create_hourly_table(**context):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS hourly_stats (
            id SERIAL PRIMARY KEY,
            stat_date DATE,
            stat_hour INTEGER,
            total_orders INTEGER,
            total_revenue NUMERIC(12,2),
            successful_payments INTEGER,
            failed_payments INTEGER,
            top_product VARCHAR(100),
            top_city VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    conn.commit()
    conn.close()
    print("✅ hourly_stats table ready!")

def calculate_hourly_stats(**context):
    conn = get_conn()
    cursor = conn.cursor()
    
    execution_time = context['execution_date']
    stat_date = execution_time.date()
    stat_hour = execution_time.hour

    # Total orders this hour
    cursor.execute("""
        SELECT COUNT(*), COALESCE(SUM(total_amount), 0)
        FROM orders
        WHERE DATE(timestamp) = %s
        AND EXTRACT(HOUR FROM timestamp) = %s
    """, (stat_date, stat_hour))
    orders_result = cursor.fetchone()
    total_orders = orders_result[0]
    total_revenue = float(orders_result[1])

    # Payment stats
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN status='success' THEN 1 END),
            COUNT(CASE WHEN status='failed' THEN 1 END)
        FROM payments
        WHERE DATE(timestamp) = %s
        AND EXTRACT(HOUR FROM timestamp) = %s
    """, (stat_date, stat_hour))
    pay_result = cursor.fetchone()
    successful_payments = pay_result[0]
    failed_payments = pay_result[1]

    # Top product this hour
    cursor.execute("""
        SELECT product_name FROM orders
        WHERE DATE(timestamp) = %s
        AND EXTRACT(HOUR FROM timestamp) = %s
        GROUP BY product_name
        ORDER BY COUNT(*) DESC LIMIT 1
    """, (stat_date, stat_hour))
    top_product_result = cursor.fetchone()
    top_product = top_product_result[0] if top_product_result else 'N/A'

    # Top city this hour
    cursor.execute("""
        SELECT city FROM orders
        WHERE DATE(timestamp) = %s
        AND EXTRACT(HOUR FROM timestamp) = %s
        GROUP BY city
        ORDER BY COUNT(*) DESC LIMIT 1
    """, (stat_date, stat_hour))
    top_city_result = cursor.fetchone()
    top_city = top_city_result[0] if top_city_result else 'N/A'

    # Save to hourly_stats
    cursor.execute("""
        INSERT INTO hourly_stats
        (stat_date, stat_hour, total_orders, total_revenue,
        successful_payments, failed_payments, top_product, top_city)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        stat_date, stat_hour, total_orders, total_revenue,
        successful_payments, failed_payments, top_product, top_city
    ))
    conn.commit()
    conn.close()

    print(f"✅ Hourly stats saved for {stat_date} hour {stat_hour}")
    print(f"   Orders: {total_orders} | Revenue: ₹{total_revenue}")
    print(f"   Payments: {successful_payments} success, {failed_payments} failed")
    print(f"   Top: {top_product} | {top_city}")

with DAG(
    dag_id='hourly_stats',
    default_args=default_args,
    description='Hourly ecommerce stats',
    schedule_interval='0 * * * *',  # Har ghante
    start_date=datetime(2026, 6, 13),
    catchup=False,
    tags=['hourly', 'stats']
) as dag:

    t1 = PythonOperator(task_id='create_hourly_table', python_callable=create_hourly_table)
    t2 = PythonOperator(task_id='calculate_hourly_stats', python_callable=calculate_hourly_stats)

    t1 >> t2
