from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'ecom-analytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        database="ecom_analytics",
        user="ecom_user",
        password="ecom123"
    )

def calculate_total_orders(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    cursor.execute("SELECT COUNT(*) FROM orders WHERE DATE(timestamp) = %s", (date,))
    total = cursor.fetchone()[0]
    conn.close()
    print(f"✅ Total orders for {date}: {total}")
    return total

def calculate_total_revenue(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    cursor.execute("SELECT COALESCE(SUM(total_amount), 0) FROM orders WHERE DATE(timestamp) = %s", (date,))
    revenue = cursor.fetchone()[0]
    conn.close()
    print(f"✅ Total revenue for {date}: ₹{revenue}")
    return float(revenue)

def get_top_product(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    cursor.execute("""
        SELECT product_name FROM orders
        WHERE DATE(timestamp) = %s
        GROUP BY product_name ORDER BY COUNT(*) DESC LIMIT 1
    """, (date,))
    result = cursor.fetchone()
    conn.close()
    top = result[0] if result else 'N/A'
    print(f"✅ Top product: {top}")
    return top

def get_top_city(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    cursor.execute("""
        SELECT city FROM orders
        WHERE DATE(timestamp) = %s
        GROUP BY city ORDER BY COUNT(*) DESC LIMIT 1
    """, (date,))
    result = cursor.fetchone()
    conn.close()
    top = result[0] if result else 'N/A'
    print(f"✅ Top city: {top}")
    return top

def get_payment_stats(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN status='success' THEN 1 END),
            COUNT(CASE WHEN status='failed' THEN 1 END)
        FROM payments WHERE DATE(timestamp) = %s
    """, (date,))
    result = cursor.fetchone()
    conn.close()
    stats = {'successful': result[0], 'failed': result[1]}
    print(f"✅ Payment stats: {stats}")
    return stats

def save_summary(**context):
    ti = context['ti']
    date = context['ds']

    total_orders = ti.xcom_pull(task_ids='calculate_total_orders')
    total_revenue = ti.xcom_pull(task_ids='calculate_total_revenue')
    top_product = ti.xcom_pull(task_ids='get_top_product')
    top_city = ti.xcom_pull(task_ids='get_top_city')
    payment_stats = ti.xcom_pull(task_ids='get_payment_stats')

    avg_order_value = round(total_revenue / total_orders, 2) if total_orders > 0 else 0

    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO daily_summary 
        (summary_date, total_orders, total_revenue, avg_order_value,
        top_product, top_city, successful_payments, failed_payments)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        date, total_orders, total_revenue, avg_order_value,
        top_product, top_city,
        payment_stats['successful'], payment_stats['failed']
    ))
    conn.commit()
    conn.close()
    print(f"✅ Summary saved! Orders:{total_orders} Revenue:₹{total_revenue} Top:{top_product} City:{top_city}")

with DAG(
    dag_id='daily_sales_summary',
    default_args=default_args,
    description='Daily ecommerce sales summary',
    schedule_interval='0 1 * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['sales', 'daily']
) as dag:

    t1 = PythonOperator(task_id='calculate_total_orders', python_callable=calculate_total_orders)
    t2 = PythonOperator(task_id='calculate_total_revenue', python_callable=calculate_total_revenue)
    t3 = PythonOperator(task_id='get_top_product', python_callable=get_top_product)
    t4 = PythonOperator(task_id='get_top_city', python_callable=get_top_city)
    t5 = PythonOperator(task_id='get_payment_stats', python_callable=get_payment_stats)
    t6 = PythonOperator(task_id='save_summary', python_callable=save_summary)

    [t1, t2, t3, t4, t5] >> t6
