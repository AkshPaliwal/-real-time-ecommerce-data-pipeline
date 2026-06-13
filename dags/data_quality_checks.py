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

def check_null_values(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    
    cursor.execute("""
        SELECT COUNT(*) FROM orders 
        WHERE DATE(timestamp) = %s
        AND (order_id IS NULL OR customer_id IS NULL 
        OR product_name IS NULL OR total_amount IS NULL)
    """, (date,))
    null_count = cursor.fetchone()[0]
    conn.close()
    
    if null_count > 0:
        raise ValueError(f"❌ Found {null_count} orders with NULL values on {date}!")
    print(f"✅ No NULL values found in orders for {date}")
    return null_count

def check_negative_amounts(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    
    cursor.execute("""
        SELECT COUNT(*) FROM orders
        WHERE DATE(timestamp) = %s
        AND (total_amount <= 0 OR price <= 0 OR quantity <= 0)
    """, (date,))
    bad_count = cursor.fetchone()[0]
    conn.close()
    
    if bad_count > 0:
        raise ValueError(f"❌ Found {bad_count} orders with invalid amounts on {date}!")
    print(f"✅ No negative/zero amounts found for {date}")
    return bad_count

def check_duplicate_orders(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    
    cursor.execute("""
        SELECT COUNT(*) FROM (
            SELECT order_id, COUNT(*) as cnt FROM orders
            WHERE DATE(timestamp) = %s
            GROUP BY order_id HAVING COUNT(*) > 1
        ) duplicates
    """, (date,))
    dup_count = cursor.fetchone()[0]
    conn.close()
    
    if dup_count > 0:
        raise ValueError(f"❌ Found {dup_count} duplicate order_ids on {date}!")
    print(f"✅ No duplicate orders found for {date}")
    return dup_count

def check_payment_order_match(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    
    cursor.execute("""
        SELECT COUNT(*) FROM orders o
        LEFT JOIN payments p ON o.order_id = p.order_id
        WHERE DATE(o.timestamp) = %s
        AND p.payment_id IS NULL
    """, (date,))
    unmatched = cursor.fetchone()[0]
    conn.close()
    
    print(f"⚠️ Orders without payments: {unmatched} on {date}")
    return unmatched

def check_failed_payment_rate(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']
    
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN status='failed' THEN 1 END) * 100.0 / COUNT(*) as fail_rate
        FROM payments
        WHERE DATE(timestamp) = %s
    """, (date,))
    fail_rate = cursor.fetchone()[0] or 0
    conn.close()
    
    if fail_rate > 30:
        raise ValueError(f"❌ High payment failure rate: {fail_rate:.1f}% on {date}!")
    print(f"✅ Payment failure rate: {fail_rate:.1f}% — within acceptable range")
    return float(fail_rate)

def save_quality_report(**context):
    ti = context['ti']
    date = context['ds']
    
    null_count = ti.xcom_pull(task_ids='check_null_values')
    neg_count = ti.xcom_pull(task_ids='check_negative_amounts')
    dup_count = ti.xcom_pull(task_ids='check_duplicate_orders')
    unmatched = ti.xcom_pull(task_ids='check_payment_order_match')
    fail_rate = ti.xcom_pull(task_ids='check_failed_payment_rate')
    
    conn = get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS data_quality_log (
            id SERIAL PRIMARY KEY,
            check_date DATE,
            null_values INTEGER,
            negative_amounts INTEGER,
            duplicate_orders INTEGER,
            unmatched_payments INTEGER,
            payment_fail_rate NUMERIC(5,2),
            status VARCHAR(10),
            created_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    cursor.execute("""
        INSERT INTO data_quality_log
        (check_date, null_values, negative_amounts, duplicate_orders, 
        unmatched_payments, payment_fail_rate, status)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (date, null_count, neg_count, dup_count, unmatched, fail_rate, 'PASSED'))
    
    conn.commit()
    conn.close()
    print(f"✅ Quality report saved for {date}")
    print(f"   Nulls: {null_count} | Negatives: {neg_count} | Duplicates: {dup_count}")
    print(f"   Unmatched payments: {unmatched} | Fail rate: {fail_rate:.1f}%")

with DAG(
    dag_id='data_quality_checks',
    default_args=default_args,
    description='Daily data quality checks',
    schedule_interval='0 2 * * *',  # Har raat 2 baje
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['data-quality', 'monitoring']
) as dag:

    t1 = PythonOperator(task_id='check_null_values', python_callable=check_null_values)
    t2 = PythonOperator(task_id='check_negative_amounts', python_callable=check_negative_amounts)
    t3 = PythonOperator(task_id='check_duplicate_orders', python_callable=check_duplicate_orders)
    t4 = PythonOperator(task_id='check_payment_order_match', python_callable=check_payment_order_match)
    t5 = PythonOperator(task_id='check_failed_payment_rate', python_callable=check_failed_payment_rate)
    t6 = PythonOperator(task_id='save_quality_report', python_callable=save_quality_report)

    [t1, t2, t3, t4, t5] >> t6
