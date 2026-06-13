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

def analyze_deliveries(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']

    # City wise delivery analysis
    cursor.execute("""
        SELECT 
            city,
            COUNT(*) as total_deliveries,
            COUNT(CASE WHEN status='dispatched' THEN 1 END) as dispatched,
            COUNT(CASE WHEN status='in_transit' THEN 1 END) as in_transit,
            COUNT(CASE WHEN status='out_for_delivery' THEN 1 END) as out_for_delivery,
            ROUND(AVG(estimated_days)::numeric, 2) as avg_days,
            COUNT(CASE WHEN estimated_days <= 2 THEN 1 END) as fast,
            COUNT(CASE WHEN estimated_days >= 6 THEN 1 END) as slow
        FROM deliveries
        WHERE DATE(timestamp) = %s
        GROUP BY city
        ORDER BY total_deliveries DESC
    """, (date,))

    results = cursor.fetchall()

    print(f"\n🚚 Delivery Analysis for {date}:")
    print("-" * 80)
    print(f"{'City':<12} {'Total':<8} {'Dispatch':<10} {'Transit':<10} {'OFD':<8} {'Avg Days':<10} {'Fast':<6} {'Slow'}")
    print("-" * 80)

    for row in results:
        city, total, dispatched, in_transit, ofd, avg_days, fast, slow = row
        print(f"{city:<12} {total:<8} {dispatched:<10} {in_transit:<10} {ofd:<8} {avg_days:<10} {fast:<6} {slow}")

        cursor.execute("""
            INSERT INTO delivery_analysis
            (analysis_date, city, total_deliveries, dispatched_count,
            in_transit_count, out_for_delivery_count, avg_estimated_days,
            fast_deliveries, slow_deliveries)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (date, city, total, dispatched, in_transit, ofd, avg_days, fast, slow))

    conn.commit()
    conn.close()
    print(f"\n✅ Delivery analysis saved for {date}!")

def show_delivery_insights(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']

    # Sabse fast city
    cursor.execute("""
        SELECT city, avg_estimated_days, total_deliveries
        FROM delivery_analysis
        WHERE analysis_date = %s
        ORDER BY avg_estimated_days ASC LIMIT 1
    """, (date,))
    fastest = cursor.fetchone()

    # Sabse slow city
    cursor.execute("""
        SELECT city, avg_estimated_days, total_deliveries
        FROM delivery_analysis
        WHERE analysis_date = %s
        ORDER BY avg_estimated_days DESC LIMIT 1
    """, (date,))
    slowest = cursor.fetchone()

    # Overall stats
    cursor.execute("""
        SELECT 
            SUM(total_deliveries),
            SUM(fast_deliveries),
            SUM(slow_deliveries),
            ROUND(AVG(avg_estimated_days)::numeric, 2)
        FROM delivery_analysis
        WHERE analysis_date = %s
    """, (date,))
    overall = cursor.fetchone()
    conn.close()

    print(f"\n📊 Delivery Insights for {date}:")
    print(f"   Total Deliveries: {overall[0]}")
    print(f"   Fast (<=2 days): {overall[1]}")
    print(f"   Slow (>=6 days): {overall[2]}")
    print(f"   Overall Avg Days: {overall[3]}")
    if fastest:
        print(f"   🏆 Fastest City: {fastest[0]} ({fastest[1]} days avg)")
    if slowest:
        print(f"   🐢 Slowest City: {slowest[0]} ({slowest[1]} days avg)")

with DAG(
    dag_id='delivery_time_analysis',
    default_args=default_args,
    description='Daily delivery time analysis by city',
    schedule_interval='0 5 * * *',  # Har raat 5 baje
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['delivery', 'analysis', 'daily']
) as dag:

    t1 = PythonOperator(task_id='analyze_deliveries', python_callable=analyze_deliveries)
    t2 = PythonOperator(task_id='show_delivery_insights', python_callable=show_delivery_insights)

    t1 >> t2
