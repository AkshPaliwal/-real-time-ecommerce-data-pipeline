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

def calculate_forecast(**context):
    conn = get_conn()
    cursor = conn.cursor()
    date = context['ds']

    # Last 7 days ka average revenue
    cursor.execute("""
        SELECT 
            AVG(daily_revenue) as avg_revenue,
            STDDEV(daily_revenue) as std_revenue
        FROM (
            SELECT 
                DATE(timestamp) as order_date,
                SUM(total_amount) as daily_revenue
            FROM orders
            WHERE DATE(timestamp) < %s
            AND DATE(timestamp) >= %s::date - INTERVAL '7 days'
            GROUP BY DATE(timestamp)
        ) daily
    """, (date, date))
    
    result = cursor.fetchone()
    avg_revenue = float(result[0] or 0)
    std_revenue = float(result[1] or 0)
    
    # Simple forecast — 7 day moving average with 5% growth factor
    predicted_revenue = avg_revenue * 1.05
    
    print(f"📊 Last 7 days avg revenue: ₹{avg_revenue:,.2f}")
    print(f"📈 Predicted revenue for {date}: ₹{predicted_revenue:,.2f}")
    
    # Aaj ka actual revenue
    cursor.execute("""
        SELECT COALESCE(SUM(total_amount), 0)
        FROM orders
        WHERE DATE(timestamp) = %s
    """, (date,))
    actual_revenue = float(cursor.fetchone()[0])
    
    # Save forecast
    cursor.execute("""
        INSERT INTO revenue_forecast
        (forecast_date, predicted_revenue, avg_last_7_days, actual_revenue)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT DO NOTHING
    """, (date, predicted_revenue, avg_revenue, actual_revenue))
    
    conn.commit()
    conn.close()
    
    print(f"✅ Actual revenue today: ₹{actual_revenue:,.2f}")
    accuracy = 100 - abs((predicted_revenue - actual_revenue) / actual_revenue * 100) if actual_revenue > 0 else 0
    print(f"🎯 Forecast accuracy: {accuracy:.1f}%")
    return predicted_revenue

def show_forecast_report(**context):
    conn = get_conn()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            forecast_date,
            ROUND(predicted_revenue::numeric, 2) as predicted,
            ROUND(actual_revenue::numeric, 2) as actual,
            ROUND(ABS(predicted_revenue - actual_revenue)::numeric, 2) as difference
        FROM revenue_forecast
        ORDER BY forecast_date DESC
        LIMIT 7
    """)
    results = cursor.fetchall()
    conn.close()
    
    print("\n📈 Revenue Forecast Report (Last 7 days):")
    print("-" * 70)
    print(f"{'Date':<15} {'Predicted':<20} {'Actual':<20} {'Difference'}")
    print("-" * 70)
    for row in results:
        print(f"{str(row[0]):<15} ₹{row[1]:<19} ₹{row[2]:<19} ₹{row[3]}")

with DAG(
    dag_id='revenue_forecasting',
    default_args=default_args,
    description='Daily revenue forecasting',
    schedule_interval='0 4 * * *',  # Har raat 4 baje
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['forecasting', 'revenue', 'daily']
) as dag:

    t1 = PythonOperator(task_id='calculate_forecast', python_callable=calculate_forecast)
    t2 = PythonOperator(task_id='show_forecast_report', python_callable=show_forecast_report)

    t1 >> t2
