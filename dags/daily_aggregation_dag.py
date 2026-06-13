"""
daily_aggregation_dag.py
Airflow DAG — runs nightly to refresh Gold layer KPI tables.
Aggregates Silver data → Gold tables for dashboard consumption.
"""

from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# ─── DAG Definition ───────────────────────────────────────────────────────────

default_args = {
    "owner":            "ecom-analytics",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

dag = DAG(
    dag_id="daily_gold_aggregation",
    default_args=default_args,
    description="Refreshes Gold layer KPI tables from Silver data",
    schedule_interval="0 1 * * *",    # Every day at 1:00 AM
    catchup=False,
    tags=["gold", "aggregation", "daily"],
)

POSTGRES_CONN_ID = "ecom_postgres"


# ─── SQL: Refresh Hourly Revenue ──────────────────────────────────────────────

REFRESH_HOURLY_REVENUE = """
INSERT INTO gold.hourly_revenue (hour_bucket, product_category, total_revenue, order_count, avg_order_value, refreshed_at)
SELECT
    DATE_TRUNC('hour', event_time)  AS hour_bucket,
    product_category,
    SUM(total_amount)               AS total_revenue,
    COUNT(*)                        AS order_count,
    AVG(total_amount)               AS avg_order_value,
    NOW()                           AS refreshed_at
FROM silver.orders
WHERE
    order_status NOT IN ('cancelled')
    AND event_time >= NOW() - INTERVAL '48 hours'
GROUP BY 1, 2
ON CONFLICT (hour_bucket, product_category)
DO UPDATE SET
    total_revenue   = EXCLUDED.total_revenue,
    order_count     = EXCLUDED.order_count,
    avg_order_value = EXCLUDED.avg_order_value,
    refreshed_at    = EXCLUDED.refreshed_at;
"""

# ─── SQL: Refresh Daily Revenue ───────────────────────────────────────────────

REFRESH_DAILY_REVENUE = """
INSERT INTO gold.daily_revenue (date_bucket, total_revenue, order_count, unique_customers, avg_order_value, top_category, refreshed_at)
SELECT
    DATE(event_time)            AS date_bucket,
    SUM(total_amount)           AS total_revenue,
    COUNT(*)                    AS order_count,
    COUNT(DISTINCT user_id)     AS unique_customers,
    AVG(total_amount)           AS avg_order_value,
    MODE() WITHIN GROUP (ORDER BY product_category) AS top_category,
    NOW()                       AS refreshed_at
FROM silver.orders
WHERE order_status NOT IN ('cancelled')
GROUP BY DATE(event_time)
ON CONFLICT (date_bucket)
DO UPDATE SET
    total_revenue    = EXCLUDED.total_revenue,
    order_count      = EXCLUDED.order_count,
    unique_customers = EXCLUDED.unique_customers,
    avg_order_value  = EXCLUDED.avg_order_value,
    top_category     = EXCLUDED.top_category,
    refreshed_at     = EXCLUDED.refreshed_at;
"""

# ─── SQL: Revenue by City ─────────────────────────────────────────────────────

REFRESH_REVENUE_BY_CITY = """
INSERT INTO gold.revenue_by_city (date_bucket, city, total_revenue, order_count, refreshed_at)
SELECT
    DATE(event_time)    AS date_bucket,
    city,
    SUM(total_amount)   AS total_revenue,
    COUNT(*)            AS order_count,
    NOW()               AS refreshed_at
FROM silver.orders
WHERE order_status NOT IN ('cancelled')
GROUP BY DATE(event_time), city
ON CONFLICT (date_bucket, city)
DO UPDATE SET
    total_revenue = EXCLUDED.total_revenue,
    order_count   = EXCLUDED.order_count,
    refreshed_at  = EXCLUDED.refreshed_at;
"""

# ─── SQL: Payment Failure Metrics ─────────────────────────────────────────────

REFRESH_PAYMENT_FAILURES = """
INSERT INTO gold.payment_failure_metrics (hour_bucket, payment_method, total_attempts, failed_count, failure_rate, refreshed_at)
SELECT
    DATE_TRUNC('hour', event_time)          AS hour_bucket,
    payment_method,
    COUNT(*)                                AS total_attempts,
    SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) AS failed_count,
    ROUND(
        100.0 * SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) / COUNT(*), 2
    )                                       AS failure_rate,
    NOW()                                   AS refreshed_at
FROM silver.payments
WHERE event_time >= NOW() - INTERVAL '48 hours'
GROUP BY 1, 2
ON CONFLICT (hour_bucket, payment_method)
DO UPDATE SET
    total_attempts = EXCLUDED.total_attempts,
    failed_count   = EXCLUDED.failed_count,
    failure_rate   = EXCLUDED.failure_rate,
    refreshed_at   = EXCLUDED.refreshed_at;
"""

# ─── SQL: Delayed Shipments ───────────────────────────────────────────────────

REFRESH_DELAYED_SHIPMENTS = """
INSERT INTO gold.delayed_shipments (date_bucket, city, delayed_count, total_shipments, delay_rate, refreshed_at)
SELECT
    DATE(event_time)            AS date_bucket,
    city,
    SUM(CASE WHEN actual_days > estimated_days THEN 1 ELSE 0 END) AS delayed_count,
    COUNT(*)                    AS total_shipments,
    ROUND(
        100.0 * SUM(CASE WHEN actual_days > estimated_days THEN 1 ELSE 0 END) / COUNT(*), 2
    )                           AS delay_rate,
    NOW()                       AS refreshed_at
FROM silver.deliveries
GROUP BY DATE(event_time), city
ON CONFLICT (date_bucket, city)
DO UPDATE SET
    delayed_count   = EXCLUDED.delayed_count,
    total_shipments = EXCLUDED.total_shipments,
    delay_rate      = EXCLUDED.delay_rate,
    refreshed_at    = EXCLUDED.refreshed_at;
"""

# ─── Python Task: Customer Churn Scoring ─────────────────────────────────────

def refresh_customer_metrics(**context):
    """
    Computes customer lifetime metrics and flags churn risk.
    A customer is 'at risk' if they haven't ordered in 30+ days.
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = """
    INSERT INTO gold.customer_metrics (
        user_id, total_orders, total_spend, avg_order_value,
        first_order_date, last_order_date, days_since_last_order,
        favourite_category, favourite_payment, churn_risk_flag, refreshed_at
    )
    SELECT
        user_id,
        COUNT(*)                                        AS total_orders,
        SUM(total_amount)                               AS total_spend,
        AVG(total_amount)                               AS avg_order_value,
        MIN(DATE(event_time))                           AS first_order_date,
        MAX(DATE(event_time))                           AS last_order_date,
        CURRENT_DATE - MAX(DATE(event_time))            AS days_since_last_order,
        MODE() WITHIN GROUP (ORDER BY product_category) AS favourite_category,
        MODE() WITHIN GROUP (ORDER BY payment_method)   AS favourite_payment,
        (CURRENT_DATE - MAX(DATE(event_time))) > 30     AS churn_risk_flag,
        NOW()                                           AS refreshed_at
    FROM silver.orders
    WHERE order_status NOT IN ('cancelled')
    GROUP BY user_id
    ON CONFLICT (user_id)
    DO UPDATE SET
        total_orders          = EXCLUDED.total_orders,
        total_spend           = EXCLUDED.total_spend,
        avg_order_value       = EXCLUDED.avg_order_value,
        first_order_date      = EXCLUDED.first_order_date,
        last_order_date       = EXCLUDED.last_order_date,
        days_since_last_order = EXCLUDED.days_since_last_order,
        favourite_category    = EXCLUDED.favourite_category,
        favourite_payment     = EXCLUDED.favourite_payment,
        churn_risk_flag       = EXCLUDED.churn_risk_flag,
        refreshed_at          = EXCLUDED.refreshed_at;
    """

    rows = hook.run(sql)
    churn_count = hook.get_first(
        "SELECT COUNT(*) FROM gold.customer_metrics WHERE churn_risk_flag = TRUE"
    )[0]
    log.info(f"✅ Customer metrics refreshed. Churn risk customers: {churn_count}")


# ─── Tasks ────────────────────────────────────────────────────────────────────

t1_hourly_revenue = PostgresOperator(
    task_id="refresh_hourly_revenue",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=REFRESH_HOURLY_REVENUE,
    dag=dag,
)

t2_daily_revenue = PostgresOperator(
    task_id="refresh_daily_revenue",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=REFRESH_DAILY_REVENUE,
    dag=dag,
)

t3_revenue_by_city = PostgresOperator(
    task_id="refresh_revenue_by_city",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=REFRESH_REVENUE_BY_CITY,
    dag=dag,
)

t4_payment_failures = PostgresOperator(
    task_id="refresh_payment_failures",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=REFRESH_PAYMENT_FAILURES,
    dag=dag,
)

t5_delayed_shipments = PostgresOperator(
    task_id="refresh_delayed_shipments",
    postgres_conn_id=POSTGRES_CONN_ID,
    sql=REFRESH_DELAYED_SHIPMENTS,
    dag=dag,
)

t6_customer_metrics = PythonOperator(
    task_id="refresh_customer_metrics",
    python_callable=refresh_customer_metrics,
    dag=dag,
)

# ─── Task Dependencies ────────────────────────────────────────────────────────
# Run revenue tasks in parallel, then customer metrics last

[t1_hourly_revenue, t2_daily_revenue, t3_revenue_by_city,
 t4_payment_failures, t5_delayed_shipments] >> t6_customer_metrics
