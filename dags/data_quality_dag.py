"""
data_quality_dag.py
Airflow DAG — runs data quality checks on Bronze and Silver layers.
Logs results to public.dq_run_log and alerts on failures.
"""

from datetime import datetime, timedelta
import logging
import uuid

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

default_args = {
    "owner":            "ecom-analytics",
    "depends_on_past":  False,
    "start_date":       datetime(2024, 1, 1),
    "retries":          1,
    "retry_delay":      timedelta(minutes=3),
}

dag = DAG(
    dag_id="data_quality_checks",
    default_args=default_args,
    description="Automated data quality checks on Bronze and Silver layers",
    schedule_interval="0 */6 * * *",    # Every 6 hours
    catchup=False,
    tags=["data-quality", "monitoring"],
)

POSTGRES_CONN_ID = "ecom_postgres"


# ─── DQ Check Runner ─────────────────────────────────────────────────────────

def run_check(hook, run_id, layer, table, check_name, sql_count, sql_failed):
    """
    Executes a quality check and logs the result.
    sql_count  → total records checked
    sql_failed → records that failed the check
    """
    total   = hook.get_first(sql_count)[0] or 0
    failed  = hook.get_first(sql_failed)[0] or 0
    status  = "passed" if failed == 0 else ("warning" if failed / max(total, 1) < 0.05 else "failed")

    hook.run("""
        INSERT INTO public.dq_run_log
            (run_id, layer, table_name, check_name, status, records_checked, records_failed)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, parameters=(run_id, layer, table, check_name, status, total, failed))

    icon = "✅" if status == "passed" else ("⚠️" if status == "warning" else "❌")
    log.info(f"{icon} [{layer}] {table} | {check_name} | {status} | {failed}/{total} failed")

    if status == "failed":
        raise ValueError(f"DQ FAILURE: {table}.{check_name} — {failed}/{total} records failed")

    return status


# ─── Bronze Layer Checks ──────────────────────────────────────────────────────

def check_bronze_layer(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    run_id = str(uuid.uuid4())[:8]

    # 1. No null event_ids
    run_check(hook, run_id, "bronze", "raw_events", "no_null_event_ids",
        "SELECT COUNT(*) FROM bronze.raw_events WHERE ingested_at >= NOW() - INTERVAL '6 hours'",
        "SELECT COUNT(*) FROM bronze.raw_events WHERE event_id IS NULL AND ingested_at >= NOW() - INTERVAL '6 hours'",
    )

    # 2. No null event types
    run_check(hook, run_id, "bronze", "raw_events", "no_null_event_types",
        "SELECT COUNT(*) FROM bronze.raw_events WHERE ingested_at >= NOW() - INTERVAL '6 hours'",
        "SELECT COUNT(*) FROM bronze.raw_events WHERE event_type IS NULL AND ingested_at >= NOW() - INTERVAL '6 hours'",
    )

    # 3. Valid event types only
    run_check(hook, run_id, "bronze", "raw_events", "valid_event_types",
        "SELECT COUNT(*) FROM bronze.raw_events WHERE ingested_at >= NOW() - INTERVAL '6 hours'",
        """SELECT COUNT(*) FROM bronze.raw_events
           WHERE event_type NOT IN (
               'order_placed','payment_success','payment_failed',
               'order_shipped','order_delivered','order_cancelled','cart_abandoned'
           ) AND ingested_at >= NOW() - INTERVAL '6 hours'""",
    )

    # 4. Data freshness — should have events in last 30 minutes during operating hours
    recent_count = hook.get_first(
        "SELECT COUNT(*) FROM bronze.raw_events WHERE ingested_at >= NOW() - INTERVAL '30 minutes'"
    )[0]
    status = "passed" if recent_count > 0 else "warning"
    hook.run("""
        INSERT INTO public.dq_run_log (run_id, layer, table_name, check_name, status, records_checked, records_failed)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, parameters=(run_id, "bronze", "raw_events", "data_freshness_30min", status, recent_count, 0))
    log.info(f"{'✅' if status == 'passed' else '⚠️'} Freshness check: {recent_count} events in last 30min")


# ─── Silver Layer Checks ──────────────────────────────────────────────────────

def check_silver_orders(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    run_id = str(uuid.uuid4())[:8]

    checks = [
        # (check_name, total_sql, failed_sql)
        (
            "no_null_user_id",
            "SELECT COUNT(*) FROM silver.orders WHERE processed_at >= NOW() - INTERVAL '6 hours'",
            "SELECT COUNT(*) FROM silver.orders WHERE user_id IS NULL AND processed_at >= NOW() - INTERVAL '6 hours'",
        ),
        (
            "no_negative_amounts",
            "SELECT COUNT(*) FROM silver.orders WHERE processed_at >= NOW() - INTERVAL '6 hours'",
            "SELECT COUNT(*) FROM silver.orders WHERE total_amount < 0 AND processed_at >= NOW() - INTERVAL '6 hours'",
        ),
        (
            "valid_quantity",
            "SELECT COUNT(*) FROM silver.orders WHERE processed_at >= NOW() - INTERVAL '6 hours'",
            "SELECT COUNT(*) FROM silver.orders WHERE quantity <= 0 AND processed_at >= NOW() - INTERVAL '6 hours'",
        ),
        (
            "no_future_timestamps",
            "SELECT COUNT(*) FROM silver.orders WHERE processed_at >= NOW() - INTERVAL '6 hours'",
            "SELECT COUNT(*) FROM silver.orders WHERE event_time > NOW() AND processed_at >= NOW() - INTERVAL '6 hours'",
        ),
        (
            "no_duplicate_order_ids",
            "SELECT COUNT(DISTINCT order_id) FROM silver.orders WHERE processed_at >= NOW() - INTERVAL '6 hours'",
            """SELECT COUNT(*) FROM (
                SELECT order_id FROM silver.orders
                WHERE processed_at >= NOW() - INTERVAL '6 hours'
                GROUP BY order_id HAVING COUNT(*) > 1
               ) dupes""",
        ),
    ]

    for check_name, total_sql, failed_sql in checks:
        run_check(hook, run_id, "silver", "orders", check_name, total_sql, failed_sql)


def check_silver_payments(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    run_id = str(uuid.uuid4())[:8]

    # Payment failure rate should not exceed 30%
    result = hook.get_first("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN payment_status = 'failed' THEN 1 ELSE 0 END) AS failures
        FROM silver.payments
        WHERE processed_at >= NOW() - INTERVAL '6 hours'
    """)
    total, failures = result[0] or 0, result[1] or 0
    failure_rate = (failures / total * 100) if total > 0 else 0
    status = "passed" if failure_rate < 30 else "warning" if failure_rate < 50 else "failed"

    hook.run("""
        INSERT INTO public.dq_run_log (run_id, layer, table_name, check_name, status, records_checked, records_failed)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, parameters=(run_id, "silver", "payments", "payment_failure_rate_check", status, total, failures))

    log.info(f"{'✅' if status=='passed' else '⚠️'} Payment failure rate: {failure_rate:.1f}%")


# ─── Summary Report ───────────────────────────────────────────────────────────

def generate_dq_summary(**context):
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    summary = hook.get_records("""
        SELECT status, COUNT(*) AS check_count
        FROM public.dq_run_log
        WHERE run_at >= NOW() - INTERVAL '7 hours'
        GROUP BY status
        ORDER BY status
    """)

    log.info("=" * 50)
    log.info("📋 DATA QUALITY SUMMARY (last 6 hours)")
    for row in summary:
        icon = {"passed": "✅", "warning": "⚠️", "failed": "❌"}.get(row[0], "❓")
        log.info(f"  {icon} {row[0].upper()}: {row[1]} checks")
    log.info("=" * 50)


# ─── Tasks ────────────────────────────────────────────────────────────────────

t1 = PythonOperator(task_id="check_bronze_layer",    python_callable=check_bronze_layer,    dag=dag)
t2 = PythonOperator(task_id="check_silver_orders",   python_callable=check_silver_orders,   dag=dag)
t3 = PythonOperator(task_id="check_silver_payments", python_callable=check_silver_payments, dag=dag)
t4 = PythonOperator(task_id="dq_summary_report",     python_callable=generate_dq_summary,   dag=dag)

t1 >> [t2, t3] >> t4
