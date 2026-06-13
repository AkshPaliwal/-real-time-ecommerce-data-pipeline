"""
spark_streaming_job.py
Consumes events from Kafka → writes Bronze (raw) → transforms to Silver (clean).
Runs as a continuous streaming job using PySpark Structured Streaming.

Run with:
    python streaming/spark_streaming_job.py
"""

import os
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, TimestampType
)

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

KAFKA_SERVERS  = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
POSTGRES_HOST  = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT  = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB    = os.getenv("POSTGRES_DB", "ecom_analytics")
POSTGRES_USER  = os.getenv("POSTGRES_USER", "ecom_user")
POSTGRES_PASS  = os.getenv("POSTGRES_PASSWORD", "ecom_pass123")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

JDBC_PROPS = {
    "user":     POSTGRES_USER,
    "password": POSTGRES_PASS,
    "driver":   "org.postgresql.Driver",
}

KAFKA_TOPICS = "ecom.orders,ecom.payments,ecom.deliveries"

# ─── Spark Session ────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("EcomAnalytics-Streaming")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.postgresql:postgresql:42.7.1")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/ecom_checkpoints")
        .config("spark.sql.shuffle.partitions", "4")   # Keep low for local dev
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .master("local[*]")
        .getOrCreate()
    )


# ─── Schemas ──────────────────────────────────────────────────────────────────

ORDER_SCHEMA = StructType([
    StructField("event_id",         StringType()),
    StructField("event_type",       StringType()),
    StructField("timestamp",        StringType()),
    StructField("user_id",          StringType()),
    StructField("session_id",       StringType()),
    StructField("order_id",         StringType()),
    StructField("product_id",       StringType()),
    StructField("product_name",     StringType()),
    StructField("product_category", StringType()),
    StructField("quantity",         IntegerType()),
    StructField("unit_price",       DoubleType()),
    StructField("total_amount",     DoubleType()),
    StructField("payment_method",   StringType()),
    StructField("city",             StringType()),
    StructField("state",            StringType()),
    StructField("device",           StringType()),
    StructField("pincode",          StringType()),
    StructField("cancel_reason",    StringType()),
    StructField("cart_value",       DoubleType()),
])

PAYMENT_SCHEMA = StructType([
    StructField("event_id",         StringType()),
    StructField("event_type",       StringType()),
    StructField("timestamp",        StringType()),
    StructField("payment_id",       StringType()),
    StructField("order_id",         StringType()),
    StructField("user_id",          StringType()),
    StructField("amount",           DoubleType()),
    StructField("payment_method",   StringType()),
    StructField("payment_status",   StringType()),
    StructField("failure_reason",   StringType()),
])

DELIVERY_SCHEMA = StructType([
    StructField("event_id",         StringType()),
    StructField("event_type",       StringType()),
    StructField("timestamp",        StringType()),
    StructField("delivery_id",      StringType()),
    StructField("order_id",         StringType()),
    StructField("user_id",          StringType()),
    StructField("delivery_status",  StringType()),
    StructField("city",             StringType()),
    StructField("state",            StringType()),
    StructField("estimated_days",   IntegerType()),
    StructField("actual_days",      IntegerType()),
    StructField("courier_partner",  StringType()),
])


# ─── Write to PostgreSQL (batch writer for foreachBatch) ──────────────────────

def write_to_postgres(df, table: str, mode: str = "append"):
    """Write a DataFrame to PostgreSQL via JDBC."""
    if df.isEmpty():
        return
    (
        df.write
        .format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASS)
        .option("driver", "org.postgresql.Driver")
        .mode(mode)
        .save()
    )
    log.info(f"✅ Written {df.count()} rows to {table}")


# ─── Bronze Writer ────────────────────────────────────────────────────────────

def write_bronze(batch_df, batch_id: int):
    """Write raw Kafka messages to bronze.raw_events."""
    log.info(f"[Bronze] Processing batch {batch_id}")

    bronze_df = batch_df.select(
        F.col("parsed.event_id").alias("event_id"),
        F.col("parsed.event_type").alias("event_type"),
        F.col("value").cast("string").alias("raw_payload"),   # raw JSON string
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.current_timestamp().alias("ingested_at"),
    ).dropDuplicates(["event_id"])

    write_to_postgres(bronze_df, "bronze.raw_events")


# ─── Silver Writers ───────────────────────────────────────────────────────────

def write_silver_orders(batch_df, batch_id: int):
    """Write cleaned order events to silver.orders."""
    orders_df = (
        batch_df
        .filter(F.col("parsed.event_type") == "order_placed")
        .filter(F.col("parsed.order_id").isNotNull())
        .filter(F.col("parsed.total_amount") > 0)
        .select(
            F.col("parsed.order_id").alias("order_id"),
            F.col("parsed.event_id").alias("event_id"),
            F.col("parsed.user_id").alias("user_id"),
            F.col("parsed.session_id").alias("session_id"),
            F.col("parsed.product_id").alias("product_id"),
            F.col("parsed.product_category").alias("product_category"),
            F.col("parsed.quantity").cast(IntegerType()).alias("quantity"),
            F.col("parsed.unit_price").cast("decimal(10,2)").alias("unit_price"),
            F.col("parsed.total_amount").cast("decimal(12,2)").alias("total_amount"),
            F.col("parsed.payment_method").alias("payment_method"),
            F.col("parsed.city").alias("city"),
            F.col("parsed.state").alias("state"),
            F.col("parsed.device").alias("device"),
            F.lit("placed").alias("order_status"),
            F.to_timestamp(F.col("parsed.timestamp")).alias("event_time"),
            F.current_timestamp().alias("processed_at"),
            F.lit(True).alias("is_valid"),
        )
        .dropDuplicates(["order_id"])
    )

    if not orders_df.isEmpty():
        write_to_postgres(orders_df, "silver.orders")


def write_silver_payments(batch_df, batch_id: int):
    """Write payment events to silver.payments."""
    payments_df = (
        batch_df
        .filter(F.col("parsed.event_type").isin("payment_success", "payment_failed"))
        .filter(F.col("parsed.payment_id").isNotNull())
        .select(
            F.col("parsed.payment_id").alias("payment_id"),
            F.col("parsed.order_id").alias("order_id"),
            F.col("parsed.user_id").alias("user_id"),
            F.col("parsed.amount").cast("decimal(12,2)").alias("amount"),
            F.col("parsed.payment_method").alias("payment_method"),
            F.col("parsed.payment_status").alias("payment_status"),
            F.col("parsed.failure_reason").alias("failure_reason"),
            F.to_timestamp(F.col("parsed.timestamp")).alias("event_time"),
            F.current_timestamp().alias("processed_at"),
        )
        .dropDuplicates(["payment_id"])
    )

    if not payments_df.isEmpty():
        write_to_postgres(payments_df, "silver.payments")


def write_silver_deliveries(batch_df, batch_id: int):
    """Write delivery events to silver.deliveries."""
    deliveries_df = (
        batch_df
        .filter(F.col("parsed.event_type").isin("order_shipped", "order_delivered"))
        .filter(F.col("parsed.delivery_id").isNotNull())
        .select(
            F.col("parsed.delivery_id").alias("delivery_id"),
            F.col("parsed.order_id").alias("order_id"),
            F.col("parsed.user_id").alias("user_id"),
            F.col("parsed.delivery_status").alias("delivery_status"),
            F.col("parsed.city").alias("city"),
            F.col("parsed.estimated_days").cast(IntegerType()).alias("estimated_days"),
            F.col("parsed.actual_days").cast(IntegerType()).alias("actual_days"),
            F.to_timestamp(F.col("parsed.timestamp")).alias("event_time"),
            F.current_timestamp().alias("processed_at"),
        )
        .dropDuplicates(["delivery_id"])
    )

    if not deliveries_df.isEmpty():
        write_to_postgres(deliveries_df, "silver.deliveries")


# ─── Combined foreachBatch handler ────────────────────────────────────────────

def process_batch(batch_df, batch_id: int):
    """Single handler — routes events to Bronze + Silver tables."""
    log.info(f"⚡ Processing batch {batch_id} | rows: {batch_df.count()}")

    # Parse the JSON value column
    parsed_df = batch_df.withColumn(
        "parsed",
        F.from_json(F.col("value").cast("string"), ORDER_SCHEMA)   # broad schema handles all types
    )

    write_bronze(parsed_df, batch_id)
    write_silver_orders(parsed_df, batch_id)
    write_silver_payments(parsed_df, batch_id)
    write_silver_deliveries(parsed_df, batch_id)


# ─── Main ─────────────────────────────────────────────────────────────────────

def run():
    log.info("🚀 Starting PySpark Structured Streaming job...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Read from all Kafka topics
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPICS)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    log.info(f"📡 Subscribed to topics: {KAFKA_TOPICS}")

    # Start the streaming query
    query = (
        kafka_df.writeStream
        .foreachBatch(process_batch)
        .trigger(processingTime="10 seconds")   # process every 10s
        .option("checkpointLocation", "/tmp/ecom_checkpoints/main")
        .start()
    )

    log.info("✅ Streaming query started. Waiting for data...")
    query.awaitTermination()


if __name__ == "__main__":
    run()
