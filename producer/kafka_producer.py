"""
kafka_producer.py
Sends generated e-commerce events to Kafka topics.
Runs continuously at a configurable rate.
M2-compatible: uses kafka-python, no JVM dependency here.
"""

import json
import time
import logging
import os
import signal
import sys
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from producer.event_generator import generate_random_event

load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
EVENT_RATE_PER_SECOND   = int(os.getenv("EVENT_RATE_PER_SECOND", "5"))
SLEEP_INTERVAL          = 1.0 / EVENT_RATE_PER_SECOND

# ─── Stats tracker ────────────────────────────────────────────────────────────

stats = {
    "total_sent":   0,
    "total_failed": 0,
    "by_topic":     {},
    "start_time":   datetime.now(),
}


def print_stats():
    elapsed = (datetime.now() - stats["start_time"]).seconds or 1
    rate = stats["total_sent"] / elapsed
    log.info(
        f"📊 Stats | Sent: {stats['total_sent']} | "
        f"Failed: {stats['total_failed']} | "
        f"Rate: {rate:.1f} events/sec | "
        f"Topics: {stats['by_topic']}"
    )


# ─── Callbacks ────────────────────────────────────────────────────────────────

def on_send_success(record_metadata, event_id: str):
    stats["total_sent"] += 1
    topic = record_metadata.topic
    stats["by_topic"][topic] = stats["by_topic"].get(topic, 0) + 1

    if stats["total_sent"] % 50 == 0:
        log.info(
            f"✅ #{stats['total_sent']} | topic={topic} | "
            f"partition={record_metadata.partition} | "
            f"offset={record_metadata.offset} | event_id={event_id}"
        )


def on_send_error(exc, event_id: str):
    stats["total_failed"] += 1
    log.error(f"❌ Failed to send event {event_id}: {exc}")


# ─── Producer Factory ─────────────────────────────────────────────────────────

def create_producer(max_retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    """Creates a Kafka producer with retry logic on startup."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",               # wait for all replicas to acknowledge
                retries=3,
                max_block_ms=10000,
                request_timeout_ms=15000,
                compression_type="gzip", # compress payloads
            )
            log.info(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
            return producer

        except NoBrokersAvailable:
            log.warning(
                f"⚠️  Kafka not reachable (attempt {attempt}/{max_retries}). "
                f"Retrying in {retry_delay}s..."
            )
            time.sleep(retry_delay)

    log.error("💀 Could not connect to Kafka after all retries. Exiting.")
    sys.exit(1)


# ─── Main Loop ────────────────────────────────────────────────────────────────

def run():
    log.info(f"🚀 Starting Kafka producer | rate={EVENT_RATE_PER_SECOND} events/sec")
    producer = create_producer()

    # Graceful shutdown on Ctrl+C
    def shutdown(sig, frame):
        log.info("\n🛑 Shutting down producer...")
        print_stats()
        producer.flush()
        producer.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    log.info("📡 Producing events. Press Ctrl+C to stop.\n")

    while True:
        try:
            topic, event = generate_random_event()
            event_id = event["event_id"]

            # Use user_id as partition key (keeps same user's events ordered)
            partition_key = event.get("user_id", "unknown")

            producer.send(
                topic,
                key=partition_key,
                value=event,
            ).add_callback(
                lambda meta, eid=event_id: on_send_success(meta, eid)
            ).add_errback(
                lambda exc, eid=event_id: on_send_error(exc, eid)
            )

            time.sleep(SLEEP_INTERVAL)

            # Print stats every 100 events
            if stats["total_sent"] > 0 and stats["total_sent"] % 100 == 0:
                print_stats()

        except Exception as e:
            log.error(f"Unexpected error: {e}")
            time.sleep(1)


if __name__ == "__main__":
    run()
