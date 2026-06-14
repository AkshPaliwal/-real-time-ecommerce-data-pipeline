<div align="center">

# 🛒 Real-Time E-Commerce Analytics Platform

**A production-grade data engineering project simulating real-time e-commerce event processing**
*Built to mirror architectures used at Flipkart, Swiggy, and Zomato*

[![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com)
[![Metabase](https://img.shields.io/badge/Metabase-Latest-509EE3?style=for-the-badge&logo=metabase&logoColor=white)](https://metabase.com)

</div>

---

## 📌 Overview

This project implements a **fully containerized, end-to-end data engineering pipeline** that ingests, validates, transforms, and visualizes e-commerce events in near-real-time. It demonstrates industry-standard patterns including event-driven architecture, medallion data layers, automated orchestration, and BI dashboarding — all running locally via Docker.

**What makes this production-grade:**
- Events flow from producer → Kafka → validated consumer → PostgreSQL in under 2 seconds
- Bad data is automatically quarantined to a rejection log, never corrupting the warehouse
- Six Airflow DAGs run nightly analytics autonomously — no manual triggers needed
- A Metabase dashboard gives business stakeholders live visibility into the Gold data layer

---

## 📐 Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA INGESTION                           │
│                                                                 │
│   Kafka Producer (Python)                                       │
│          │  Simulates orders, payments & delivery events        │
│          ▼                                                      │
│     Apache Kafka                                                │
│   ┌──────────────────────────────────────┐                     │
│   │  Topic: orders  │  Topic: payments   │  Topic: deliveries  │
│   └──────────────────────────────────────┘                     │
│          │                                                      │
│          ▼                                                      │
│   Kafka Consumer (Validated)                                    │
│          │                                                      │
│    ┌─────┴───────────┐                                         │
│    ▼                 ▼                                         │
│  PostgreSQL    Rejection Log                                    │
│  (Valid data)  (Invalid records)                               │
└─────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MEDALLION ARCHITECTURE                       │
│                                                                 │
│   🥉 Bronze Layer          🥈 Silver Layer       🥇 Gold Layer  │
│   Raw ingested data   →   Cleaned & joined   →  Aggregated BI  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATION & ANALYTICS                    │
│                                                                 │
│             Apache Airflow (6 Automated DAGs)                  │
│     Daily Sales │ Quality Checks │ Customer Segmentation       │
│     Revenue Forecast │ Delivery Analysis │ Hourly Stats        │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BUSINESS INTELLIGENCE                        │
│                                                                 │
│           Metabase Live Dashboards                              │
│   Revenue by City │ Orders by Category │ Payment Methods       │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Version | Role |
|------|---------|------|
| **Apache Kafka** | 7.4.0 (Confluent) | Real-time event streaming across 3 topics |
| **Apache Airflow** | 2.8.1 | Pipeline orchestration & DAG scheduling |
| **PostgreSQL** | 15 | Data warehouse (Bronze / Silver / Gold layers) |
| **Metabase** | Latest | Self-serve BI dashboards for stakeholders |
| **Docker Compose** | — | Single-command service containerization |
| **Python** | 3.10 | Event generation, validation, and processing |

---

## ✨ Key Features

| Feature | Description |
|---------|-------------|
| ⚡ **Real-Time Streaming** | Events processed every 2 seconds via Kafka across 3 topics |
| ✅ **Automated Validation** | Invalid records quarantined to a rejection log automatically |
| 🏗️ **Medallion Architecture** | Bronze → Silver → Gold data layers enforcing progressive quality |
| 🤖 **6 Airflow DAGs** | Nightly analytics pipelines running without manual intervention |
| 👥 **RFM Segmentation** | Customers ranked by Recency, Frequency, and Monetary value |
| 📈 **Revenue Forecasting** | Moving average model predicts next-day revenue |
| 🚚 **Delivery SLA Tracking** | City-wise on-time delivery performance breakdown |
| 📊 **Live BI Dashboards** | Metabase reads from Gold layer for always-fresh visualizations |
| 🇮🇳 **India-Specific Data** | UPI payments, Indian cities, local product categories simulated |

---

## 📁 Project Structure

```
ecom-analytics-platform/
│
├── docker-compose.yml              # All 5 services: Kafka, Zookeeper, PostgreSQL,
│                                   # Kafka UI, Metabase, pgAdmin
│
├── .env                            # Credentials & environment config (not committed)
│
├── dags/                           # Airflow DAG definitions
│   ├── daily_sales_summary.py      # Revenue & order volume aggregation
│   ├── data_quality_checks.py      # Null detection, duplicate flagging
│   ├── hourly_stats.py             # Rolling hourly metrics snapshot
│   ├── customer_segmentation_dag.py  # RFM analysis → customer tiers
│   ├── revenue_forecasting_dag.py  # Moving average next-day forecast
│   └── delivery_analysis_dag.py   # City-wise delivery SLA tracking
│
├── scripts/
│   ├── kafka_producer.py           # Simulates 1,000+ events/min across 3 topics
│   └── kafka_consumer_validated.py # Validates events; routes to DB or rejection log
│
└── sql/
    └── schema.sql                  # Full DDL: Bronze, Silver, Gold layer tables
```

---

## 🚀 Setup & Run

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- [Python 3.10](https://www.python.org/downloads/) via Conda
- 8 GB RAM recommended for all containers

---

### Step 1 — Clone & Start Services

```bash
git clone https://github.com/akshpaliwal/ecom-analytics-platform.git
cd ecom-analytics-platform
docker compose up -d
```

> All services (Kafka, Zookeeper, PostgreSQL, Metabase, Kafka UI, pgAdmin) start via Docker Compose. Wait ~60 seconds for Kafka to be fully ready before starting the producer.

---

### Step 2 — Start Airflow

Open two separate terminals:

```bash
# Activate the environment (both terminals)
conda activate airflow-py310
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Terminal 1 — Webserver
airflow webserver --port 8081

# Terminal 2 — Scheduler
airflow scheduler
```

Navigate to [http://localhost:8081](http://localhost:8081) (admin / admin) to verify all 6 DAGs appear.

---

### Step 3 — Start the Data Pipeline

Open two more terminals:

```bash
# Terminal 3 — Start event producer
python scripts/kafka_producer.py

# Terminal 4 — Start validated consumer
python scripts/kafka_consumer_validated.py
```

You should see events flowing in the Kafka UI at [http://localhost:8090](http://localhost:8090) and records appearing in PostgreSQL within seconds.

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8081 | `admin` / `admin` |
| **Metabase** | http://localhost:3000 | Create on first run |
| **Kafka UI** | http://localhost:8090 | — |
| **pgAdmin** | http://localhost:5050 | `admin@ecom.local` / `admin123` |

---

## ⚙️ Airflow DAGs

| DAG | Schedule | What It Does |
|-----|----------|--------------|
| `daily_sales_summary` | 1 AM daily | Aggregates total revenue, orders, and AOV by city and category |
| `data_quality_checks` | 2 AM daily | Flags nulls, duplicates, and schema drift in Silver tables |
| `hourly_stats` | Every hour | Snapshots rolling metrics for live dashboard freshness |
| `customer_segmentation` | 3 AM daily | Scores all customers via RFM; writes segments to Gold layer |
| `revenue_forecasting` | 4 AM daily | Computes 7-day moving average; forecasts next-day revenue |
| `delivery_time_analysis` | 5 AM daily | Calculates city-wise delivery time and SLA breach rates |

---

## 🥇 Medallion Data Architecture

```
Bronze (Raw)          Silver (Cleaned)          Gold (Aggregated)
─────────────         ────────────────          ─────────────────
orders_raw       →    orders_validated     →    daily_sales_summary
payments_raw     →    payments_validated   →    customer_segments
deliveries_raw   →    delivery_events      →    city_delivery_sla
                                           →    revenue_forecast
```

- **Bronze** — Raw events exactly as consumed from Kafka; no transformations
- **Silver** — Validated, deduplicated, and joined records; typed columns
- **Gold** — Business-ready aggregations consumed directly by Metabase

---

## 📊 Metabase Dashboards

After connecting Metabase to PostgreSQL (Gold schema), the following dashboards are available:

- 📍 **Revenue by City** — Which Indian cities generate the most GMV
- 📦 **Orders by Product Category** — Top-selling categories by volume and value
- 💳 **Payment Method Breakdown** — UPI vs COD vs Card share over time
- ⏱️ **Delivery Time Heatmap** — City-wise average delivery time vs SLA target

---

## 📈 Project Metrics

| Metric | Value |
|--------|-------|
| Throughput | 1,000+ events per minute |
| Kafka Topics | 3 (orders, payments, deliveries) |
| Airflow DAGs | 6 automated pipelines |
| PostgreSQL Tables | 10+ across 3 layers |
| Analytics Models | RFM Segmentation, Moving Average Forecast |
| Containerized Services | 6 (Kafka, Zookeeper, PostgreSQL, Metabase, Kafka UI, pgAdmin) |

---

## 🧠 Concepts Demonstrated

- **Stream processing** — Apache Kafka for real-time, high-throughput event ingestion
- **Pipeline orchestration** — Airflow DAGs with dependency management and retry logic
- **Medallion architecture** — Progressive data quality enforcement across Bronze/Silver/Gold
- **Data quality monitoring** — Automated null detection, deduplication, and schema validation
- **Customer analytics** — RFM-based segmentation to identify high-value customers
- **Time-series forecasting** — Rolling moving average for next-day revenue prediction
- **Containerized services** — Docker Compose for reproducible, single-command deployments
- **Business intelligence** — Metabase connecting directly to the Gold layer for live dashboards

---

## 🔄 End-to-End Data Flow

```
1. Producer  →  Generates synthetic Indian e-commerce events (UPI, Indian cities,
                real product categories) at 1,000+ events/minute

2. Kafka     →  Routes events across 3 partitioned topics for parallel consumption

3. Consumer  →  Validates schema & types; writes clean records to PostgreSQL Bronze;
                sends invalid records to rejection log

4. Airflow   →  Nightly DAGs transform Bronze → Silver → Gold, run quality checks,
                compute RFM segments, and generate revenue forecasts

5. Metabase  →  Reads from Gold layer and serves live dashboards with no ETL delay
```

---

## 👤 Author

**Aksh Paliwal**
Aspiring Data Engineer — open to full-time opportunities and internships

[![GitHub](https://img.shields.io/badge/GitHub-akshpaliwal-181717?style=flat-square&logo=github)](https://github.com/akshpaliwal)

---

<div align="center">

⭐ **Found this useful? Star the repo to help others discover it.**

*Built with Apache Kafka · Apache Airflow · PostgreSQL · Metabase · Docker*

</div>
