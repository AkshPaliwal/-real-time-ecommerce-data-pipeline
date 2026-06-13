# 🛒 Real-Time E-Commerce Analytics Platform

> A production-grade data engineering project simulating real-time e-commerce event processing — built to mirror architectures used at **Flipkart, Swiggy, and Zomato**.

![Python](https://img.shields.io/badge/Python-3.10-blue) ![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.x-black) ![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-red) ![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue) ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## 📐 Architecture

```
Kafka Producer (Python)
        │
        ▼
  Apache Kafka
  (3 Topics: orders, payments, deliveries)
        │
        ▼
Kafka Consumer (Validated)
        │
   ┌────┴────┐
   ▼         ▼
PostgreSQL  Rejected Records Log
(10+ tables)
   │
   ▼
Apache Airflow
(6 Automated DAGs)
   │
   ▼
Metabase Dashboard
(Live BI Visualizations)
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Apache Kafka | 7.4.0 | Real-time event streaming |
| Apache Airflow | 2.8.1 | Pipeline orchestration |
| PostgreSQL | 15 | Data storage (Bronze/Silver/Gold) |
| Metabase | Latest | Business intelligence & dashboards |
| Docker Compose | - | Service containerization |
| Python | 3.10 | Data processing & scripting |

---

## ✨ Features

- ⚡ **Real-time streaming** — Orders, payments, delivery events processed every 2 seconds
- ✅ **Data validation** — Invalid records automatically rejected and logged
- 📦 **Medallion architecture** — Bronze → Silver → Gold data layers in PostgreSQL
- 🤖 **6 Automated Airflow DAGs** — Daily and hourly analytics pipelines
- 👥 **Customer Segmentation** — RFM (Recency, Frequency, Monetary) analysis
- 📈 **Revenue Forecasting** — Moving average model for next-day prediction
- 🚚 **Delivery Time Analysis** — City-wise performance breakdown
- 📊 **Live Metabase Dashboards** — Revenue, orders, payment methods

---

## 📁 Project Structure

```
ecom-analytics-platform/
├── docker-compose.yml          # All services definition
├── .env                        # Environment variables
├── dags/
│   ├── daily_sales_summary.py         # Revenue & order summary
│   ├── data_quality_checks.py         # Data quality monitoring
│   ├── hourly_stats.py                # Hourly metrics snapshot
│   ├── customer_segmentation_dag.py   # RFM analysis
│   ├── revenue_forecasting_dag.py     # Next-day revenue prediction
│   └── delivery_analysis_dag.py       # City-wise delivery analysis
├── scripts/
│   ├── kafka_producer.py              # Event generator
│   └── kafka_consumer_validated.py    # Validated consumer
└── sql/
    └── schema.sql                     # DB schema (Bronze/Silver/Gold)
```

---

## 🚀 Setup & Run

### Prerequisites
- Docker Desktop
- Python 3.10
- Conda

### Step 1 — Clone & Start Services
```bash
git clone https://github.com/akshpaliwal/ecom-analytics-platform.git
cd ecom-analytics-platform
docker compose up -d
```

### Step 2 — Start Airflow
```bash
conda activate airflow-py310
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Terminal 1
airflow webserver --port 8081

# Terminal 2
airflow scheduler
```

### Step 3 — Start Data Pipeline
```bash
# Terminal 3 — Start Producer
python scripts/kafka_producer.py

# Terminal 4 — Start Consumer
python scripts/kafka_consumer_validated.py
```

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | admin / admin |
| Metabase | http://localhost:3000 | Setup on first run |
| Kafka UI | http://localhost:8090 | — |
| pgAdmin | http://localhost:5050 | admin@ecom.local / admin123 |

---

## ⚙️ Airflow DAGs

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `daily_sales_summary` | 1 AM daily | Revenue and order aggregation |
| `data_quality_checks` | 2 AM daily | Null checks, duplicate detection |
| `hourly_stats` | Every hour | Real-time metrics snapshot |
| `customer_segmentation` | 3 AM daily | RFM customer segments |
| `revenue_forecasting` | 4 AM daily | Next-day revenue prediction |
| `delivery_time_analysis` | 5 AM daily | City-wise delivery SLA tracking |

---

## 📊 Metabase Dashboards

- 📍 **Revenue by City** — Geographic revenue breakdown
- 📦 **Orders by Product Category** — Top-selling categories
- 💳 **Payment Method Breakdown** — UPI vs COD vs Card split
- ⏱️ **Delivery Time Heatmap** — City-wise delivery performance

---

## 📈 Key Metrics

| Metric | Value |
|--------|-------|
| Events processed | 1,000+ per minute |
| Kafka Topics | 3 (orders, payments, deliveries) |
| Airflow DAGs | 6 automated pipelines |
| PostgreSQL Tables | 10+ |
| Data Layers | Bronze / Silver / Gold |
| Analytics Models | RFM Segmentation, Moving Average Forecast |

---

## 🔄 Data Pipeline Flow

```
1. Producer generates Indian e-commerce events (UPI payments, Indian cities, real products)
2. Kafka streams events across 3 topics in real-time
3. Consumer validates records → valid data to PostgreSQL, invalid to rejection log
4. Airflow DAGs run nightly analytics (segmentation, forecasting, quality checks)
5. Metabase reads from Gold layer → displays live dashboards
```

---

## 🧠 Concepts Demonstrated

- Real-time stream processing with Apache Kafka
- Pipeline orchestration with Apache Airflow
- Medallion data architecture (Bronze/Silver/Gold)
- Data quality monitoring and validation
- RFM-based customer segmentation
- Time-series revenue forecasting
- Containerized microservices with Docker Compose
- Business intelligence with Metabase

---

## 👤 Author

**Aksh Paliwal**  
📧 [GitHub](https://github.com/akshpaliwal)  
💼 Aspiring Data Engineer | Open to opportunities

---

> ⭐ If you found this project useful, please give it a star!
