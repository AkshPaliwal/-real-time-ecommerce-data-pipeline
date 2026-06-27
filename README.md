<div align="center">

# ⚡ Real-Time E-Commerce Analytics

**Production-grade data pipeline · Built like Flipkart & Swiggy do it**

[![Python](https://img.shields.io/badge/Python-3.10-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.138-009688?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![React](https://img.shields.io/badge/React-18-61DAFB?style=flat-square&logo=react&logoColor=black)](https://reactjs.org)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?style=flat-square&logo=postgresql&logoColor=white)](https://postgresql.org)
[![Kafka](https://img.shields.io/badge/Apache_Kafka-3.x-231F20?style=flat-square&logo=apachekafka&logoColor=white)](https://kafka.apache.org)
[![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8-017CEE?style=flat-square&logo=apacheairflow&logoColor=white)](https://airflow.apache.org)

</div>

---

## Architecture

```
Kafka Producer ──► Apache Kafka ──► Kafka Consumer
  (Python)         3 Topics          (Validated)
                                          │
                                    PostgreSQL
                                  Bronze/Silver/Gold
                                          │
                                   Apache Airflow
                                    6 DAGs nightly
                                          │
                              ┌───────────┴───────────┐
                           FastAPI                  React
                         10 endpoints            Dashboard
                       (REST + Analytics)      (Recharts UI)
```

---

## Stack

| Layer | Tool | Purpose |
|---|---|---|
| Streaming | Apache Kafka | Real-time event ingestion |
| Storage | PostgreSQL 15 | Bronze → Silver → Gold layers |
| Orchestration | Apache Airflow | 6 automated nightly DAGs |
| Backend | FastAPI | 10 REST + analytics endpoints |
| Frontend | React + Recharts | Live dashboard with filters |
| Infra | Docker Compose | One-command local setup |

---

## Dashboard Features

```
📦 KPI Cards         — Orders · Revenue · Customers · AOV
📈 Revenue Trend     — Monthly gradient line chart
🥧 Order Status      — Donut breakdown
🏙️  Revenue by City   — Multi-color bar chart
🔮 Sales Forecast    — Linear regression · Next 7 days
📊 Category Demand   — Growth % with rising/falling badges
👤 Customer Churn    — Active / At Risk / Churned segmentation
⚠️  Inventory Alerts  — Demand velocity + risk scoring
```

**Filters:** City · Category · Date (7d / 30d / 90d / Custom) — applied across all charts

---

## API Endpoints

```
GET /api/dashboard/summary              KPIs
GET /api/dashboard/revenue-by-city      City revenue
GET /api/dashboard/category-sales       Category breakdown
GET /api/dashboard/order-status         Status distribution
GET /api/dashboard/monthly-revenue      Month-over-month trend
GET /api/dashboard/top-products         Top 10 by revenue
GET /api/dashboard/sales-prediction     7-day forecast
GET /api/dashboard/category-forecast    Category growth %
GET /api/dashboard/inventory-alert      High-demand products
GET /api/dashboard/customer-churn       RFM churn segments
```

All endpoints accept `?city=&category=&date_from=&date_to=` query params.

---

## Airflow DAGs

| DAG | Schedule | Job |
|---|---|---|
| `daily_sales_summary` | 1 AM | Revenue & order aggregation |
| `data_quality_checks` | 2 AM | Null checks, duplicate detection |
| `hourly_stats` | Every hour | Real-time metrics snapshot |
| `customer_segmentation` | 3 AM | RFM analysis |
| `revenue_forecasting` | 4 AM | Next-day prediction |
| `delivery_time_analysis` | 5 AM | City-wise SLA tracking |

---

## Local Setup

**Prerequisites:** Docker Desktop · Python 3.10 · Node 18+

```bash
# 1. Clone
git clone https://github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline.git
cd real-time-ecommerce-data-pipeline

# 2. Start Kafka + PostgreSQL
docker compose up -d

# 3. Start data pipeline
python scripts/kafka_producer.py        # Terminal 1
python scripts/kafka_consumer_validated.py  # Terminal 2

# 4. Start FastAPI backend
cd backend && uvicorn app.main:app --reload --port 8000

# 5. Start React frontend
cd frontend && npm install && npm start  # Opens on :3001
```

**Service URLs**

| Service | URL |
|---|---|
| React Dashboard | http://localhost:3001 |
| FastAPI Docs | http://localhost:8000/docs |
| Airflow | http://localhost:8081 |
| Kafka UI | http://localhost:8090 |
| pgAdmin | http://localhost:5050 |

---

## Data Flow

```
1. Producer streams Indian e-commerce events every 2s
   └── Orders · Payments · Deliveries across 3 Kafka topics

2. Consumer validates records
   └── Valid  ──► PostgreSQL (Bronze layer)
   └── Invalid ──► Rejection log

3. Airflow DAGs promote & aggregate nightly
   └── Bronze ──► Silver ──► Gold

4. FastAPI reads from Gold layer
   └── 10 endpoints with city / category / date filters

5. React dashboard polls every 30s
   └── Live charts · KPIs · Forecasts · Alerts
```

---

## Key Numbers

| Metric | Value |
|---|---|
| Events processed | 1,000+ / minute |
| Kafka topics | 3 |
| PostgreSQL tables | 10+ |
| Airflow DAGs | 6 |
| API endpoints | 10 |
| Dashboard charts | 12 |

---

<div align="center">

**Aksh Paliwal** · Aspiring Data Engineer · [GitHub](https://github.com/AkshPaliwal)

*Built to mirror production architectures at Flipkart, Swiggy & Zomato*

</div>
