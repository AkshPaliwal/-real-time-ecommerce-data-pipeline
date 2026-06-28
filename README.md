# ⚡ Real-Time E-Commerce Analytics Platform

> End-to-end data engineering pipeline — from raw events to live business intelligence.

**Live Demo →** [real-time-ecommerce-data-pipeline.vercel.app](https://real-time-ecommerce-data-pipeline.vercel.app)

---

## Architecture

```
Kafka Producer
     │
     ▼
┌─────────────────────────────────────────────┐
│              Apache Kafka                   │
│   orders ──── payments ──── deliveries      │
└─────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────┐
│           PostgreSQL  (Medallion)           │
│   Bronze → Silver → Gold                   │
└─────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────┐
│              Apache Airflow                 │
│   6 DAGs — hourly & daily analytics        │
└─────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────┐
│           FastAPI  (10 endpoints)           │
│   REST APIs + ML forecasting + churn       │
└─────────────────────────────────────────────┘
     │
     ▼
┌─────────────────────────────────────────────┐
│         React Dashboard (Vercel)            │
│   Live charts · Filters · Auto-refresh     │
└─────────────────────────────────────────────┘
```

---

## Stack

| Layer | Technology |
|-------|-----------|
| Streaming | Apache Kafka |
| Warehouse | PostgreSQL (Bronze → Silver → Gold) |
| Orchestration | Apache Airflow |
| Backend | FastAPI (Python) |
| Frontend | React + Recharts |
| Deployment | Railway + Vercel |

---

## Features

**Pipeline**
- 1,000+ events/min across 3 Kafka topics — orders, payments, deliveries
- Medallion architecture with automated data validation
- Invalid records routed to separate error log

**Airflow DAGs**
- RFM customer segmentation
- Revenue forecasting (hourly + daily)
- City-wise delivery SLA tracking
- Payment failure monitoring

**Analytics API**
- `/summary` — KPIs: orders, revenue, customers, AOV
- `/revenue-by-city` — city-wise breakdown
- `/category-sales` — category performance
- `/monthly-revenue` — trend over time
- `/top-products` — top 10 by revenue
- `/sales-prediction` — 7-day ML forecast (linear regression)
- `/category-forecast` — growth % with trend detection
- `/inventory-alert` — demand velocity + risk scoring
- `/customer-churn` — Active / At Risk / Churned segmentation
- `/order-status` — status distribution

**Dashboard**
- Live auto-refresh every 30 seconds
- City + category filters
- Sales forecast chart (actual vs predicted)
- Customer churn analysis
- Inventory demand alerts

---

## Quickstart

```bash
# Clone
git clone https://github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline
cd -real-time-ecommerce-data-pipeline

# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Frontend
cd frontend
npm install
npm start
```

**API Docs →** `http://localhost:8000/docs`

---

## Deployment

| Service | Platform | Status |
|---------|----------|--------|
| PostgreSQL + FastAPI | Railway | ✅ Live |
| React Dashboard | Vercel | ✅ Live |

---

## Project Structure

```
├── backend/
│   └── app/
│       ├── main.py
│       ├── database.py
│       └── routers/
│           └── dashboard.py
├── frontend/
│   └── src/
│       └── App.js
├── dags/              # Airflow DAGs
├── producer/          # Kafka producer
├── sql/               # Schema + migrations
└── docker-compose.yml
```

---

<p align="center">
  Built by <a href="https://github.com/AkshPaliwal">Aksh Paliwal</a>
</p>
