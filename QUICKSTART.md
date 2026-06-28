# Quickstart

Full stack running locally in under 10 minutes.

**Live Demo →** [real-time-ecommerce-data-pipeline.vercel.app](https://real-time-ecommerce-data-pipeline.vercel.app)

---

## Prerequisites

| Tool | Version |
|------|---------|
| Python | 3.10+ |
| Node.js | 18+ |
| PostgreSQL | 14+ |
| Docker | 20+ (optional) |

---

## Step 1 — Clone

```bash
git clone https://github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline
cd -real-time-ecommerce-data-pipeline
```

---

## Step 2 — PostgreSQL setup

```bash
# start postgres
brew services start postgresql@16
psql postgres
```

```sql
-- inside psql
CREATE USER ecom_user WITH PASSWORD 'ecom123';
CREATE DATABASE ecom_analytics OWNER ecom_user;
GRANT ALL PRIVILEGES ON DATABASE ecom_analytics TO ecom_user;
\q
```

```bash
# load schema
psql -U ecom_user -d ecom_analytics -f sql/schema.sql
```

```bash
# seed sample data (5000 orders)
psql -U ecom_user -d ecom_analytics << 'SQL'
CREATE TABLE IF NOT EXISTS orders AS
SELECT
    'ORD' || LPAD(gs::text, 6, '0') AS order_id,
    'CUST' || LPAD((RANDOM()*1000+1)::int::text, 4, '0') AS customer_id,
    'Customer ' || gs AS customer_name,
    (ARRAY['Mumbai','Delhi','Bangalore','Chennai','Hyderabad','Pune','Kolkata','Ahmedabad'])[floor(RANDOM()*8+1)::int] AS city,
    (ARRAY['Electronics','Fashion','Home & Kitchen','Sports','Books','Beauty'])[floor(RANDOM()*6+1)::int] AS category,
    (ARRAY['iPhone 15 Pro','Samsung Galaxy S24','Sony WH-1000XM5','MacBook Air M2','Nike Air Max','Adidas Ultraboost','Levi Jeans 511','Allen Solly Shirt','Instant Pot Duo','Philips Air Fryer','Dyson V12','Prestige Cooker','Yoga Mat Pro','Whey Protein 2kg','Fitbit Charge 6','Decathlon Cycle','Atomic Habits','Rich Dad Poor Dad','Zero to One','Deep Work','LOreal Serum','Mamaearth Sunscreen','Wow Shampoo','Plum Face Wash'])[floor(RANDOM()*24+1)::int] AS product_name,
    (RANDOM()*5+1)::int AS quantity,
    (RANDOM()*10000+500)::numeric(10,2) AS total_amount,
    (ARRAY['delivered','pending','cancelled','processing'])[floor(RANDOM()*4+1)::int] AS status,
    NOW() - (RANDOM()*90 || ' days')::interval AS timestamp
FROM generate_series(1, 5000) AS gs;
SQL
```

---

## Step 3 — FastAPI backend

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

> API docs → [http://localhost:8000/docs](http://localhost:8000/docs)

---

## Step 4 — React frontend

```bash
cd frontend
npm install
npm start
```

> Dashboard → [http://localhost:3000](http://localhost:3000)

---

## Step 5 — Kafka + Airflow (optional)

> Skip this for dashboard only. Required for real-time streaming.

```bash
docker-compose up -d
docker-compose ps
```

| Service | URL |
|---------|-----|
| Kafka | localhost:9092 |
| Airflow | http://localhost:8080 (admin / admin) |
| Zookeeper | localhost:2181 |

### Start Kafka producer

```bash
cd producer
pip install -r requirements.txt
python producer.py
```

### Enable Airflow DAGs

Login at [http://localhost:8080](http://localhost:8080) and enable:

- `daily_revenue_aggregation`
- `hourly_revenue_aggregation`
- `rfm_customer_segmentation`
- `revenue_forecast`
- `delivery_sla_tracking`
- `payment_failure_monitor`

---

## Environment variables

Create `backend/.env`:

```env
DATABASE_URL=postgresql://ecom_user:ecom123@localhost:5432/ecom_analytics
```

For Railway, set `DATABASE_URL = ${{Postgres.DATABASE_URL}}` in the Variables tab.

---

## Deployment

### FastAPI → Railway

1. Push code to GitHub
2. Create new Railway project → deploy from GitHub
3. Add PostgreSQL service
4. Set `DATABASE_URL = ${{Postgres.DATABASE_URL}}`
5. Railway auto-deploys on every push

### React → Vercel

1. Import GitHub repo on Vercel
2. Set root directory → `frontend`
3. Framework → Create React App
4. Deploy

---

## Live URLs

| Service | URL |
|---------|-----|
| Dashboard | [real-time-ecommerce-data-pipeline.vercel.app](https://real-time-ecommerce-data-pipeline.vercel.app) |
| API | [real-time-ecommerce-data-pipeline-production.up.railway.app](https://real-time-ecommerce-data-pipeline-production.up.railway.app) |
| API docs | [.../docs](https://real-time-ecommerce-data-pipeline-production.up.railway.app/docs) |

---

## Troubleshooting

**Port already in use**
```bash
kill -9 $(lsof -t -i:8000)
kill -9 $(lsof -t -i:3000)
```

**PostgreSQL role does not exist**
```bash
psql postgres -c "CREATE USER ecom_user WITH PASSWORD 'ecom123';"
```

**React showing NaN**
- Check API is running on port 8000
- Check browser console for CORS errors
- Verify `API` URL in `frontend/src/App.js`

---

<p align="center">Built by <a href="https://github.com/AkshPaliwal">Aksh Paliwal</a></p>
