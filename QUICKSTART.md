# Quick Start Guide

This guide explains how to run the Real-Time E-Commerce Analytics Platform on Mac, Windows, and Linux.

---

## Prerequisites

| Tool | Version | Download |
|------|---------|----------|
| Docker Desktop | Latest | https://www.docker.com/products/docker-desktop |
| Python | 3.10 | https://www.python.org/downloads |
| Conda | Latest | https://www.anaconda.com/download |
| Git | Latest | https://git-scm.com/downloads |

---

## Step 1 — Clone the Repository

```bash
git clone https://github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline.git
cd -real-time-ecommerce-data-pipeline
```

---

## Step 2 — Create Python Environment

```bash
conda create -n airflow-py310 python=3.10 -y
conda activate airflow-py310
```

---

## Step 3 — Install Dependencies

```bash
pip install apache-airflow==2.8.1 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

pip install psycopg2-binary kafka-python faker
```

---

## Step 4 — Start Docker Services

Ensure Docker Desktop is running, then:

```bash
docker compose up -d
```

Wait 30 seconds, then verify all services are running:

```bash
docker compose ps
```

Expected services: `ecom_postgres`, `ecom_kafka`, `ecom_zookeeper`, `ecom_kafka_ui`, `ecom_metabase`, `ecom_pgadmin`

---

## Step 5 — Initialize Airflow

### Mac / Linux

```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow db init

airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@ecom.local
```

### Windows (Command Prompt)

```cmd
set AIRFLOW_HOME=%USERPROFILE%\airflow
set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
set AIRFLOW__CORE__EXECUTOR=LocalExecutor
set AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow db init

airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@ecom.local
```

---

## Step 6 — Fix for Mac M1/M2 (if needed)

If you encounter `Internal Server Error` on the Airflow UI, apply this fix:

```bash
# Get the file path
python3 -c "import flask_session.sessions as s; import inspect; print(inspect.getfile(s))"

# Replace <PATH> with the output from the command above
sed -i '' 's/saved_session.expiry <= datetime.utcnow()/saved_session.expiry <= datetime.now(timezone.utc)/' <PATH>/flask_session/sessions.py

sed -i '' 's/from datetime import datetime/from datetime import datetime, timezone/' <PATH>/flask_session/sessions.py
```

---

## Step 7 — Start Airflow

Open two separate terminals and run the following.

### Terminal 1 — Webserver

**Mac / Linux**
```bash
conda activate airflow-py310
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow webserver --port 8081
```

**Windows**
```cmd
conda activate airflow-py310
set AIRFLOW_HOME=%USERPROFILE%\airflow
set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
set AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow webserver --port 8081
```

### Terminal 2 — Scheduler

**Mac / Linux**
```bash
conda activate airflow-py310
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow scheduler
```

**Windows**
```cmd
conda activate airflow-py310
set AIRFLOW_HOME=%USERPROFILE%\airflow
set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
set AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow scheduler
```

---

## Step 8 — Start the Data Pipeline

Open two more terminals.

### Terminal 3 — Kafka Producer
```bash
conda activate airflow-py310
python scripts/kafka_producer.py
```

### Terminal 4 — Kafka Consumer
```bash
conda activate airflow-py310
python scripts/kafka_consumer_validated.py
```

---

## Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow | http://localhost:8081 | admin / admin |
| Metabase | http://localhost:3000 | Setup on first run |
| Kafka UI | http://localhost:8090 | None |
| pgAdmin | http://localhost:5050 | admin@ecom.local / admin123 |

---

## Stopping All Services

```bash
# Stop Docker
docker compose down

# Stop Airflow (Mac/Linux)
pkill -f "airflow webserver"
pkill -f "airflow scheduler"

# Stop Airflow (Windows)
taskkill /IM "airflow" /F
```

---

## Troubleshooting

**Port already in use**

Mac / Linux:
```bash
lsof -ti:8081 | xargs kill -9
```

Windows:
```cmd
netstat -ano | findstr :8081
taskkill /PID <PID> /F
```

**Docker services not starting**

Ensure Docker Desktop is open and fully loaded before running `docker compose up -d`.

**Kafka connection refused**
```bash
docker compose restart kafka zookeeper
```

**PostgreSQL authentication failed**
```bash
docker compose down -v
docker compose up -d
```

---

## Project Structure

```
-real-time-ecommerce-data-pipeline/
├── docker-compose.yml
├── dags/
├── scripts/
├── sql/
└── streaming/
```

---

For issues or questions, open a GitHub issue at https://github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline
