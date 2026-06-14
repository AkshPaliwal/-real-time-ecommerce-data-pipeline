<div align="center">

# 🚀 Quick Start Guide
### Real-Time E-Commerce Analytics Platform

**Get the full pipeline running locally in under 15 minutes**

[Prerequisites](#-prerequisites) · [Installation](#-installation) · [Running the Pipeline](#-running-the-pipeline) · [Verify Everything Works](#-verify-everything-works) · [Stopping Services](#-stopping-all-services) · [Troubleshooting](#-troubleshooting)

</div>

---

## 🖥️ Platform Support

| OS | Status | Notes |
|----|--------|-------|
| macOS (Intel) | ✅ Fully supported | — |
| macOS (M1/M2/M3) | ✅ Fully supported | See [M1/M2 fix](#-fix-for-mac-m1m2-apple-silicon) if Airflow UI errors occur |
| Linux (Ubuntu/Debian) | ✅ Fully supported | — |
| Windows 10/11 | ✅ Supported | Use Command Prompt or PowerShell; WSL2 also works |

---

## 📦 Prerequisites

Install all four tools before proceeding. Versions matter — mismatches are the #1 cause of setup issues.

| Tool | Required Version | Download |
|------|-----------------|----------|
| **Docker Desktop** | Latest | [docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop) |
| **Python** | 3.10 exactly | [python.org/downloads](https://www.python.org/downloads) |
| **Conda** | Latest | [anaconda.com/download](https://www.anaconda.com/download) |
| **Git** | Latest | [git-scm.com/downloads](https://git-scm.com/downloads) |

> ⚠️ **Python version matters.** Airflow 2.8.1 constraints are pinned to Python 3.10. Using 3.11 or 3.12 will cause dependency conflicts.

> 💾 **Resources needed:** At least 8 GB RAM and 10 GB free disk space for Docker images and PostgreSQL data.

---

## 🛠️ Installation

### Step 1 — Clone the Repository

```bash
git clone https://github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline.git
cd -real-time-ecommerce-data-pipeline
```

---

### Step 2 — Create the Python Environment

```bash
conda create -n airflow-py310 python=3.10 -y
conda activate airflow-py310
```

---

### Step 3 — Install Python Dependencies

Install Airflow first with its official constraints file, then add the remaining packages:

```bash
# Install Airflow 2.8.1 (constraints ensure compatible dependency versions)
pip install apache-airflow==2.8.1 \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.10.txt"

# Install project dependencies
pip install psycopg2-binary kafka-python faker
```

> 🕐 Airflow installation may take 3–5 minutes. This is normal.

---

### Step 4 — Start Docker Services

Make sure Docker Desktop is **open and fully loaded** (whale icon in your taskbar/menu bar), then:

```bash
docker compose up -d
```

Wait ~30–60 seconds for all containers to initialize, then confirm they're all running:

```bash
docker compose ps
```

**Expected output — all 6 services should show `Up` or `running`:**

```
NAME                 STATUS
ecom_postgres        Up
ecom_zookeeper       Up
ecom_kafka           Up
ecom_kafka_ui        Up
ecom_metabase        Up
ecom_pgadmin         Up
```

> ❌ If any service shows `Exit` or `Restarting`, see [Troubleshooting](#-troubleshooting).

---

### Step 5 — Initialize Airflow Database

This step creates the Airflow metadata tables in PostgreSQL and sets up the admin user. Run it **once** during first-time setup only.

#### Mac / Linux

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

#### Windows (Command Prompt)

```cmd
set AIRFLOW_HOME=%USERPROFILE%\airflow
set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
set AIRFLOW__CORE__EXECUTOR=LocalExecutor
set AIRFLOW__CORE__LOAD_EXAMPLES=False

airflow db init

airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@ecom.local
```

> ✅ You should see `Admin user admin created` at the end. If you see `User already exists`, that's fine — skip to the next step.

---

### Step 6 — Fix for Mac M1/M2 (Apple Silicon)

> **Skip this step** if you're on Intel Mac, Linux, or Windows.

If the Airflow UI shows an `Internal Server Error` after login, apply this one-time patch to `flask_session`:

```bash
# Find the file path
python3 -c "import flask_session.sessions as s; import inspect; print(inspect.getfile(s))"

# Apply the fix — replace <PATH> with the output from the command above
sed -i '' 's/saved_session.expiry <= datetime.utcnow()/saved_session.expiry <= datetime.now(timezone.utc)/' <PATH>/flask_session/sessions.py
sed -i '' 's/from datetime import datetime/from datetime import datetime, timezone/' <PATH>/flask_session/sessions.py
```

This patches a timezone-aware datetime comparison bug triggered by newer macOS system libraries.

---

## ▶️ Running the Pipeline

Every time you want to run the project, open **4 separate terminal windows** and run the following commands in order.

---

### Terminal 1 — Airflow Webserver

#### Mac / Linux
```bash
conda activate airflow-py310
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow webserver --port 8081
```

#### Windows
```cmd
conda activate airflow-py310
set AIRFLOW_HOME=%USERPROFILE%\airflow
set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
set AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow webserver --port 8081
```

---

### Terminal 2 — Airflow Scheduler

#### Mac / Linux
```bash
conda activate airflow-py310
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow scheduler
```

#### Windows
```cmd
conda activate airflow-py310
set AIRFLOW_HOME=%USERPROFILE%\airflow
set AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://ecom_user:ecom_pass123@localhost:5432/ecom_analytics
set AIRFLOW__CORE__EXECUTOR=LocalExecutor
airflow scheduler
```

---

### Terminal 3 — Kafka Producer

```bash
conda activate airflow-py310
python scripts/kafka_producer.py
```

The producer will begin emitting simulated Indian e-commerce events (orders, payments, deliveries) at **1,000+ events per minute** across 3 Kafka topics.

---

### Terminal 4 — Kafka Consumer

```bash
conda activate airflow-py310
python scripts/kafka_consumer_validated.py
```

The consumer validates each event. Valid records are written to PostgreSQL (Bronze layer); invalid records go to the rejection log.

---

## ✅ Verify Everything Works

After all 4 terminals are running, confirm the pipeline is healthy:

| Check | URL | What to Look For |
|-------|-----|-----------------|
| **Airflow UI** | http://localhost:8081 | All 6 DAGs visible; no import errors |
| **Kafka UI** | http://localhost:8090 | 3 topics with growing message counts |
| **Metabase** | http://localhost:3000 | Setup wizard (first run) or dashboard |
| **pgAdmin** | http://localhost:5050 | Tables populating in `ecom_analytics` DB |

**In pgAdmin**, connect with:
- Host: `localhost`
- Port: `5432`
- Database: `ecom_analytics`
- Username: `ecom_user`
- Password: `ecom_pass123`

---

## 🌐 Service Reference

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8081 | `admin` / `admin` |
| **Metabase** | http://localhost:3000 | Create on first run |
| **Kafka UI** | http://localhost:8090 | None required |
| **pgAdmin** | http://localhost:5050 | `admin@ecom.local` / `admin123` |

---

## 🛑 Stopping All Services

When you're done, shut down cleanly in this order:

**1. Stop the producer and consumer** — `Ctrl+C` in Terminals 3 and 4

**2. Stop Airflow** — `Ctrl+C` in Terminals 1 and 2, then:

```bash
# Mac / Linux — force kill any lingering processes
pkill -f "airflow webserver"
pkill -f "airflow scheduler"
```

```cmd
:: Windows
taskkill /IM "airflow" /F
```

**3. Stop Docker services**

```bash
# Stop containers but keep data (recommended)
docker compose down

# Stop AND delete all data volumes (full reset)
docker compose down -v
```

> ⚠️ `docker compose down -v` deletes your PostgreSQL data. Only use it if you want a completely clean slate.

---

## 🔧 Troubleshooting

### Port already in use

If port 8081 (or any other) is occupied:

**Mac / Linux**
```bash
# Find and kill the process on port 8081
lsof -ti:8081 | xargs kill -9
```

**Windows**
```cmd
netstat -ano | findstr :8081
taskkill /PID <PID_FROM_ABOVE> /F
```

---

### Docker service not starting or crashing

```bash
# Check logs for the failing service (e.g., kafka)
docker compose logs kafka

# Restart a specific service
docker compose restart kafka zookeeper
```

Make sure Docker Desktop is **fully loaded** before running `docker compose up -d`. Starting it too early is a common issue.

---

### Kafka connection refused

```bash
docker compose restart kafka zookeeper
```

Wait 20 seconds after restart before re-running the producer.

---

### PostgreSQL authentication failed

This usually means the database was initialized in a broken state. Reset it:

```bash
docker compose down -v
docker compose up -d
```

Then re-run the Airflow `db init` command from Step 5.

---

### Airflow DAGs not showing up

Confirm the `AIRFLOW_HOME` export points to the correct directory and that the `dags/` folder is inside it (or symlinked):

```bash
echo $AIRFLOW_HOME          # Should print ~/airflow
ls $AIRFLOW_HOME/dags/      # Should list the 6 DAG files
```

If the `dags/` folder is missing, copy it:

```bash
cp -r dags/ ~/airflow/dags/
```

---

### Airflow UI shows Internal Server Error (Mac M1/M2 only)

See [Step 6 — Fix for Mac M1/M2](#-fix-for-mac-m1m2-apple-silicon) above.

---

## 📁 Project Structure

```
-real-time-ecommerce-data-pipeline/
│
├── docker-compose.yml          # All 6 containerized services
├── .env                        # Environment variables (not committed to git)
│
├── dags/                       # Airflow DAG definitions (6 pipelines)
│   ├── daily_sales_summary.py
│   ├── data_quality_checks.py
│   ├── hourly_stats.py
│   ├── customer_segmentation_dag.py
│   ├── revenue_forecasting_dag.py
│   └── delivery_analysis_dag.py
│
├── scripts/                    # Data pipeline scripts
│   ├── kafka_producer.py       # Event generator (1,000+ events/min)
│   └── kafka_consumer_validated.py  # Validated consumer → PostgreSQL
│
├── sql/
│   └── schema.sql              # Bronze / Silver / Gold DDL
│
└── streaming/                  # Additional streaming utilities
```

---

<div align="center">

**Still stuck?** Open an issue at [github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline/issues](https://github.com/AkshPaliwal/-real-time-ecommerce-data-pipeline/issues)

*Part of the Real-Time E-Commerce Analytics Platform · See the main [README](./README.md) for architecture details*

</div>
