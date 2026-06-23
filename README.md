# Weather Analytics Pipeline

A complete end-to-end data engineering pipeline that automatically ingests live weather data, processes it through a Hadoop-inspired architecture, and visualises the results – all orchestrated with **Apache Airflow**.

---

# Table of Contents

- [Architecture & Tools](#architecture--tools)
- [Hadoop Ecosystem Mapping](#hadoop-ecosystem-mapping)
- [Project Structure](#project-structure)
- [Pipeline Workflow](#pipeline-workflow)
- [Setup Instructions](#setup-instructions)
- [Airflow DAG](#airflow-dag)
- [Grafana Dashboard](#grafana-dashboard)
- [CI/CD](#cicd-with-github-actions)
- [Challenges & Learnings](#challenges--learnings)
- [Future Enhancements](#future-enhancements)

---

# Architecture & Tools

| Layer | Tool / Service | Purpose |
|---|---|---|
| Ingestion | Azure Event Hubs | Kafka-compatible streaming ingestion |
| Orchestration | Apache Airflow | Scheduling and workflow management |
| Processing | Apache Hive | SQL based ETL aggregation |
| Storage | CSV + Hive external tables | Data lake style storage |
| Serving | Azure PostgreSQL | Analytics serving layer |
| Visualization | Grafana | Dashboard and reporting |
| CI/CD | GitHub Actions | Automated build/deployment |
| Data Source | Open-Meteo API | Free weather API |

---

# Hadoop Ecosystem Mapping

| This Project | Hadoop Equivalent |
|-|-|
| Azure Event Hubs | Kafka / Flume |
| Consumer Service | Flume / Spark Streaming |
| Hive External Tables | HDFS + Hive |
| Airflow DAG | Oozie |
| PostgreSQL Serving Layer | RDBMS Reporting Layer |
| Grafana | Zeppelin / BI Dashboard |

---

# Project Structure


weather-pipeline/

```
├── producer/
│   ├── weather_producer.py
│   └── weather_producer_once.py
│
├── consumer/
│   └── consume_batch.py
│
├── airflow/
│   ├── dags/
│   │   └── weather_pipeline_v2.py
│   └── plugins/
│
├── docker/
│   └── airflow/
│       └── Dockerfile
│
├── hive-data/
│   └── weather csv files
│
├── grafana/
│   └── weather-dashboard.json
│
├── docker-compose-airflow.yml
│
└── README.md
```

---

# Pipeline Workflow

Every hour Airflow runs:

```
fetch_weather
        |
        v
ingest_to_csv
        |
        v
run_hive_etl
        |
        v
load_to_postgres
```

### Tasks

### 1. fetch_weather

- Calls Open-Meteo API
- Produces JSON events
- Sends data to Azure Event Hub


### 2. ingest_to_csv

- Reads from Event Hub
- Converts JSON → CSV
- Stores raw data


### 3. run_hive_etl

Hive performs:

- daily aggregation
- average temperature
- max temperature
- min temperature
- humidity
- wind calculation


### 4. load_to_postgres

Moves Hive output into Azure PostgreSQL for analytics.

---

# Setup Instructions

## Prerequisites

Install:

- Docker
- Docker Compose
- Python 3.10+
- Azure CLI


---

# 1. Clone Repository


```bash
git clone https://github.com/YOUR_USER/weather-pipeline.git

cd weather-pipeline
```

---

# 2. Configure Environment

Create:

```
.env
```


Example:


```env
EVENTHUB_CONNECTION_STRING=Endpoint=sb://your-eventhub.servicebus.windows.net/...
EVENTHUB_NAME=weather-raw
```


---

# 3. Create Docker Network


```bash
docker network create weather-net
```

---

# 4. Start Hive


```bash
docker run -d \
--name hive-standalone \
--network weather-net \
-p 10000:10000 \
-v $(pwd)/hive-data:/data \
-e SERVICE_NAME=hiveserver2 \
apache/hive:4.0.0
```

Check:

```bash
docker logs hive-standalone
```

---

# 5. Start Airflow


Build:


```bash
docker compose \
-f docker-compose-airflow.yml \
build --no-cache
```


Start:


```bash
docker compose \
-f docker-compose-airflow.yml \
up -d
```


Airflow:

```
http://localhost:8081
```


Login:

```
username: admin
password: admin
```

---

# 6. Start Grafana


```bash
docker run -d \
--name grafana \
-p 3000:3000 \
grafana/grafana
```


Open:

```
http://localhost:3000
```


Import dashboard:

```
grafana/weather-dashboard.json
```

---

# Airflow DAG

DAG:

```
weather_pipeline_v2
```


Schedule:

```
@hourly
```


Tasks:

```
fetch_weather
      |
      v
ingest_to_csv
      |
      v
run_hive_etl
      |
      v
load_to_postgres
```


---

# Grafana Dashboard


Dashboard contains:


- Temperature trends
- Humidity trends
- Wind speed
- City level summaries
- Daily analytics


Import:

```
Dashboards
   |
Import
   |
Upload JSON
```


---

# CI/CD with GitHub Actions


Pipeline:

```
Git push
    |
    v
GitHub Actions
    |
    v
Build Docker images
    |
    v
Deploy containers
```


Required secrets:


```
AZURE_CREDENTIALS

ACR_PASSWORD

EVENTHUB_CONNECTION_STRING
```

---

# Challenges & Learnings


## 1. Docker API Compatibility

Problem:

Docker CLI inside Airflow was outdated.


Solution:

Installed latest Docker CLI inside custom Airflow image.


---

## 2. Hive Connectivity

Problem:

Airflow could not communicate with Hive container.


Solution:

Connected both containers to the same Docker network.


---

## 3. Event Hub Authentication


Problem:

Environment variables were missing inside Airflow.


Solution:

Passed secrets using Docker compose environment.


---

## 4. CSV NULL Handling


Problem:

Hive returned NULL strings.


Solution:

Added safe conversion before PostgreSQL insert.


---

# Future Enhancements


- Replace CSV with Parquet
- Add Spark processing
- Add Great Expectations checks
- Deploy Airflow on AKS
- Move Hive to Azure HDInsight
- Add streaming consumer service
- Add monitoring with Prometheus


---

# License

MIT License


---

Built as a learning Data Engineering project.

Happy coding!