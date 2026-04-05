# 📊 Social Crypto Intelligence Platform

> A hybrid batch + real-time data pipeline correlating tech news cycles with live cryptocurrency price movements — built with Apache Airflow, Kafka, Spark Streaming, and Snowflake.

![Airflow](https://img.shields.io/badge/Apache_Airflow-2.8.1-017CEE?logo=apacheairflow)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.4.0-231F20?logo=apachekafka)
![Spark](https://img.shields.io/badge/Spark-3.5.4-E25A1C?logo=apachespark)
![Snowflake](https://img.shields.io/badge/Snowflake-Cloud-29B5E8?logo=snowflake)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python)

---

## 🧠 Problem Statement

Crypto traders and analysts need to understand how news cycles affect market movements. This platform ingests live tech news via **HackerNews + News APIs** and real-time crypto prices via **Coinbase WebSocket**, runs dual batch + streaming pipelines, and surfaces correlations between trending stories and price movements in a unified Gold layer.

---

## 🏗️ Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                          │
│   HackerNews API          News API          Coinbase WS       │
│   (top stories)        (tech articles)    (BTC/ETH/SOL)       │
└──────────┬─────────────────┬──────────────────┬──────────────┘
           │                 │                  │
           │  BATCH (15min)  │                  │  STREAMING (real-time)
           ▼                 ▼                  ▼
┌──────────────────┐                  ┌──────────────────────┐
│     AIRFLOW      │                  │    KAFKA + SPARK      │
│  (Orchestration) │                  │  (Stream Processing)  │
│                  │                  │                       │
│ fetch_hackernews │                  │  crypto_producer.py   │
│ fetch_news       │                  │  → topic:             │
│ load_to_bronze   │                  │    crypto_prices      │
│ transform_silver │                  │  → Spark Streaming    │
│ transform_gold   │                  │    (10s micro-batch)  │
│ combined_insights│                  │    (1-min OHLC)       │
└────────┬─────────┘                  └──────────┬───────────┘
         │                                        │
         └──────────────────┬─────────────────────┘
                            ▼
          ┌─────────────────────────────────────┐
          │              SNOWFLAKE               │
          │  Bronze → Silver → Gold (Medallion)  │
          └─────────────────────────────────────┘
```

---

## 🛠️ Tech Stack

| Layer | Technology | Purpose |
|---|---|---|
| Orchestration | Apache Airflow 2.8.1 | DAG scheduling, task dependencies |
| Message Broker | Apache Kafka 7.4.0 | Real-time crypto price streaming |
| Stream Processing | Spark Streaming 3.5.4 | Micro-batch + OHLC aggregations |
| Data Warehouse | Snowflake | Storage + SQL transformations |
| Containerization | Docker Compose | 6-service local deployment |
| Language | Python 3.8+ | API ingestion, pipeline logic |

---

## ❄️ Snowflake Data Model (Medallion Architecture)

| Layer | Table | Description |
|---|---|---|
| 🥉 Bronze | `RAW_SOCIAL_MEDIA_DATA` | Raw JSON from HackerNews + News API |
| 🥉 Bronze | `RAW_CRYPTO_STREAM` | Raw real-time crypto prices from Coinbase |
| 🥉 Bronze | `PIPELINE_RUNS` | Pipeline run metadata |
| 🥈 Silver | `SOCIAL_POSTS_CLEAN` | Cleaned, deduplicated news posts |
| 🥈 Silver | `CRYPTO_PRICES_CLEAN` | Validated & normalized price records |
| 🥇 Gold | `DAILY_TOP_STORIES` | Top 10 stories by score per day |
| 🥇 Gold | `DAILY_SOURCE_SUMMARY` | Daily stats aggregated per source |
| 🥇 Gold | `CRYPTO_ANALYTICS` | 1-min OHLC windows (BTC/ETH/SOL) |
| 🥇 Gold | `COMBINED_INSIGHTS` ⭐ | News sentiment ↔ crypto price correlation |

---

## ✨ Key Features

- **Dual Pipeline Architecture** — Batch (Airflow, 15-min schedule) + Streaming (Kafka/Spark) running simultaneously
- **Medallion Architecture** — Bronze → Silver → Gold with clear data contracts
- **Idempotent Pipeline** — Safe to re-run; DELETE before INSERT prevents duplicates
- **Bulk Loading** — Single SQL call loads all records vs row-by-row inserts
- **OHLC Aggregations** — 1-minute Open/High/Low/Close windows via Spark windowing
- **Cross-Domain Insights** — `COMBINED_INSIGHTS` joins news sentiment with live crypto prices
- **Data Quality** — `QUALIFY ROW_NUMBER()` deduplication, null filtering, title cleaning

---

## 🔄 Airflow Pipeline Flow

```
fetch_hackernews ─┐
                  ├─→ load_to_snowflake → transform_to_silver → transform_to_gold → build_combined_insights
fetch_news ───────┘
(parallel)          (BRONZE bulk load)   (SQL in Snowflake)   (aggregations)      (news ↔ crypto join)
```

---

## 📦 Project Structure

```
social-crypto-intelligence-platform/
├── dags/
│   ├── load_to_snowflake.py       # Main Airflow DAG (6 tasks)
│   ├── crypto_producer.py         # Kafka producer (Coinbase WebSocket)
│   ├── spark_crypto_stream.py     # Spark Streaming job (OHLC aggregations)
│   └── crypto_consumer.py         # Kafka consumer (dev/testing)
├── docker-compose.yml             # 6-service Docker setup
├── snowflake_setup.sql            # Run first — creates all tables
├── logs/                          # Airflow task logs
├── plugins/                       # Airflow plugins
├── .gitignore
└── README.md
```

---

## 🚀 How to Run

### Prerequisites
- Docker Desktop (4GB+ RAM allocated)
- Python 3.8+
- Snowflake account (free trial works)

### Step 1 — Clone and start Docker services
```bash
git clone https://github.com/ugeebindu15/social-crypto-intelligence-platform
cd social-crypto-intelligence-platform
docker compose up -d
```

Verify all 6 services are running:
```bash
docker compose ps
```

### Step 2 — Create Snowflake tables
Run `snowflake_setup.sql` in your Snowflake worksheet to create the Bronze/Silver/Gold schema.

### Step 3 — Add Snowflake connection to Airflow
```bash
docker compose exec airflow-webserver airflow connections add 'snowflake_default' \
  --conn-type 'snowflake' \
  --conn-login 'YOUR_SNOWFLAKE_USER' \
  --conn-password 'YOUR_PASSWORD' \
  --conn-schema 'BRONZE' \
  --conn-extra '{"account": "YOUR_ACCOUNT", "warehouse": "AIRFLOW_WH", "database": "SOCIAL_ANALYTICS", "role": "ACCOUNTADMIN"}'
```

### Step 4 — Start the streaming pipeline
```bash
# Terminal 1 — Start Kafka producer (Coinbase WebSocket)
python dags/crypto_producer.py

# Terminal 2 — Start Spark Streaming job
python dags/spark_crypto_stream.py
```

### Step 5 — Trigger the Airflow DAG
Go to `localhost:8080` → unpause `social_media_to_snowflake` → trigger manually or wait 15 minutes for auto-run.

---

## 📊 Sample Output — COMBINED_INSIGHTS

```
Date          : 2026-02-19
Top HN Story  : Gemini 3.1 Pro
HN Score      : 572
Total Articles: 45
BTC Avg Price : $66,837.87
ETH Avg Price : $1,942.87
SOL Avg Price : $82.21
BTC Change    : -1.2% (vs previous day)
```

---

## 🏛️ Architecture Decisions

**Why ELT over ETL?**
Raw data lands in Bronze first; transformations happen inside Snowflake using SQL. Snowflake scales compute better than Airflow workers for heavy transformations.

**Why Kafka over direct API polling?**
Kafka decouples producer and consumer, enables fault tolerance, and makes it trivial to add new consumers (e.g., alerts, ML models) without touching the producer.

**Why Spark Streaming over Kafka Streams?**
Spark's micro-batch model integrates naturally with Snowflake bulk loads and supports complex windowed aggregations (OHLC) out of the box.

---

## 👩‍💻 Author

**Hima Bindu Thondamanati**
MS Computer Science — Cleveland State University
[GitHub](https://github.com/ugeebindu15) · [LinkedIn](https://linkedin.com/in/your-profile)
