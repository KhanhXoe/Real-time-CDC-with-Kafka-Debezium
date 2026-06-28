# Real-time CDC Pipeline

A production-style **Change Data Capture (CDC)** pipeline that streams every INSERT, UPDATE, and DELETE from PostgreSQL to Kafka in real time — no polling, no delay. Built as a local proof-of-concept with a clear migration path to AWS.

---

## Architecture

```
┌─────────────────────┐   WAL (logical replication)   ┌─────────────────┐
│   PostgreSQL 15     │ ─────────────────────────────► │    Debezium     │
│  schema: ecommerce  │                                │  Kafka Connect  │
└─────────────────────┘                                └────────┬────────┘
                                                                │ JSON events
                                                       ┌────────▼────────┐
                                                       │      Kafka      │
                                                       │   (KRaft mode)  │
                                                       └────────┬────────┘
                                          ┌────────────────────┼────────────────────┐
                                          │                    │                    │
                               ┌──────────▼──────┐  ┌─────────▼──────┐  ┌─────────▼──────┐
                               │   Kafka UI      │  │   Dashboard    │  │  Future sinks  │
                               │  (monitoring)   │  │  (Streamlit)   │  │  Spark, Flink  │
                               └─────────────────┘  └────────────────┘  └────────────────┘
```

**How it works:**
1. PostgreSQL writes all changes to its **WAL (Write-Ahead Log)** in `logical` mode.
2. Debezium reads the WAL via a **replication slot** and **publication**, packaging each change as a JSON event.
3. Events land on **Kafka topics** (`cdc.ecommerce.<table>`), partitioned and retained for downstream consumers.
4. The Streamlit dashboard consumes topics directly and renders live metrics.

---

## Tech Stack

| Service | Image / Tool | Port | Role |
|---------|-------------|------|------|
| PostgreSQL 15 | `postgres:15` | `5432` | Source database — logical replication enabled |
| Kafka | `confluentinc/cp-kafka:latest` | `9092` / `29092` | Message broker — KRaft mode (no Zookeeper) |
| Debezium | `debezium/connect:2.7.3.Final` | `8083` | CDC engine — bridges Postgres WAL to Kafka |
| Kafka UI | `provectuslabs/kafka-ui:latest` | `8080` | Web UI to inspect topics and messages |
| Adminer | `adminer:latest` | `8081` | Lightweight PostgreSQL web UI |
| Dashboard | `./dashboard` (Streamlit) | `8501` | Real-time CDC event dashboard from Kafka |

---

## Data Model

Schema `ecommerce` in database `enterprise_db` — simulates a computer hardware e-commerce store.

```
ecommerce
├── customers        — buyers with loyalty tiers: BRONZE / SILVER / GOLD / PLATINUM
├── products         — catalog (CPU, GPU, RAM, SSD, PSU, Case, Cooler, Monitor, Peripheral)
├── orders           — orders with auto-generated number (ORD-YYYY-NNNNNN)
├── order_items      — line items per order (FK → orders, products)
├── inventory        — stock levels per product across warehouses A/B/C
├── audit_logs       — manual change tracking log
├── cdc_heartbeat    — Debezium heartbeat table (prevents WAL bloat)
├── debezium_signals — send signals to Debezium (incremental snapshot, etc.)
└── cdc_metrics      — trigger-based counter: INSERT/UPDATE/DELETE per table
```

Debezium tracks 6 tables via `cdc_publication`:
`customers`, `products`, `orders`, `order_items`, `inventory`, `debezium_signals`

---

## Project Structure

```
Real-time CDC/
├── docker-compose.yml
├── requirements.txt              # host-side deps for generate_data.py
├── postgres/
│   ├── init.sql                  # schema, seed data, triggers, functions
│   ├── postgresql.conf           # logical replication settings
│   └── pg_hba.conf
├── debezium/
│   └── postgres-connector.json   # connector config
├── scripts/
│   └── generate_data.py          # continuous data generator
└── dashboard/
    ├── app.py                    # Streamlit real-time dashboard
    ├── Dockerfile
    └── requirements.txt
```

---

## Quick Start

### 1. Start the stack

```bash
docker compose up -d
```

Wait ~30 seconds for all services to become healthy:

```bash
docker compose ps
```

### 2. Register the Debezium connector

This step is **required** — without it Kafka receives nothing.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-connector.json
```

Verify it is running:

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

### 3. Generate data

```bash
pip install -r requirements.txt

# Default: 2 orders every 5 seconds
python scripts/generate_data.py

# Faster for demo
python scripts/generate_data.py --interval 2 --orders-per-tick 5
```

| Flag | Default | Description |
|------|---------|-------------|
| `--interval` | `5.0` | Seconds between ticks |
| `--orders-per-tick` | `2` | Orders created per tick |
| `--new-customers` | `1` | New customers per tick (40% chance) |
| `--once` | off | Run one tick then exit |

### 4. Open the UIs

| UI | URL | Credentials |
|----|-----|-------------|
| Kafka UI | http://localhost:8080 | — |
| Adminer (DB) | http://localhost:8081 | Server: `postgres` · User: `admin` · Password: `admin` · DB: `enterprise_db` |
| CDC Dashboard | http://localhost:8501 | — |
| Debezium API | http://localhost:8083 | — |

---

## Kafka Topics

| Topic | Source table |
|-------|-------------|
| `cdc.ecommerce.customers` | customers |
| `cdc.ecommerce.products` | products |
| `cdc.ecommerce.orders` | orders |
| `cdc.ecommerce.order_items` | order_items |
| `cdc.ecommerce.inventory` | inventory |
| `debezium-config` | Debezium internal |
| `debezium-offsets` | Debezium offset tracking |
| `debezium-status` | Debezium connector state |

Each event payload:

```json
{
  "before": { "id": "...", "status": "PROCESSING" },
  "after":  { "id": "...", "status": "SHIPPED" },
  "op":     "u",
  "ts_ms":  1782662915000,
  "source": { "table": "orders", "lsn": 12345678 }
}
```

`op` values: `c` = INSERT · `u` = UPDATE · `d` = DELETE · `r` = snapshot READ

---

## Key Configuration

### PostgreSQL — logical replication (`postgresql.conf`)

```ini
wal_level = logical        # required for CDC
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 1GB        # retain WAL long enough for Debezium to catch up
```

### Debezium connector highlights (`debezium/postgres-connector.json`)

```json
"plugin.name":              "pgoutput",
"snapshot.mode":            "initial",
"replica.identity.autoset": "ecommerce.*:FULL",
"heartbeat.interval.ms":    "10000",
"provide.transaction.metadata": "true",
"decimal.handling.mode":    "double"
```

`FULL` replica identity ensures `before` values are always present on UPDATE/DELETE.

### Kafka — KRaft mode (no Zookeeper)

Two listeners are configured:
- `PLAINTEXT://kafka:9092` — for inter-container communication (Debezium, Dashboard in Docker)
- `PLAINTEXT_HOST://localhost:29092` — for connections from the host machine

---

## Reset

Full reset — wipes all data and volumes:

```bash
docker compose down
rm -rf postgres/data
docker compose up -d
# Re-register the connector (step 2 above)
```

---

---

# Phase 2 — Scaling to AWS

## Target Architecture

```
┌──────────────────────┐     CDC / DMS      ┌──────────────────┐
│  Amazon RDS          │ ──────────────────► │   Amazon MSK     │
│  (PostgreSQL 15)     │  or Debezium        │   (Managed Kafka)│
│  Multi-AZ, encrypted │  on ECS Fargate     └────────┬─────────┘
└──────────────────────┘                              │
                                     ┌────────────────┼─────────────────────┐
                                     │                │                     │
                           ┌─────────▼──────┐  ┌─────▼──────────┐  ┌──────▼──────────┐
                           │  Kinesis Data  │  │   AWS Lambda   │  │  Apache Flink   │
                           │  Firehose      │  │  (event-driven)│  │  on Amazon EMR  │
                           └─────────┬──────┘  └─────┬──────────┘  └──────┬──────────┘
                                     │               │                     │
                            ┌────────▼────────┐  ┌──▼───────────┐  ┌──────▼──────────┐
                            │   Amazon S3     │  │  DynamoDB    │  │  Amazon         │
                            │  (Data Lake)    │  │  (Real-time) │  │  Redshift       │
                            └────────┬────────┘  └──────────────┘  │  (Data Warehouse│
                                     │                              └──────┬──────────┘
                              ┌──────▼──────┐                      ┌──────▼──────────┐
                              │  AWS Glue   │                      │  Amazon         │
                              │  (ETL/Cat.) │                      │  QuickSight     │
                              └─────────────┘                      └─────────────────┘
```

## Migration Map: Local → AWS

| Local component | AWS equivalent | Notes |
|----------------|---------------|-------|
| PostgreSQL 15 (Docker) | **Amazon RDS PostgreSQL** or **Aurora PostgreSQL** | Enable logical replication: `rds.logical_replication = 1` |
| Debezium (Docker) | **Debezium on ECS Fargate** or **AWS DMS** | DMS is simpler to operate; Debezium gives more control |
| Kafka (KRaft) | **Amazon MSK** (Managed Kafka) | MSK removes broker management overhead |
| `./postgres/data` bind mount | **RDS automated backups + Multi-AZ** | Point-in-time recovery built in |
| Streamlit Dashboard | **Amazon ECS** + **Application Load Balancer** | Or replace with QuickSight for managed BI |
| Manual connector registration | **AWS Lambda** or **Step Functions** | Automate connector lifecycle on deploy |
| `generate_data.py` (local) | **ECS Task** or **AWS Batch** | Run as a scheduled or on-demand task |

## Phase 2 Roadmap

### Stage 1 — Lift & Shift (low risk)
- Migrate PostgreSQL → **RDS PostgreSQL 15** with `logical_replication` enabled
- Move Kafka → **Amazon MSK** (2 brokers, 3 AZs)
- Deploy Debezium as an **ECS Fargate** service (containerize the existing image)
- Store connector config in **AWS Secrets Manager**

### Stage 2 — Cloud-native sinks
- Add **Kafka Connect S3 Sink** → land raw CDC events in **S3** as Parquet (data lake)
- Add **Kafka Connect Redshift Sink** → stream aggregated data into **Redshift** for BI
- Catalog S3 data with **AWS Glue Data Catalog** + run ad-hoc queries via **Athena**

### Stage 3 — Real-time processing
- Deploy **Apache Flink on Amazon EMR** (or **Kinesis Data Analytics**) for stream aggregations:
  - Rolling revenue per customer tier
  - Inventory reorder alerts
  - Order lifecycle SLA monitoring
- Write aggregated results to **DynamoDB** for low-latency API reads

### Stage 4 — Observability & governance
- **CloudWatch** metrics for MSK lag, Debezium connector health, ECS CPU/memory
- **AWS Glue** data quality checks on S3 landing zone
- **Lake Formation** for fine-grained column-level access control
- **SNS / PagerDuty** alerts on connector failures or WAL lag spikes

## Cost Drivers to Watch

| Resource | Cost lever |
|----------|-----------|
| MSK | Broker instance type and count — start with `kafka.m5.large` x2 |
| RDS | Multi-AZ doubles storage I/O cost — enable only in production |
| Kinesis Firehose | Charged per GB delivered — compress with GZIP before S3 |
| Redshift | Use **Serverless** for dev/staging; provisioned clusters for production |
| Glue ETL | Billed per DPU-hour — batch jobs at off-peak hours |

## What Stays the Same

The Debezium connector config (`debezium/postgres-connector.json`), the schema design, and the Kafka topic naming convention all carry forward unchanged. The only difference is the broker endpoint and authentication method (IAM or SASL/SCRAM on MSK).
