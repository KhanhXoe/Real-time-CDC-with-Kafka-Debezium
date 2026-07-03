# Real-time CDC Pipeline

A local **Change Data Capture (CDC)** lab for a realistic computer-hardware ecommerce domain.

The current pipeline streams PostgreSQL row changes through Debezium into Kafka, then exposes the stream through Kafka UI and a Streamlit dashboard. Business changes are generated through a FastAPI mock source application instead of direct random inserts, so the CDC events look like real customer, cart, order, payment, shipment, and inventory workflows.

Target path:

```text
Source business API -> PostgreSQL -> Debezium -> Kafka -> Dashboard / Consumers
```

The longer-term lakehouse target remains:

```text
PostgreSQL -> Debezium -> Kafka -> Flink -> Iceberg -> MinIO/S3
```

---

## Architecture

```text
┌──────────────────────┐       SQL writes        ┌──────────────────────┐
│ Mock Business API    │ ──────────────────────► │ PostgreSQL 15         │
│ FastAPI, host:8000   │                         │ schema: ecommerce     │
└──────────────────────┘                         └──────────┬───────────┘
                                                             │ WAL logical replication
                                                   ┌─────────▼──────────┐
                                                   │ Debezium Connect   │
                                                   │ pgoutput plugin    │
                                                   └─────────┬──────────┘
                                                             │ JSON CDC events
                                                   ┌─────────▼──────────┐
                                                   │ Kafka, KRaft mode  │
                                                   └─────────┬──────────┘
                                      ┌──────────────────────┼──────────────────────┐
                                      │                      │                      │
                            ┌─────────▼─────────┐  ┌─────────▼─────────┐  ┌─────────▼─────────┐
                            │ Kafka UI          │  │ Streamlit         │  │ Future consumers  │
                            │ topic inspection  │  │ CDC dashboard     │  │ Flink/lakehouse   │
                            └───────────────────┘  └───────────────────┘  └───────────────────┘
```

How it works:

1. The mock API performs valid ecommerce actions against PostgreSQL.
2. PostgreSQL writes all table changes to WAL with `wal_level = logical`.
3. Debezium reads the WAL through publication `cdc_publication` and slot `debezium_slot`.
4. Debezium publishes JSON events to Kafka topics named `cdc.ecommerce.<table>`.
5. Kafka UI and the Streamlit dashboard consume the topics for inspection and live metrics.

---

## Tech Stack

| Component | Tool / Image | Port | Role |
|-----------|--------------|------|------|
| PostgreSQL | `postgres:15` | `5432` | Source operational database with logical replication |
| Kafka | `confluentinc/cp-kafka:latest` | `9092`, `29092` | KRaft broker for CDC topics |
| Debezium | `debezium/connect:2.7.3.Final` | `8083` | PostgreSQL WAL to Kafka connector |
| Kafka UI | `provectuslabs/kafka-ui:latest` | `8080` | Topic, message, and connector inspection |
| Adminer | `adminer:latest` | `8081` | PostgreSQL web UI |
| Dashboard | `./dashboard` Streamlit app | `8501` | Real-time CDC event dashboard |
| Mock API | `scripts/mock_business_api.py` | `8000` | Source-app simulator and scenario scheduler |

Host Python dependencies are in `requirements.txt`.

---

## Data Model

Database: `enterprise_db`

Schema: `ecommerce`

The schema models an ecommerce business that sells computer hardware:

| Area | Tables |
|------|--------|
| Customer | `customers`, `customer_addresses` |
| Catalog | `brands`, `categories`, `products`, `product_categories` |
| Supplier | `suppliers`, `supplier_products` |
| Inventory | `warehouses`, `inventory`, `inventory_movements` |
| Purchasing | `purchase_orders`, `purchase_order_items` |
| Cart | `carts`, `cart_items` |
| Order | `orders`, `order_items` |
| Payment | `payments` |
| Fulfillment | `shipments` |
| Return | `returns`, `return_items` |
| Promotion | `promotions` |
| CDC support | `cdc_heartbeat`, `debezium_signals`, `cdc_metrics`, `audit_logs` |

See:

- `docs/data-arch/business_flow.md` for business semantics.
- `docs/data-arch/system_flow.md` for CDC and target system flow.
- `docs/data-arch/erDiagram.txt` for the ERD source.

---

## Project Structure

```text
.
├── docker-compose.yml
├── requirements.txt
├── postgres/
│   ├── init.sql                  # ecommerce schema, triggers, publication, helper views
│   ├── postgresql.conf           # logical replication settings
│   └── pg_hba.conf
├── debezium/
│   └── postgres-connector.json   # Debezium PostgreSQL connector config
├── scripts/
│   └── mock_business_api.py      # FastAPI source-app simulator
├── dashboard/
│   ├── app.py                    # Streamlit CDC dashboard
│   ├── Dockerfile
│   └── requirements.txt
└── docs/
    ├── README.md
    └── data-arch/
        ├── business_flow.md
        ├── erDiagram.txt
        └── system_flow.md
```

---

## Quick Start

### 1. Start infrastructure

```bash
docker compose up -d
docker compose ps
```

Wait until PostgreSQL, Kafka, Debezium, Kafka UI, and the dashboard are healthy or running.

### 2. Register the Debezium connector

This step is required after a fresh start/reset.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-connector.json
```

Check connector status:

```bash
curl -s http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

### 3. Run the mock business API

The API runs on the host and connects to PostgreSQL through `localhost:5432`.

```bash
pip install -r requirements.txt
uvicorn scripts.mock_business_api:app --host 0.0.0.0 --port 8000
```

Open the control panel:

```text
http://localhost:8000
```

Useful API checks:

```bash
curl http://localhost:8000/health
curl http://localhost:8000/actions
```

### 4. Generate business events

Bootstrap catalog and inventory:

```bash
curl -X POST http://localhost:8000/scenarios/bootstrap_catalog \
  -H "Content-Type: application/json" \
  -d '{"products":10,"warehouses":3}'
```

Create one checkout flow:

```bash
curl -X POST http://localhost:8000/scenarios/customer_cart_checkout \
  -H "Content-Type: application/json" \
  -d '{}'
```

Create a paid and shipped order:

```bash
curl -X POST http://localhost:8000/scenarios/paid_shipped_order \
  -H "Content-Type: application/json" \
  -d '{}'
```

Start scheduled random activity:

```bash
curl -X POST http://localhost:8000/schedules \
  -H "Content-Type: application/json" \
  -d '{"scenario":"random_activity","interval_seconds":5,"max_runs":50,"payload":{}}'
```

Available scenarios:

- `bootstrap_catalog`
- `customer_cart_checkout`
- `paid_shipped_order`
- `order_lifecycle_step`
- `replenish_inventory`
- `sales_burst`
- `random_activity`

### 5. Open UIs

| UI | URL | Notes |
|----|-----|-------|
| Mock API control panel | http://localhost:8000 | Run actions, scenarios, and schedules |
| Kafka UI | http://localhost:8080 | Inspect topics, messages, and Debezium connector |
| Adminer | http://localhost:8081 | Server: `postgres`, user: `admin`, password: `admin`, DB: `enterprise_db` |
| CDC Dashboard | http://localhost:8501 | Live Streamlit metrics from Kafka |
| Debezium API | http://localhost:8083 | Kafka Connect REST API |

---

## Kafka Topics

Debezium uses topic prefix `cdc`, so table topics follow this format:

```text
cdc.ecommerce.<table_name>
```

Configured source tables:

```text
customers
customer_addresses
brands
categories
products
product_categories
suppliers
supplier_products
warehouses
inventory
inventory_movements
purchase_orders
purchase_order_items
carts
cart_items
promotions
orders
order_items
payments
shipments
returns
return_items
debezium_signals
```

Debezium internal topics:

```text
debezium-config
debezium-offsets
debezium-status
```

The Streamlit dashboard currently subscribes to:

```text
cdc.ecommerce.customers
cdc.ecommerce.customer_addresses
cdc.ecommerce.products
cdc.ecommerce.orders
cdc.ecommerce.order_items
cdc.ecommerce.inventory
cdc.ecommerce.inventory_movements
cdc.ecommerce.carts
cdc.ecommerce.cart_items
cdc.ecommerce.payments
cdc.ecommerce.shipments
```

Event shape with schemas disabled:

```json
{
  "before": null,
  "after": {
    "order_id": "97d2...",
    "order_status": "paid",
    "total_amount": 1299.0
  },
  "source": {
    "version": "2.7.3.Final",
    "connector": "postgresql",
    "db": "enterprise_db",
    "schema": "ecommerce",
    "table": "orders"
  },
  "op": "c",
  "ts_ms": 1782662915000
}
```

Operation values:

- `c`: insert
- `u`: update
- `d`: delete
- `r`: initial snapshot read

---

## Key Configuration

### PostgreSQL logical replication

`postgres/postgresql.conf`:

```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
wal_keep_size = 1GB
```

`postgres/init.sql` creates:

- Schema `ecommerce`.
- Publication `cdc_publication`.
- Debezium signal table `ecommerce.debezium_signals`.
- Heartbeat table `ecommerce.cdc_heartbeat`.
- Trigger-maintained event counters in `ecommerce.cdc_metrics`.
- Helper views/functions such as `dashboard_metrics`, `cdc_replication_status`, and `get_table_counts()`.

### Debezium connector

`debezium/postgres-connector.json`:

```json
{
  "topic.prefix": "cdc",
  "plugin.name": "pgoutput",
  "publication.name": "cdc_publication",
  "slot.name": "debezium_slot",
  "snapshot.mode": "initial",
  "replica.identity.autoset.values": "ecommerce.*:FULL",
  "heartbeat.interval.ms": "10000",
  "signal.enabled.channels": "source",
  "signal.data.collection": "ecommerce.debezium_signals",
  "decimal.handling.mode": "double",
  "provide.transaction.metadata": "true"
}
```

`replica.identity.autoset.values = ecommerce.*:FULL` keeps full `before` values available for updates and deletes.

The connector masks `ecommerce.customers.phone` with `column.mask.with.length.chars`.

### Kafka listeners

Kafka exposes two listeners:

- `kafka:9092` for containers on the Docker network.
- `localhost:29092` for host-side clients.

The dashboard container uses `KAFKA_BROKER=kafka:9092`.

---

## Reset

Full reset, including local PostgreSQL bind-mounted data and Kafka volume:

```bash
docker compose down -v
rm -rf postgres/data
docker compose up -d
```

Then register the connector again and rerun the mock API scenarios.

---

## Roadmap

The next milestone is to add a streaming lakehouse consumer:

```text
PostgreSQL -> Debezium -> Kafka -> Flink -> Iceberg -> MinIO/S3
```

Expected work:

- Consume `cdc.ecommerce.*` topics from Flink.
- Parse Debezium envelopes into row-level change events.
- Apply inserts, updates, and deletes to Iceberg tables.
- Store Iceberg data locally in MinIO.
- Keep the design portable to AWS equivalents: RDS PostgreSQL, MSK, Managed Service for Apache Flink, S3, and Glue Catalog.
