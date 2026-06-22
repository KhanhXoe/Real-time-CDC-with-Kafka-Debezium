# Real-time CDC Pipeline 🔄

> *"Mày update cái bảng lúc 3 giờ sáng, tao biết ngay lập tức."* — Debezium

Đây là một pipeline **Change Data Capture (CDC)** chạy real-time, bắt mọi thay đổi từ PostgreSQL và đẩy thẳng lên Kafka. Dữ liệu thay đổi một cái là hệ thống downstream nhận được ngay — không polling, không delay, không cần hỏi lại database liên tục như người ngồi F5 trang web bán vé concert.

---

## Luồng chạy tổng quát

```
┌─────────────────┐    WAL Logs     ┌───────────────┐    Events     ┌─────────────┐
│   PostgreSQL 15 │ ─────────────►  │    Debezium   │ ────────────► │    Kafka    │
│  (Source DB)    │  logical repl.  │  (Connector)  │               │  (Broker)   │
└─────────────────┘                 └───────────────┘               └──────┬──────┘
                                                                           │
                                                                    ┌──────▼──────┐
                                                                    │  Kafka UI   │
                                                                    │ (port 8080) │
                                                                    └─────────────┘
```

**Cụ thể hơn:**

1. **PostgreSQL** ghi mọi thay đổi vào **WAL (Write-Ahead Log)** ở chế độ `logical` — đây là tính năng built-in của Postgres, không cần trigger hay gì thêm.
2. **Debezium** đọc WAL thông qua **replication slot** `debezium_slot` và **publication** `cdc_publication` — nó ngồi đó đọc log như người đọc nhật ký người khác.
3. Mỗi sự kiện INSERT/UPDATE/DELETE được Debezium đóng gói thành message JSON rồi đẩy lên **Kafka topic** tương ứng.
4. Downstream systems (Spark, Flink, consumer apps...) subscribe vào topic là nhận được dữ liệu real-time.

---

## Stack công nghệ

| Service | Image | Port | Mô tả |
|---------|-------|------|--------|
| **PostgreSQL 15** | `postgres:15` | `5432` | Source database, chạy ở chế độ logical replication |
| **Kafka** | `confluentinc/cp-kafka:latest` | `9092`, `29092` | Message broker, chạy KRaft mode (không cần Zookeeper — thoát khỏi thứ đó rồi!) |
| **Debezium** | `debezium/connect:2.7.3.Final` | `8083` | CDC engine, bridge giữa Postgres WAL và Kafka |
| **Kafka UI** | `provectuslabs/kafka-ui:latest` | `8080` | Giao diện web để xem topic, message, consumer group |

---

## Data Model

Schema `ecommerce` trong database `enterprise_db`, mô phỏng một hệ thống thương mại điện tử bán linh kiện máy tính:

```
ecommerce
├── customers        — khách hàng với tier BRONZE/SILVER/GOLD/PLATINUM
├── products         — sản phẩm (CPU, GPU, RAM, SSD... hàng xịn cả)
├── orders           — đơn hàng, tự sinh order_number dạng ORD-2026-000001
├── order_items      — chi tiết từng dòng trong đơn
├── inventory        — tồn kho theo warehouse A/B/C
├── audit_logs       — log thay đổi dữ liệu
├── cdc_heartbeat    — bảng heartbeat để Debezium biết mình còn sống
├── debezium_signals — gửi signal cho Debezium (incremental snapshot, v.v.)
└── cdc_metrics      — đếm số lần INSERT/UPDATE/DELETE theo bảng
```

Debezium theo dõi 6 bảng chính: `customers`, `products`, `orders`, `order_items`, `inventory`, `debezium_signals`.

---

## Cấu hình quan trọng

### PostgreSQL — bật logical replication

File `postgres/postgresql.conf`:
```ini
wal_level = logical          # bắt buộc cho CDC
max_wal_senders = 10         # số kết nối replication tối đa
max_replication_slots = 10   # số replication slot tối đa
wal_keep_size = 1GB          # giữ WAL lại đủ lâu để Debezium kịp đọc
```

### Kafka — KRaft mode

Kafka chạy không cần Zookeeper nhờ **KRaft** (`KAFKA_PROCESS_ROLES: broker,controller`). Có hai listener:

- `PLAINTEXT://kafka:9092` — cho các service trong Docker network (Debezium, Kafka UI)
- `PLAINTEXT_HOST://localhost:29092` — cho kết nối từ máy host

> ⚠️ **Lưu ý quan trọng:** `CLUSTER_ID` phải là UUID dạng base64 (22 ký tự), sinh bằng lệnh `kafka-storage random-uuid`. Nếu đổi cluster ID thì phải xóa volume `kafka_data` và chạy lại từ đầu.

### Debezium Connector

Xem chi tiết tại `debezium/postgres-connector.json`. Một vài điểm đáng chú ý:

- `plugin.name: pgoutput` — dùng plugin built-in của Postgres, không cần cài thêm
- `snapshot.mode: initial` — lần đầu chạy sẽ đọc toàn bộ dữ liệu hiện có, sau đó chuyển sang streaming
- `topic.prefix: cdc` — topic sẽ có tên dạng `cdc.ecommerce.customers`
- `column.mask.with.length.chars: ecommerce.customers.phone:12` — tự động mask số điện thoại khách hàng (privacy đàng hoàng)
- `heartbeat.interval.ms: 10000` — gửi heartbeat mỗi 10 giây để WAL không bị giữ lại vô hạn
- `provide.transaction.metadata: true` — gom các thay đổi trong cùng 1 transaction thành 1 batch

---

## Chạy project

### 1. Khởi động toàn bộ stack

```bash
docker compose up -d
```

Đợi khoảng 30-60 giây để các service healthy. Kiểm tra:

```bash
docker compose ps
```

### 2. Đăng ký Debezium connector

Đây là bước **bắt buộc** — không làm bước này thì Kafka trống rỗng như ví ngày cuối tháng.

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @debezium/postgres-connector.json
```

Kiểm tra connector đã chạy chưa:

```bash
curl http://localhost:8083/connectors/postgres-connector/status | python3 -m json.tool
```

### 3. Mở Kafka UI

Truy cập [http://localhost:8080](http://localhost:8080) — sẽ thấy cluster `kafka` và các topic `cdc.ecommerce.*` xuất hiện sau khi connector chạy xong initial snapshot.

### 4. Xem Debezium API

```bash
# Danh sách connector
curl http://localhost:8083/connectors

# Trạng thái connector
curl http://localhost:8083/connectors/postgres-connector/status

# Xóa connector (nếu cần reset)
curl -X DELETE http://localhost:8083/connectors/postgres-connector
```

---

## Reset hoàn toàn

Nếu muốn chạy lại từ đầu (xóa sạch data + volume):

```bash
docker compose down -v
docker compose up -d
```

> `-v` xóa luôn `kafka_data` và `postgres_data`. Sau khi up lại, nhớ đăng ký lại connector ở bước 2.

---

## Các topic Kafka sau khi connector chạy

| Topic | Nội dung |
|-------|---------|
| `cdc.ecommerce.customers` | Thay đổi bảng customers |
| `cdc.ecommerce.products` | Thay đổi bảng products |
| `cdc.ecommerce.orders` | Thay đổi bảng orders |
| `cdc.ecommerce.order_items` | Thay đổi bảng order_items |
| `cdc.ecommerce.inventory` | Thay đổi bảng inventory |
| `debezium-config` | Config nội bộ của Debezium |
| `debezium-offsets` | Offset tracking — Debezium đọc đến đâu rồi |
| `debezium-status` | Trạng thái connector |
