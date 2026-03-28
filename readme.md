# Oil Price Real-Time Streaming Pipeline

End-to-end open-source data pipeline for real-time global oil price analytics.
Kafka → Stream Processing → TimescaleDB → Grafana / Superset.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  INGEST                                                         │
│  GBM Simulator (prod: CME / ICE / ADNOC WebSocket)             │
│  kafka-python producer · Avro + Schema Registry                 │
└───────────────────────────┬─────────────────────────────────────┘
                            │  oil.prices.raw
┌───────────────────────────▼─────────────────────────────────────┐
│  KAFKA (KRaft, no ZooKeeper)                                    │
│  oil.prices.raw · oil.prices.agg.1m · oil.alerts · oil.vwap    │
└──────┬────────────────────┬────────────────────────────┬────────┘
       │                    │                            │
┌──────▼──────┐   ┌─────────▼──────────┐   ┌────────────▼───────┐
│ Streams     │   │ Apache Flink       │   │ Alert Engine       │
│ Processor   │   │ (optional heavy    │   │ CEP — spike/drop/  │
│ OHLCV·VWAP  │   │  windowing jobs)   │   │ spread/vol alerts  │
└──────┬──────┘   └────────────────────┘   └────────────┬───────┘
       │                                                │
       ├──────────────────────────────────────────────┐ │
       ▼                                              ▼ ▼
┌─────────────────┐   ┌──────────────┐   ┌─────────────────────┐
│  TimescaleDB    │   │  Redis       │   │  MinIO + Iceberg    │
│  oil_ticks      │   │  live cache  │   │  Parquet data lake  │
│  oil_ohlcv_1m   │   │  pub/sub     │   │  (backfill/ML)      │
│  oil_alerts     │   │  dashboards  │   └─────────────────────┘
│  oil_volatility │   └──────────────┘
│  (hypertables)  │
└────────┬────────┘
         │
┌────────▼──────────────────────────────────────────────────────┐
│  ANALYTICS                                                     │
│  Grafana :3000   · Superset :8088  · dbt models               │
│  Kafka UI :8080  · Flink UI :8082  · MinIO UI :9001           │
└────────────────────────────────────────────────────────────────┘
```

---

## Stack (100% open source)

| Layer | Tool | Version | License |
|---|---|---|---|
| Message bus | Apache Kafka (KRaft) | 7.6.0 | Apache 2.0 |
| Schema management | Confluent Schema Registry | 7.6.0 | Apache 2.0 |
| Kafka UI | Provectus Kafka UI | latest | Apache 2.0 |
| Stream processing | Python confluent-kafka + Flink | 1.18 | Apache 2.0 |
| Time-series DB | TimescaleDB on PostgreSQL 15 | latest | Apache 2.0 |
| Live cache | Valkey (Redis OSS fork) | 7.2 | BSD |
| Object storage | MinIO | latest | AGPL 3.0 |
| Transformation | dbt Core | 1.7 | Apache 2.0 |
| Dashboards | Grafana OSS | 10.3.1 | AGPL 3.0 |
| BI / Analytics | Apache Superset | 3.1.0 | Apache 2.0 |

---

## Prerequisites

- Docker Desktop ≥ 24 (or Docker Engine + Compose v2)
- 8 GB RAM allocated to Docker (12 GB recommended)
- Ports free: 3000, 5432, 6379, 8080–8082, 8088, 9000–9001, 9092, 9101

---

## Quick Start

### 1. Clone and enter the project

```bash
git clone https://github.com/your-org/oil-pipeline.git
cd oil-pipeline
```

### 2. Make init script executable

```bash
chmod +x init_scripts/create_topics.sh
```

### 3. Start the full stack

```bash
docker compose up -d
```

Watch startup order (takes ~90 seconds on first run):

```bash
docker compose logs -f producer streams-processor
```

### 4. Verify everything is running

```bash
docker compose ps
```

Expected output — all services should be `healthy` or `running`:

```
NAME                  STATUS          PORTS
kafka                 healthy         0.0.0.0:9092->9092
schema-registry       healthy         0.0.0.0:8081->8081
timescaledb           healthy         0.0.0.0:5432->5432
redis                 healthy         0.0.0.0:6379->6379
oil-producer          running
streams-processor     running
grafana               healthy         0.0.0.0:3000->3000
superset              running         0.0.0.0:8088->8088
flink-jobmanager      healthy         0.0.0.0:8082->8081
kafka-ui              running         0.0.0.0:8080->8080
minio                 healthy         0.0.0.0:9000-9001->9000-9001
```

---

## Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Grafana (dashboards) | http://localhost:3000 | admin / admin |
| Apache Superset (BI) | http://localhost:8088 | admin / admin |
| Kafka UI | http://localhost:8080 | — |
| Flink UI | http://localhost:8082 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Schema Registry API | http://localhost:8081 | — |

---

## Verifying Data Flow

### Check Kafka topics are receiving messages

```bash
# List topics
docker exec kafka kafka-topics --bootstrap-server localhost:9092 --list

# Tail raw ticks (Ctrl+C to stop)
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic oil.prices.raw \
  --from-beginning \
  --max-messages 5

# Check topic offsets (message counts)
docker exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group oil-streams-processor \
  --describe
```

### Query TimescaleDB directly

```bash
docker exec -it timescaledb psql -U oiluser -d oildb
```

```sql
-- Latest prices per benchmark
SELECT benchmark, price, time
FROM oil_ticks
ORDER BY time DESC
LIMIT 9;

-- 1-minute OHLCV candles
SELECT time, benchmark, open, high, low, close, volume, vwap
FROM oil_ohlcv_1m
ORDER BY time DESC
LIMIT 10;

-- Recent alerts
SELECT time, benchmark, alert_type, severity, message
FROM oil_alerts
ORDER BY time DESC
LIMIT 10;

-- Hourly VWAP from continuous aggregate
SELECT bucket, benchmark, vwap, volume
FROM oil_hourly
WHERE bucket > NOW() - INTERVAL '6 hours'
ORDER BY bucket DESC;

-- Compression stats
SELECT hypertable_name,
       pg_size_pretty(before_compression_total_bytes) AS before,
       pg_size_pretty(after_compression_total_bytes)  AS after
FROM hypertable_compression_stats(NULL);
```

### Check Redis live cache

```bash
docker exec -it redis valkey-cli

# Get live WTI price
HGETALL oil:live:WTI

# Check recent price time series (last 10 entries)
ZRANGE oil:ts:WTI -10 -1 WITHSCORES

# Subscribe to live price stream
SUBSCRIBE oil:tick:WTI
```

---

## Project Structure

```
oil-pipeline/
├── docker-compose.yml              # Full stack definition
│
├── producer/                       # Python Kafka producer
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── producer.py                 # GBM simulator + Kafka publish
│   └── oil_tick.avsc               # Avro schema
│
├── kafka_streams/                  # Stream processor (consumer + sinks)
│   ├── Dockerfile
│   ├── requirements.txt
│   └── processor.py                # OHLCV, VWAP, alerts, DB sink
│
├── flink_jobs/                     # Apache Flink PyFlink jobs
│   └── flink_oil_analytics.py      # Windowed aggregations (optional)
│
├── dbt_project/                    # dbt transformation models
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   └── stg_oil_ticks.sql
│       └── marts/
│           ├── oil_daily_analytics.sql
│           └── oil_spread_analysis.sql
│
├── grafana/
│   ├── provisioning/
│   │   ├── datasources/datasources.yml
│   │   └── dashboards/dashboards.yml
│   └── dashboards/
│       └── oil_realtime.json       # Auto-provisioned dashboard
│
├── superset/
│   └── superset_config.py
│
└── init_scripts/
    ├── create_topics.sh            # Kafka topic creation
    └── init_timescale.sql          # DB schema + hypertables
```

---

## dbt Transformations

Run dbt models manually after data has been flowing for a few minutes:

```bash
# Install dbt
pip install dbt-postgres

# Run from dbt_project directory
cd dbt_project

# Test connection
dbt debug --profiles-dir .

# Run all models
dbt run --profiles-dir .

# Run tests
dbt test --profiles-dir .

# Generate docs
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .
```

Or run inside Docker against the running TimescaleDB:

```bash
docker run --rm \
  --network oil-pipeline_oil-net \
  -v $(pwd)/dbt_project:/usr/app \
  -w /usr/app \
  ghcr.io/dbt-labs/dbt-postgres:1.7.latest \
  dbt run --profiles-dir .
```

---

## Superset — Adding the TimescaleDB datasource

1. Go to http://localhost:8088 → Settings → Database Connections → + Database
2. Select **PostgreSQL**
3. Use this connection string:
   ```
   postgresql://oiluser:oilpass@timescaledb:5432/oildb
   ```
4. Click **Test Connection** → Save
5. Go to **SQL Lab** and run:
   ```sql
   SELECT time_bucket('1 minute', time) AS minute,
          benchmark,
          avg(price) AS avg_price,
          sum(volume) AS total_volume
   FROM oil_ticks
   WHERE time > NOW() - INTERVAL '1 hour'
   GROUP BY 1, 2
   ORDER BY 1 DESC;
   ```
6. Save as chart → Add to dashboard

---

## Grafana — Useful Dashboard Queries

### Real-time candlestick (paste into Explore)

```sql
SELECT
    time_bucket('1 minute', time) AS time,
    first(price, time)  AS open,
    max(price)          AS high,
    min(price)          AS low,
    last(price, time)   AS close
FROM oil_ticks
WHERE benchmark = 'WTI'
  AND $__timeFilter(time)
GROUP BY 1
ORDER BY 1;
```

### Spread heatmap

```sql
SELECT
    time_bucket('5 minutes', t.time) AS time,
    ABS(w.price - b.price) AS wti_brent_spread
FROM oil_ticks t
JOIN LATERAL (
    SELECT price FROM oil_ticks WHERE benchmark='WTI'
      AND time = t.time ORDER BY time DESC LIMIT 1
) w ON true
JOIN LATERAL (
    SELECT price FROM oil_ticks WHERE benchmark='BRENT'
      AND time = t.time ORDER BY time DESC LIMIT 1
) b ON true
WHERE $__timeFilter(t.time)
GROUP BY 1
ORDER BY 1;
```

### Alert frequency by type

```sql
SELECT
    time_bucket('10 minutes', time) AS time,
    alert_type,
    count(*) AS alert_count
FROM oil_alerts
WHERE $__timeFilter(time)
GROUP BY 1, 2
ORDER BY 1;
```

---

## Connecting Real Data Sources (Production)

Replace the simulator in `producer/producer.py` with real feed adapters:

### CME DataMine (WTI futures)
```python
import websocket, json

def on_message(ws, message):
    data = json.loads(message)
    tick = {
        "benchmark": "WTI",
        "exchange": "NYMEX",
        "price": float(data["tradePrice"]),
        "volume": float(data["tradeQuantity"]),
        "event_time": int(data["timestamp"]),
        ...
    }
    publish_to_kafka(tick)

ws = websocket.WebSocketApp(
    "wss://api.cmegroup.com/streaming/v1/market-data",
    header={"Authorization": f"Bearer {CME_API_KEY}"},
    on_message=on_message,
)
ws.run_forever()
```

### ICE Connect (Brent crude)
```python
# ICE provides FIX protocol and REST/WebSocket APIs
# Documentation: https://www.theice.com/market-data/connectivity
import requests
r = requests.get(
    "https://api.theice.com/v1/products/254/market-data",
    headers={"ICE-Api-Key": ICE_API_KEY}
)
```

### Free alternatives for development
- **Alpha Vantage** (free tier): `https://www.alphavantage.co/query?function=WTI&interval=daily`
- **EIA API** (US Energy Information Administration, free): `https://api.eia.gov/v2/petroleum/pri/spt/data/`
- **Yahoo Finance** (yfinance Python library): symbols `CL=F` (WTI), `BZ=F` (Brent)

---

## Scaling for Production

### Kafka
- Increase replication factor to 3 (requires 3-broker cluster)
- Enable rack-aware replication
- Use Kafka MirrorMaker 2 for geo-replication

### TimescaleDB
- Use TimescaleDB Multinode for horizontal scaling
- Or switch to Citus extension for distributed PostgreSQL

### Stream Processor
- Scale horizontally: run multiple `streams-processor` replicas
- Kafka consumer groups handle partition assignment automatically

### Redis
- Switch to Redis Cluster or Valkey Cluster for HA
- Use Sentinel for automatic failover

### Monitoring
- Add Prometheus + Node Exporter for infrastructure metrics
- Kafka JMX metrics via `kafka-jmx-exporter`
- AlertManager for PagerDuty / Slack routing

---

## Stopping and Cleanup

```bash
# Stop all services (preserve data volumes)
docker compose down

# Stop and remove all data (full reset)
docker compose down -v

# View logs for a specific service
docker compose logs -f producer
docker compose logs -f streams-processor
docker compose logs -f kafka
```

---

## Troubleshooting

**Producer exits immediately**
```bash
docker compose logs producer
# Usually Schema Registry not ready — it retries automatically
# Wait 30s and restart: docker compose restart producer
```

**TimescaleDB hypertable errors**
```bash
docker exec -it timescaledb psql -U oiluser -d oildb -c "\dx"
# Verify timescaledb extension is listed
```

**Kafka topics not created**
```bash
docker compose logs topic-init
# Re-run: docker compose run --rm topic-init
```

**No data in Grafana**
```bash
# Check streams-processor is consuming
docker compose logs streams-processor | tail -50
# Verify TimescaleDB has rows
docker exec timescaledb psql -U oiluser -d oildb -c "SELECT count(*) FROM oil_ticks;"
```

---

## License

Apache 2.0 — see LICENSE file.
