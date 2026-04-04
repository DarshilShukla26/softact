# DataFlow Observatory

**Real-Time Data Quality Monitoring Platform**

> End-to-end streaming pipeline with automated quality validation, alerting, and observability.

---

## Overview

DataFlow Observatory is a full-stack real-time data quality monitoring system built from scratch. It ingests ~200 events/second from two Python simulators, validates every event using Go microservices, stores results in Snowflake with dbt transformations, and surfaces quality metrics through a Prometheus + Grafana dashboard.

---

## Architecture

```
Python Simulators → Apache Kafka (raw_events)
    → Go Ingestor → Go Validator → Go Dispatcher
        → Redis (alerts)
        → Snowflake (via Loader) → dbt Models
            → Metrics Bridge → Prometheus → Grafana
```

---

## Stack

| Layer | Technology |
|-------|-----------|
| Ingestion | Python (confluent-kafka), lz4 compression |
| Message Bus | Apache Kafka (Confluent 7.6.0), 3 topics |
| Processing | Go 1.24, segmentio/kafka-go |
| Alert Store | Redis (sorted set by severity) |
| Data Warehouse | Snowflake (VARIANT columns, MERGE upserts) |
| Transformations | dbt (4 models: staging, facts, metrics, alerts) |
| Observability | Prometheus + Grafana (11-panel dashboard) |
| Infrastructure | Docker Compose (7 containers) |

---

## Repository Structure

```
dataflow-observatory/
├── simulators/
│   ├── clickstream.py       # Web event simulator (~200 req/s)
│   ├── iot_sensor.py        # IoT sensor simulator (temp/humidity/battery)
│   └── requirements.txt
├── services/
│   ├── ingestor/            # Kafka consumer → dead-letter routing
│   ├── validator/           # Welford Z-score + null rate validation
│   ├── dispatcher/          # Alert routing to Redis
│   ├── internal/            # Shared types and Kafka helpers
│   ├── go.mod
│   └── go.sum
├── snowflake/
│   ├── setup.sql            # DB, schema, table DDL
│   ├── loader.py            # Kafka → Snowflake (4-thread daemon)
│   ├── metrics_bridge.py    # Snowflake + Redis → Prometheus metrics
│   └── run_setup.py
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   └── models/
│       ├── stg_events.sql       # Staging view (normalize + dedupe)
│       ├── fct_events.sql       # Incremental fact table + quality scores
│       ├── dq_metrics.sql       # Hourly quality aggregates per source
│       ├── alert_summary.sql    # Hourly alert counts by type + severity
│       ├── schema.yml
│       └── sources.yml
├── grafana/                 # Provisioned dashboard YAML
├── screenshots/             # Live screenshots from running system
├── docker-compose.yml
├── config.yaml
├── DataFlow_Observatory_Report.pptx
├── DataFlow_Observatory_Report.pdf
└── verify.py
```

---

## Components

### Python Simulators

**Clickstream** (`simulators/clickstream.py`):
- Generates realistic web events at ~200 req/s
- 15-field events: `event_id`, `user_id`, `session_id`, `url`, `referrer`, `ip`, `country`, `duration_ms`, `page_depth`, etc.
- 7 intentional defect types: `null_user_id`, `null_event_type`, `bad_timestamp`, `negative_duration`, `wrong_type_duration`, `extra_large_depth`, `missing_source`

**IoT Sensors** (`simulators/iot_sensor.py`):
- Simulates temperature (°C), humidity (%), battery level (%) readings
- Sinusoidal baseline drift + Gaussian noise per sensor
- 7 defect types including `out_of_range` values and bad timestamps

### Go Microservices (`services/`)

**Ingestor** (`dfo-ingestor` consumer group):
- Decodes raw JSON from `raw_events` Kafka topic
- Dead-letters malformed JSON to `dead_letter` topic
- Logs per-source RPS every 5 seconds

**Validator** (`dfo-validators` consumer group):
- Welford online algorithm for incremental Z-score anomaly detection
- Sliding-window null rate tracking per `source:field`
- Numeric ranges: temperature [-50, 80]°C, humidity [0, 100]%, battery [0, 100]%
- Publishes `QualityAlert` events to `quality_alerts` topic

**Dispatcher** (`dfo-dispatcher` consumer group):
- Reads `quality_alerts`, routes to Redis:
  - `ZADD dfo:alerts:by_severity` — sorted set, scored by severity
  - `SET dfo:alerts:latest:<source>` — latest alert per source
  - `INCR dfo:alert_count:<source>:<type>` — cumulative counters
  - `PUBLISH dfo:alerts:stream` — real-time pub/sub channel

### Kafka Topics

| Topic | Partitions | Messages | Size |
|-------|-----------|---------|------|
| `raw_events` | 6 | 994,298+ | 450 MB |
| `quality_alerts` | 3 | 20,070 | 7 MB |
| `dead_letter` | 3 | 0 | — |

### Snowflake

- **Database**: `DATAFLOW_OBSERVATORY`
- **RAW schema**: `EVENTS` (250 rows), `QUALITY_ALERTS` (114 rows)
- **ANALYTICS schema**: `FCT_EVENTS` (250), `DQ_METRICS` (3), `ALERT_SUMMARY` (24)
- Loader uses `MERGE` statements for idempotent upserts; `VARIANT` columns store JSON natively

### dbt Models

| Model | Type | Description |
|-------|------|-------------|
| `stg_events` | View | Normalize VARIANT payload, safe type casting with `TRY_TO_NUMBER(col::string)` |
| `fct_events` | Incremental | Null flags, range violations, composite quality score (0–1) |
| `dq_metrics` | Incremental | Hourly quality aggregates per source |
| `alert_summary` | Incremental | Hourly alert counts by source + type + severity |

### Redis Alert Store

- `dfo:alerts:by_severity` — sorted set, 10,000 entries, 4 MB, scored by severity
- `dfo:alerts:latest:<source>` — latest alert JSON per source
- `dfo:alert_count:<source>:<type>` — cumulative counters (11 keys)
- Total: 15 keys, ~5 MB memory usage

### Grafana Dashboard (11 panels)

**Row 1 — KPIs:**
- Quality Score (Clickstream): 100%
- Quality Score (IoT Sensor): 100%
- Events Last Hour: 60
- Redis Alert Queue: 10,000

**Row 2 — Time Series:**
- Quality Score Over Time
- Alert Rate by Severity

**Row 3 — Alert Analysis:**
- Alerts by Type: `range_violation` (33), `null_field` (25), `anomaly` (17), `type_error` (14), `null_rate` (5)
- Null Rates by Field

**Row 4 — Performance:**
- Dirty Event % Over Time
- Ingest Lag: clickstream 43.8s, iot_sensor 29.5s
- Alert Distribution by Source

---

## Screenshots

Live screenshots captured from the running system:

| Screenshot | Description |
|-----------|-------------|
| `screenshots/grafana_dashboard.png` | Full 11-panel Grafana dashboard |
| `screenshots/kafka_topics.png` | Kafka UI topics list (raw_events 994K msgs, 448 MB) |
| `screenshots/kafka_raw_events.png` | raw_events partition breakdown |
| `screenshots/kafka_consumers.png` | Consumer groups (dfo-ingestor, dfo-validators, dfo-dispatcher) |
| `screenshots/prometheus_targets.png` | Prometheus scrape targets — metrics_bridge UP |
| `screenshots/prometheus_graph.png` | dfo_quality_score over time |
| `screenshots/metrics_bridge.png` | Metrics bridge output |
| `screenshots/redis_insight.png` | Redis Insight showing 10,000-entry sorted set |
| `screenshots/snowflake_row_counts.png` | Snowflake row counts across all tables |
| `screenshots/snowflake_raw_events.png` | RAW.EVENTS table with UUID IDs and JSON payloads |

---

## Running Locally

### Prerequisites

- Docker + Docker Compose
- Go 1.24+
- Python 3.11+
- Snowflake account
- dbt Core

### Start Infrastructure

```bash
docker-compose up -d
```

This starts: Kafka + Zookeeper, Kafka UI (`:8080`), Redis (`:6379`), Redis Insight (`:5540`), Prometheus (`:9090`), Grafana (`:3000`).

### Run Simulators

```bash
cd simulators
pip install -r requirements.txt
python clickstream.py &
python iot_sensor.py &
```

### Run Go Services

```bash
cd services
go run ./ingestor &
go run ./validator &
go run ./dispatcher &
```

### Run Snowflake Loader + Metrics Bridge

```bash
cd snowflake
python loader.py &
python metrics_bridge.py &
```

### Run dbt Transformations

```bash
cd dbt
dbt run
```

---

## Key Technical Challenges

| Challenge | Solution |
|----------|---------|
| Kafka consumer groups committed offsets during failed runs | Deleted CGs with `kafka-consumer-groups --delete`, waited 12s for session timeout |
| `PARSE_JSON()` not valid in VALUES clause | Switched to SELECT-based MERGE statements |
| `TRY_TO_NUMBER` on VARIANT numbers fails | Cast VARIANT to `::string` first: `TRY_TO_NUMBER(col::string)` |
| `COUNTIF` unknown in Snowflake | Use `COUNT_IF` (Snowflake-specific syntax) |
| dbt `alert_summary` duplicate row error | Added `field_name` to `unique_key` list |
| Docker `credential-desktop` not found | Removed `credsStore` from `~/.docker/config.json` |
| Grafana panels empty on load | Missing time-range data — fixed by clicking panel to trigger refresh |

---

## Report

A full project report (15 slides) is included:

- `DataFlow_Observatory_Report.pptx`
- `DataFlow_Observatory_Report.pdf`

---

## Author

**Darshil Shukla** — April 2026
