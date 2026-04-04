"""
metrics_bridge.py — Prometheus exporter for DataFlow Observatory.

Queries Snowflake (dq_metrics, alert_summary) every SNOWFLAKE_INTERVAL seconds
and Redis every REDIS_INTERVAL seconds, then exposes metrics on :8000/metrics.

Grafana → Prometheus → metrics_bridge → Snowflake + Redis

Usage:
    source .env
    pip install prometheus-client snowflake-connector-python redis
    python3 snowflake/metrics_bridge.py [--port 8000]
"""

import argparse
import logging
import os
import sys
import threading
import time
import warnings

warnings.filterwarnings("ignore")

import redis as redis_lib
import snowflake.connector
import yaml
from prometheus_client import Gauge, start_http_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("metrics_bridge")

# ── Prometheus metrics ─────────────────────────────────────────────────────────

quality_score = Gauge(
    "dfo_quality_score",
    "Data quality score (0=worst, 1=best)",
    ["source"],
)
total_events = Gauge(
    "dfo_total_events",
    "Total events in last completed hour",
    ["source"],
)
dirty_events = Gauge(
    "dfo_dirty_events",
    "Dirty events in last completed hour",
    ["source"],
)
ingest_lag = Gauge(
    "dfo_ingest_lag_seconds",
    "Average ingest lag in seconds",
    ["source"],
)
null_rate = Gauge(
    "dfo_null_rate",
    "Null rate for a field (0-1)",
    ["source", "field"],
)
alert_count = Gauge(
    "dfo_alert_count",
    "Alert count in last completed hour",
    ["source", "alert_type", "severity"],
)
redis_queue_size = Gauge(
    "dfo_redis_alert_queue_size",
    "Number of alerts in the Redis sorted set",
)
redis_alert_counter = Gauge(
    "dfo_redis_alert_type_count",
    "Cumulative alert count by source and type",
    ["source", "alert_type"],
)

# ── Snowflake collector ────────────────────────────────────────────────────────

SNOWFLAKE_INTERVAL = 60   # seconds between Snowflake queries

DQ_METRICS_SQL = """
    SELECT
        source,
        quality_score,
        total_events,
        dirty_events,
        avg_ingest_lag_seconds,
        null_rate_user_id,
        null_rate_event_type,
        null_rate_url,
        null_rate_sensor_id,
        null_rate_temperature,
        null_rate_humidity,
        null_rate_timestamp
    FROM DATAFLOW_OBSERVATORY.ANALYTICS_ANALYTICS.DQ_METRICS
    WHERE event_hour = (
        SELECT MAX(event_hour) FROM DATAFLOW_OBSERVATORY.ANALYTICS_ANALYTICS.DQ_METRICS
    )
"""

ALERT_SUMMARY_SQL = """
    SELECT
        source,
        alert_type,
        severity,
        SUM(alert_count) AS cnt
    FROM DATAFLOW_OBSERVATORY.ANALYTICS_ANALYTICS.ALERT_SUMMARY
    WHERE alert_hour >= DATEADD('hour', -1, (
        SELECT MAX(alert_hour) FROM DATAFLOW_OBSERVATORY.ANALYTICS_ANALYTICS.ALERT_SUMMARY
    ))
    GROUP BY 1, 2, 3
"""

NULL_FIELD_MAP = {
    "null_rate_user_id":    ("clickstream", "user_id"),
    "null_rate_event_type": ("clickstream", "event_type"),
    "null_rate_url":        ("clickstream", "url"),
    "null_rate_sensor_id":  ("iot_sensor",  "sensor_id"),
    "null_rate_temperature":("iot_sensor",  "temperature"),
    "null_rate_humidity":   ("iot_sensor",  "humidity"),
    "null_rate_timestamp":  ("all",         "timestamp"),
}


def collect_snowflake(conn_params: dict) -> None:
    while True:
        try:
            conn = snowflake.connector.connect(**conn_params)
            cur  = conn.cursor()

            # DQ metrics
            cur.execute(DQ_METRICS_SQL)
            cols = [d[0].lower() for d in cur.description]
            for row in cur.fetchall():
                r = dict(zip(cols, row))
                src = r["source"] or "unknown"
                if r["quality_score"] is not None:
                    quality_score.labels(source=src).set(float(r["quality_score"]))
                if r["total_events"] is not None:
                    total_events.labels(source=src).set(float(r["total_events"]))
                if r["dirty_events"] is not None:
                    dirty_events.labels(source=src).set(float(r["dirty_events"]))
                if r["avg_ingest_lag_seconds"] is not None:
                    ingest_lag.labels(source=src).set(float(r["avg_ingest_lag_seconds"]))
                # null rates
                for col, (expected_src, field) in NULL_FIELD_MAP.items():
                    val = r.get(col)
                    if val is not None:
                        lbl_src = src if expected_src == "all" else expected_src
                        null_rate.labels(source=lbl_src, field=field).set(float(val))

            # Alert summary
            cur.execute(ALERT_SUMMARY_SQL)
            for (src, atype, sev, cnt) in cur.fetchall():
                if cnt is not None:
                    alert_count.labels(
                        source=src or "unknown",
                        alert_type=atype or "unknown",
                        severity=sev or "unknown",
                    ).set(float(cnt))

            cur.close()
            conn.close()
            log.info("Snowflake metrics refreshed")

        except Exception as e:
            log.error("Snowflake collect error: %s", e)

        time.sleep(SNOWFLAKE_INTERVAL)


# ── Redis collector ────────────────────────────────────────────────────────────

REDIS_INTERVAL = 10


def collect_redis(addr: str) -> None:
    rdb = redis_lib.Redis.from_url(f"redis://{addr}", decode_responses=True)
    while True:
        try:
            # Sorted set size = total stored alerts (capped at 10k)
            size = rdb.zcard("dfo:alerts:by_severity")
            redis_queue_size.set(size)

            # Per-source/type counters
            for key in rdb.scan_iter("dfo:alert_count:*"):
                # key format: dfo:alert_count:<source>:<alert_type>
                parts = key.split(":")
                if len(parts) >= 4:
                    src   = parts[2]
                    atype = ":".join(parts[3:])
                    val   = rdb.get(key)
                    if val:
                        redis_alert_counter.labels(
                            source=src, alert_type=atype
                        ).set(float(val))

        except Exception as e:
            log.error("Redis collect error: %s", e)

        time.sleep(REDIS_INTERVAL)


# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="DFO Prometheus metrics bridge")
    parser.add_argument("--port",     type=int, default=8000)
    parser.add_argument("--config",   default="config.yaml")
    args = parser.parse_args()

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing  = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"ERROR: missing env vars {missing} — run: source .env")
        sys.exit(1)

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    conn_params = dict(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        role      = "ACCOUNTADMIN",
        warehouse = "DFO_WH",
        database  = "DATAFLOW_OBSERVATORY",
    )
    redis_addr = cfg["redis"]["addr"]

    start_http_server(args.port)
    log.info("Metrics bridge listening on :%d/metrics", args.port)

    threads = [
        threading.Thread(target=collect_snowflake, args=(conn_params,), daemon=True),
        threading.Thread(target=collect_redis,     args=(redis_addr,),  daemon=True),
    ]
    for t in threads:
        t.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopped.")


if __name__ == "__main__":
    main()
