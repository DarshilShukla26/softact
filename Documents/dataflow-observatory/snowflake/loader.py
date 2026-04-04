"""
loader.py — Micro-batch Kafka → Snowflake loader.

Reads from raw_events and quality_alerts topics and bulk-inserts into
Snowflake every FLUSH_INTERVAL_SEC seconds.  Runs two threads, one per topic.

This is intentionally simple (no Kafka Connect, no Snowpipe) — just Python
so you can see exactly what's happening.

Usage:
    source .env
    python3 snowflake/loader.py [--config config.yaml] [--interval 10]
"""

import argparse
import json
import logging
import os
import queue
import sys
import threading
import time
from datetime import datetime, timezone

import snowflake.connector
import yaml
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  [%(name)s]  %(message)s",
    datefmt="%H:%M:%S",
)

# ── shared queues ──────────────────────────────────────────────────────────────
events_q  = queue.Queue()
alerts_q  = queue.Queue()


# ── Kafka consumer thread ─────────────────────────────────────────────────────

def kafka_consumer(brokers: str, topic: str, group_id: str, dest_q: queue.Queue):
    log = logging.getLogger(f"consumer.{topic}")
    c = Consumer({
        "bootstrap.servers":  brokers,
        "group.id":           group_id,
        "auto.offset.reset":  "earliest",
        "enable.auto.commit": True,
    })
    c.subscribe([topic])
    log.info("subscribed → %s", topic)
    while True:
        msg = c.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                log.error("Kafka error: %s", msg.error())
            continue
        dest_q.put(msg.value())   # raw bytes


# ── Snowflake writer thread ────────────────────────────────────────────────────

def snowflake_writer(
    dest_q:        queue.Queue,
    table:         str,            # e.g. "RAW.EVENTS"
    id_field:      str,            # primary key field name in JSON
    extra_fields:  list[str],      # top-level columns to extract from JSON
    flush_interval: int,
    conn_params:   dict,
):
    log   = logging.getLogger(f"writer.{table}")
    conn  = snowflake.connector.connect(**conn_params)
    cur   = conn.cursor()
    cur.execute("USE DATABASE DATAFLOW_OBSERVATORY")
    cur.execute("USE SCHEMA RAW")

    # PARSE_JSON() is only valid in a SELECT clause, not VALUES.
    non_payload_cols = [id_field.upper()] + [f.upper() for f in extra_fields]
    n_static = len(non_payload_cols)
    static_placeholders = ", ".join(["%s"] * n_static)
    # MERGE ignores duplicates (idempotent re-runs when loader restarts from earliest)
    merge_cols = non_payload_cols + ["LOADED_AT", "PAYLOAD"]
    merge_vals = f"{static_placeholders}, CURRENT_TIMESTAMP(), PARSE_JSON(%s)"
    insert_sql = (
        f"MERGE INTO {table} AS tgt "
        f"USING (SELECT {merge_vals}) AS src ({', '.join(merge_cols)}) "
        f"ON tgt.{id_field.upper()} = src.{id_field.upper()} "
        f"WHEN NOT MATCHED THEN INSERT ({', '.join(merge_cols)}) "
        f"VALUES ({', '.join(['src.' + c for c in merge_cols])})"
    )

    log.info("writer ready → %s (flush every %ds)", table, flush_interval)
    total_inserted = 0

    while True:
        time.sleep(flush_interval)

        # Drain queue
        batch = []
        while True:
            try:
                raw = dest_q.get_nowait()
                batch.append(raw)
            except queue.Empty:
                break

        if not batch:
            continue

        rows = []
        skipped = 0
        for raw in batch:
            try:
                ev = json.loads(raw)
                id_val = ev.get(id_field)
                if not id_val:          # skip events with no primary key
                    skipped += 1
                    continue
                # skip if any non-nullable extracted column is missing
                if any(ev.get(f) is None for f in extra_fields):
                    skipped += 1
                    continue
                row = [id_val]
                for f in extra_fields:
                    row.append(ev.get(f))
                row.append(raw.decode() if isinstance(raw, bytes) else raw)
                rows.append(tuple(row))
            except Exception:
                skipped += 1

        try:
            # executemany doesn't support PARSE_JSON in SELECT; use a loop instead
            for row in rows:
                cur.execute(insert_sql, row)
            conn.commit()
            total_inserted += len(rows)
            log.info("inserted %d rows into %s  (total=%d, skipped=%d)",
                     len(rows), table, total_inserted, skipped)
        except Exception as e:
            log.error("insert failed: %s", e)
            try:
                conn = snowflake.connector.connect(**conn_params)
                cur  = conn.cursor()
                cur.execute("USE DATABASE DATAFLOW_OBSERVATORY")
                cur.execute("USE SCHEMA RAW")
            except Exception:
                pass


# ── main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Kafka → Snowflake micro-batch loader")
    parser.add_argument("--config",   default="config.yaml")
    parser.add_argument("--interval", type=int, default=10,
                        help="Flush to Snowflake every N seconds (default 10)")
    args = parser.parse_args()

    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    missing  = [v for v in required if not os.getenv(v)]
    if missing:
        print(f"ERROR: missing env vars {missing} — run: source .env")
        sys.exit(1)

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    brokers = ",".join(cfg["kafka"]["brokers"])
    conn_params = dict(
        account  = os.environ["SNOWFLAKE_ACCOUNT"],
        user     = os.environ["SNOWFLAKE_USER"],
        password = os.environ["SNOWFLAKE_PASSWORD"],
    )

    threads = [
        # Kafka consumer: raw_events
        threading.Thread(
            target=kafka_consumer,
            args=(brokers, "raw_events", "dfo-sf-loader-events", events_q),
            daemon=True, name="kafka-events",
        ),
        # Kafka consumer: quality_alerts
        threading.Thread(
            target=kafka_consumer,
            args=(brokers, "quality_alerts", "dfo-sf-loader-alerts", alerts_q),
            daemon=True, name="kafka-alerts",
        ),
        # Snowflake writer: RAW.EVENTS
        threading.Thread(
            target=snowflake_writer,
            args=(events_q, "RAW.EVENTS", "event_id", ["source"],
                  args.interval, conn_params),
            daemon=True, name="sf-events",
        ),
        # Snowflake writer: RAW.QUALITY_ALERTS
        threading.Thread(
            target=snowflake_writer,
            args=(alerts_q, "RAW.QUALITY_ALERTS", "alert_id", ["source", "severity"],
                  args.interval, conn_params),
            daemon=True, name="sf-alerts",
        ),
    ]

    for t in threads:
        t.start()

    print(f"Loader running — flushing every {args.interval}s. Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped.")


if __name__ == "__main__":
    main()
