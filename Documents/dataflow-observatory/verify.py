#!/usr/bin/env python3
"""
verify.py — End-to-end health check for DataFlow Observatory.
Run this while the simulators and Go services are running.

Usage: ~/anaconda3/bin/python3 verify.py
"""

import json
import sys
import time

PASS = "\033[32m✓\033[0m"
FAIL = "\033[31m✗\033[0m"
WARN = "\033[33m~\033[0m"

def check(label, ok, detail=""):
    icon = PASS if ok else FAIL
    print(f"  {icon}  {label}" + (f"  →  {detail}" if detail else ""))
    return ok

def section(title):
    print(f"\n{'─'*50}")
    print(f"  {title}")
    print(f"{'─'*50}")

results = []

# ── Kafka ──────────────────────────────────────────────────────────────────────
section("Kafka")
try:
    from confluent_kafka.admin import AdminClient
    from confluent_kafka import Consumer, TopicPartition

    admin = AdminClient({"bootstrap.servers": "localhost:9092"})
    meta  = admin.list_topics(timeout=5)

    expected_topics = ["raw_events", "quality_alerts", "dead_letter"]
    for t in expected_topics:
        exists = t in meta.topics
        results.append(check(f"topic '{t}' exists", exists))

    # count messages per topic
    c = Consumer({"bootstrap.servers": "localhost:9092", "group.id": "verify-tmp"})
    for topic in expected_topics:
        partitions = [TopicPartition(topic, p) for p in range(len(meta.topics[topic].partitions))]
        lo_hi = c.get_watermark_offsets
        total = 0
        for tp in partitions:
            low, high = c.get_watermark_offsets(tp, timeout=3)
            total += max(0, high - low)
        # dead_letter being empty is fine — means no malformed JSON, which is good
        if topic == "dead_letter":
            print(f"  {WARN}  'dead_letter' message count  →  {total:,} messages  (0 = good, means no malformed JSON)")
            results.append(True)
        else:
            results.append(check(f"'{topic}' message count", total > 0, f"{total:,} messages"))
    c.close()

except Exception as e:
    results.append(check("Kafka reachable", False, str(e)))

# ── Redis ──────────────────────────────────────────────────────────────────────
section("Redis")
try:
    import redis as redis_lib
    r = redis_lib.Redis(host="localhost", port=6379, decode_responses=True)
    r.ping()
    results.append(check("Redis ping", True))

    alert_count = r.zcard("dfo:alerts:by_severity")
    results.append(check("Alerts in sorted set", alert_count > 0, f"{alert_count} alerts"))

    sources = ["clickstream", "iot_sensor"]
    for src in sources:
        val = r.get(f"dfo:alerts:latest:{src}")
        if val:
            alert = json.loads(val)
            results.append(check(f"Latest alert for '{src}'", True,
                                 f"type={alert.get('alert_type')} severity={alert.get('severity')}"))
        else:
            results.append(check(f"Latest alert for '{src}'", False, "no key found (services running?)"))

    count_keys = r.keys("dfo:alert_count:*")
    results.append(check("Alert type counters exist", len(count_keys) > 0,
                         f"{len(count_keys)} counter keys"))

except Exception as e:
    results.append(check("Redis reachable", False, str(e)))

# ── Snowflake (dbt connection) ─────────────────────────────────────────────────
section("Snowflake / dbt")
try:
    import os, subprocess
    env = dict(os.environ)
    dbt_bin = os.path.expanduser("~/anaconda3/bin/dbt")
    r2 = subprocess.run(
        [dbt_bin, "debug", "--profiles-dir", "dbt/", "--project-dir", "dbt/"],
        capture_output=True, text=True, env=env, cwd=os.path.expanduser("~/dataflow-observatory")
    )
    ok = "All checks passed" in r2.stdout or "connection ok" in r2.stdout
    results.append(check("dbt → Snowflake connection", ok,
                         "All checks passed" if ok else r2.stdout[-200:]))
except Exception as e:
    results.append(check("dbt debug", False, str(e)))

# ── Summary ───────────────────────────────────────────────────────────────────
section("Summary")
passed = sum(results)
total  = len(results)
print(f"\n  {passed}/{total} checks passed\n")
if passed == total:
    print("  \033[32mAll systems go.\033[0m\n")
else:
    print("  \033[33mSome checks failed — see above.\033[0m\n")
    sys.exit(1)
