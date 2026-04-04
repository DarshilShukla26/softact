"""
clickstream.py — Simulates web click events at ~200/sec to raw_events Kafka topic.

Each event is a JSON object with realistic fields. A small fraction (~2%) are
intentionally "dirty" (nulls, wrong types, out-of-range values) so the validator
has something to catch.

Usage:
    python clickstream.py [--config ../config.yaml] [--rps 200] [--dirty-rate 0.02]
"""

import argparse
import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import yaml
from confluent_kafka import Producer
from faker import Faker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("clickstream")
fake = Faker()

# ── constants ──────────────────────────────────────────────────────────────────

EVENT_TYPES   = ["pageview", "click", "scroll", "form_submit", "video_play"]
PAGES         = ["/", "/products", "/pricing", "/blog", "/about", "/checkout",
                 "/login", "/signup", "/search", "/cart"]
ELEMENTS      = ["btn-cta", "nav-link", "product-card", "search-bar",
                 "footer-link", "hero-image", "add-to-cart", "checkout-btn"]
COUNTRIES     = ["US", "GB", "DE", "FR", "CA", "AU", "IN", "BR", "JP", "MX"]
USER_AGENTS   = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1",
    "Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36",
]

# ── event generation ───────────────────────────────────────────────────────────

def _clean_event() -> dict[str, Any]:
    url = random.choice(PAGES)
    return {
        "event_id":    str(uuid.uuid4()),
        "source":      "clickstream",
        "event_type":  random.choice(EVENT_TYPES),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "user_id":     f"usr_{random.randint(1000, 99999)}",
        "session_id":  f"sess_{uuid.uuid4().hex[:12]}",
        "url":         url,
        "referrer":    random.choice(PAGES + [None]),
        "element_id":  random.choice(ELEMENTS) if random.random() > 0.2 else None,
        "ip_address":  fake.ipv4_public(),
        "user_agent":  random.choice(USER_AGENTS),
        "country":     random.choice(COUNTRIES),
        "duration_ms": random.randint(50, 8000),
        "page_depth":  round(random.uniform(0.0, 1.0), 2),
    }


def _dirty_event() -> dict[str, Any]:
    """Return a malformed event — random defect injected."""
    ev = _clean_event()
    defect = random.choice([
        "null_user_id",
        "null_event_type",
        "bad_timestamp",
        "negative_duration",
        "wrong_type_duration",
        "extra_large_depth",
        "missing_source",
    ])
    if defect == "null_user_id":
        ev["user_id"] = None
    elif defect == "null_event_type":
        ev["event_type"] = None
    elif defect == "bad_timestamp":
        ev["timestamp"] = "not-a-date"
    elif defect == "negative_duration":
        ev["duration_ms"] = -random.randint(1, 500)
    elif defect == "wrong_type_duration":
        ev["duration_ms"] = "fast"
    elif defect == "extra_large_depth":
        ev["page_depth"] = random.uniform(5.0, 100.0)
    elif defect == "missing_source":
        del ev["source"]
    ev["_defect"] = defect   # tag so we can verify validator catches it
    return ev


def make_event(dirty_rate: float) -> dict[str, Any]:
    return _dirty_event() if random.random() < dirty_rate else _clean_event()

# ── Kafka delivery callback ────────────────────────────────────────────────────

_delivered = 0
_errors    = 0

def _on_delivery(err, msg):
    global _delivered, _errors
    if err:
        _errors += 1
        log.warning("Delivery failed: %s", err)
    else:
        _delivered += 1

# ── main loop ─────────────────────────────────────────────────────────────────

def run(brokers: str, topic: str, target_rps: int, dirty_rate: float) -> None:
    producer = Producer({
        "bootstrap.servers":            brokers,
        "linger.ms":                    5,       # micro-batch for throughput
        "batch.num.messages":           500,
        "compression.type":             "lz4",
        "acks":                         "1",
        "queue.buffering.max.messages": 100_000,
    })

    log.info("Starting clickstream simulator → topic=%s  rps=%d  dirty=%.0f%%",
             topic, target_rps, dirty_rate * 100)

    interval  = 1.0 / target_rps
    last_stat = time.monotonic()
    sent      = 0

    def _shutdown(sig, frame):
        log.info("Shutting down — flushing producer...")
        producer.flush(timeout=10)
        log.info("Done. delivered=%d  errors=%d", _delivered, _errors)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        t0  = time.monotonic()
        ev  = make_event(dirty_rate)
        key = ev.get("user_id") or ev.get("session_id") or ""

        producer.produce(
            topic,
            key=key.encode(),
            value=json.dumps(ev).encode(),
            callback=_on_delivery,
        )
        producer.poll(0)   # trigger delivery callbacks without blocking
        sent += 1

        # print stats every 5 s
        now = time.monotonic()
        if now - last_stat >= 5:
            log.info("sent=%d  delivered=%d  errors=%d  rps≈%.0f",
                     sent, _delivered, _errors, sent / (now - last_stat + 1e-9))
            sent      = 0
            last_stat = now

        # rate-limit
        elapsed = time.monotonic() - t0
        sleep   = interval - elapsed
        if sleep > 0:
            time.sleep(sleep)

# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Clickstream event simulator")
    parser.add_argument("--config",     default="../config.yaml")
    parser.add_argument("--rps",        type=int,   default=None)
    parser.add_argument("--dirty-rate", type=float, default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    brokers    = ",".join(cfg["kafka"]["brokers"])
    topic      = cfg["kafka"]["topics"]["raw_events"]
    target_rps = args.rps        or cfg["simulators"]["clickstream"]["target_rps"]
    dirty_rate = args.dirty_rate or 0.02

    run(brokers, topic, target_rps, dirty_rate)


if __name__ == "__main__":
    main()
