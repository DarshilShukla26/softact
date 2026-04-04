"""
iot_sensor.py — Simulates IoT sensor readings (temperature + humidity) to raw_events.

Each sensor fires every `interval_ms` milliseconds. A rolling baseline drifts
slowly over time so readings look realistic. ~2% of events are dirty.

Usage:
    python iot_sensor.py [--config ../config.yaml] [--interval-ms 500] [--dirty-rate 0.02]
"""

import argparse
import json
import logging
import math
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import yaml
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("iot_sensor")

# ── sensor state (per sensor id) ──────────────────────────────────────────────

class SensorState:
    """Tracks rolling baseline so readings drift naturally."""

    def __init__(self, sensor_id: str, location: str) -> None:
        self.sensor_id  = sensor_id
        self.location   = location
        # baseline varies by location type
        if "server" in location:
            self._base_temp = random.uniform(18.0, 22.0)   # cool server room
            self._base_hum  = random.uniform(30.0, 45.0)
        else:
            self._base_temp = random.uniform(20.0, 26.0)   # warehouse
            self._base_hum  = random.uniform(40.0, 60.0)
        self._battery   = random.uniform(70.0, 100.0)
        self._tick      = 0

    def next_reading(self) -> tuple[float, float, float]:
        """Return (temperature_c, humidity_pct, battery_pct)."""
        self._tick += 1
        # slow sinusoidal drift + small gaussian noise
        drift_temp = 2.0 * math.sin(self._tick / 200)
        drift_hum  = 3.0 * math.cos(self._tick / 180)
        temp    = self._base_temp + drift_temp + random.gauss(0, 0.3)
        hum     = self._base_hum  + drift_hum  + random.gauss(0, 0.5)
        hum     = max(0.0, min(100.0, hum))
        # battery drains slowly
        self._battery = max(0.0, self._battery - random.uniform(0.0, 0.01))
        return round(temp, 2), round(hum, 2), round(self._battery, 1)

    def status(self, temp: float, hum: float, battery: float) -> str:
        if battery < 15 or temp > 35 or hum > 85:
            return "critical"
        if battery < 30 or temp > 30 or hum > 70:
            return "warning"
        return "ok"


# ── event generation ───────────────────────────────────────────────────────────

def _clean_event(state: SensorState) -> dict[str, Any]:
    temp, hum, bat = state.next_reading()
    return {
        "event_id":    str(uuid.uuid4()),
        "source":      "iot_sensor",
        "sensor_id":   state.sensor_id,
        "location":    state.location,
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "temperature": temp,
        "humidity":    hum,
        "battery_level": bat,
        "status":      state.status(temp, hum, bat),
        "firmware":    "v2.1.4",
    }


def _dirty_event(state: SensorState) -> dict[str, Any]:
    ev = _clean_event(state)
    defect = random.choice([
        "null_temperature",
        "null_sensor_id",
        "bad_timestamp",
        "out_of_range_humidity",    # > 100%
        "out_of_range_temperature", # physically impossible
        "wrong_type_battery",
        "missing_source",
    ])
    if defect == "null_temperature":
        ev["temperature"] = None
    elif defect == "null_sensor_id":
        ev["sensor_id"] = None
    elif defect == "bad_timestamp":
        ev["timestamp"] = 99999999  # epoch int instead of ISO string
    elif defect == "out_of_range_humidity":
        ev["humidity"] = random.uniform(101.0, 200.0)
    elif defect == "out_of_range_temperature":
        ev["temperature"] = random.choice([-300.0, 999.9])
    elif defect == "wrong_type_battery":
        ev["battery_level"] = "full"
    elif defect == "missing_source":
        del ev["source"]
    ev["_defect"] = defect
    return ev


def make_event(state: SensorState, dirty_rate: float) -> dict[str, Any]:
    return _dirty_event(state) if random.random() < dirty_rate else _clean_event(state)

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

def run(brokers: str, topic: str, sensors: list[dict],
        interval_ms: int, dirty_rate: float) -> None:

    producer = Producer({
        "bootstrap.servers":            brokers,
        "linger.ms":                    2,
        "compression.type":             "lz4",
        "acks":                         "1",
        "queue.buffering.max.messages": 10_000,
    })

    states = [SensorState(s["id"], s["location"]) for s in sensors]

    log.info("Starting IoT simulator → topic=%s  sensors=%d  interval=%dms  dirty=%.0f%%",
             topic, len(states), interval_ms, dirty_rate * 100)

    interval_sec = interval_ms / 1000.0
    last_stat    = time.monotonic()
    sent         = 0

    def _shutdown(sig, frame):
        log.info("Shutting down — flushing producer...")
        producer.flush(timeout=10)
        log.info("Done. delivered=%d  errors=%d", _delivered, _errors)
        sys.exit(0)

    signal.signal(signal.SIGINT,  _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    while True:
        t0 = time.monotonic()

        for state in states:
            ev = make_event(state, dirty_rate)
            producer.produce(
                topic,
                key=state.sensor_id.encode(),
                value=json.dumps(ev).encode(),
                callback=_on_delivery,
            )
            sent += 1

        producer.poll(0)

        now = time.monotonic()
        if now - last_stat >= 10:
            log.info("sent=%d  delivered=%d  errors=%d",
                     sent, _delivered, _errors)
            sent      = 0
            last_stat = now

        elapsed = time.monotonic() - t0
        sleep   = interval_sec - elapsed
        if sleep > 0:
            time.sleep(sleep)

# ── entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="IoT sensor simulator")
    parser.add_argument("--config",      default="../config.yaml")
    parser.add_argument("--interval-ms", type=int,   default=None)
    parser.add_argument("--dirty-rate",  type=float, default=None)
    args = parser.parse_args()

    with open(args.config) as f:
        cfg = yaml.safe_load(f)

    brokers     = ",".join(cfg["kafka"]["brokers"])
    topic       = cfg["kafka"]["topics"]["raw_events"]
    sim_cfg     = cfg["simulators"]["iot_sensor"]
    interval_ms = args.interval_ms or sim_cfg["interval_ms"]
    dirty_rate  = args.dirty_rate  or 0.02
    sensors     = sim_cfg["sensors"]

    run(brokers, topic, sensors, interval_ms, dirty_rate)


if __name__ == "__main__":
    main()
