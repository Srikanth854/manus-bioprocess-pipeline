"""
realtime_simulator.py
=====================
Simulates a live DCS historian stream from Manus fermentation tanks.

Generates realistic sensor readings every few seconds and publishes
them to Kafka — exactly how a real DCS historian would behave.

In production at Manus this script is replaced by a real
Kafka connector talking directly to OSIsoft PI or similar historian.
"""

import json
import time
import uuid
import random
import logging
from datetime import datetime, timezone
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import KAFKA_CONFIG, KAFKA_TOPIC

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SIMULATOR] %(message)s")
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# TANK CONFIGURATIONS
# Simulates multiple fermentation tanks running simultaneously
# ─────────────────────────────────────────────
TANKS = [
    {
        "tank_id": "TANK-01",
        "batch_id": "BATCH-2026-001",
        "product_name": "fatty acid",
        "base_temp": 34.0,
        "base_ph": 6.8,
        "base_o2": 45.0,
        "base_pressure": 1.2,
        "base_agitation": 250,
        "base_feed_rate": 12.0,
    },
    {
        "tank_id": "TANK-02",
        "batch_id": "BATCH-2026-002",
        "product_name": "isoprene",
        "base_temp": 37.0,
        "base_ph": 7.0,
        "base_o2": 42.0,
        "base_pressure": 1.1,
        "base_agitation": 200,
        "base_feed_rate": 10.0,
    },
    {
        "tank_id": "TANK-03",
        "batch_id": "BATCH-2026-003",
        "product_name": "farnesene",
        "base_temp": 30.0,
        "base_ph": 6.5,
        "base_o2": 48.0,
        "base_pressure": 1.3,
        "base_agitation": 300,
        "base_feed_rate": 15.0,
    },
]


def generate_reading(tank: dict, add_anomaly: bool = False) -> dict:
    """
    Generate a realistic sensor reading for a tank.
    Adds small random variation to simulate real sensor behavior.
    Occasionally adds anomalies to demonstrate outlier detection.
    """
    # Normal variation — sensors fluctuate slightly
    temp = tank["base_temp"] + random.uniform(-0.5, 0.5)
    ph = tank["base_ph"] + random.uniform(-0.1, 0.1)
    o2 = tank["base_o2"] + random.uniform(-2.0, 2.0)
    pressure = tank["base_pressure"] + random.uniform(-0.05, 0.05)
    agit = tank["base_agitation"] + random.uniform(-5, 5)
    feed = tank["base_feed_rate"] + random.uniform(-0.5, 0.5)

    # Simulate anomaly — e.g. temperature spike
    if add_anomaly:
        temp = temp + random.uniform(8, 12)
        log.warning(f"ANOMALY injected for {tank['tank_id']} — temp spike: {temp:.1f}C")

    return {
        "message_id": str(uuid.uuid4()),
        "batch_id": tank["batch_id"],
        "tank_id": tank["tank_id"],
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "temperature_c": round(temp, 2),
        "pressure_bar": round(pressure, 3),
        "ph_level": round(ph, 2),
        "dissolved_o2": round(o2, 2),
        "agitation_rpm": round(agit, 1),
        "feed_rate_lph": round(feed, 2),
        "product_name": tank["product_name"],
        "_source_system": "dcs_historian_sim",
        "_pipeline_version": "1.0.0",
    }


def run_simulator(
    interval_seconds: float = 2.0,
    anomaly_probability: float = 0.05,
    max_readings: int = None,
):
    """
    Main simulator loop.
    Generates readings for all tanks every interval_seconds.

    Args:
        interval_seconds:    How often to generate readings
        anomaly_probability: Chance of injecting an anomaly (0-1)
        max_readings:        Stop after this many readings (None = run forever)
    """
    try:
        from confluent_kafka import Producer

        producer = Producer(KAFKA_CONFIG)
        log.info(f"Connected to Kafka. Simulating {len(TANKS)} tanks...")
        log.info(f"Publishing to topic: {KAFKA_TOPIC}")
        log.info(
            f"Interval: {interval_seconds}s | Anomaly rate: {anomaly_probability*100:.0f}%"
        )
        log.info("Press Ctrl+C to stop\n")

    except ImportError:
        log.error("confluent-kafka not installed")
        return

    reading_count = 0

    try:
        while True:
            for tank in TANKS:
                # Randomly inject anomaly
                add_anomaly = random.random() < anomaly_probability
                reading = generate_reading(tank, add_anomaly=add_anomaly)

                # Publish to Kafka
                producer.produce(
                    topic=KAFKA_TOPIC,
                    key=tank["tank_id"],
                    value=json.dumps(reading),
                )
                producer.poll(0)

                reading_count += 1

                log.info(
                    f"{tank['tank_id']} | "
                    f"Temp: {reading['temperature_c']}C | "
                    f"pH: {reading['ph_level']} | "
                    f"O2: {reading['dissolved_o2']}% | "
                    f"{'ANOMALY' if add_anomaly else 'normal'}"
                )

            producer.flush()

            if max_readings and reading_count >= max_readings:
                log.info(f"Reached max readings: {max_readings}")
                break

            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        log.info(f"\nSimulator stopped. Total readings published: {reading_count}")
    finally:
        producer.flush()


if __name__ == "__main__":
    run_simulator(interval_seconds=2.0, anomaly_probability=0.05)
