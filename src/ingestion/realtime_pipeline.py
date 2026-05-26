"""
realtime_pipeline.py
====================
Real-time pipeline that consumes Kafka messages and processes them
through Bronze -> Silver -> Gold layers into SQLite.

This is the consumer side of the real-time demo.
It runs continuously, processing each message as it arrives.

This is what production looks like at Manus:
  DCS Historian -> Kafka -> This pipeline -> SQLite/Data Lake
"""

import json
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import KAFKA_CONFIG, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP, PIPELINE_VERSION
from src.database.db_manager import (
    initialize_database,
    insert_bronze,
    insert_silver,
    upsert_gold,
    log_pipeline_run,
    get_layer_counts,
    get_connection,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PIPELINE] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# BRONZE PROCESSOR
# ─────────────────────────────────────────────


def process_bronze(message: dict, run_id: str) -> dict:
    """
    Bronze layer processing.
    Lands raw message as-is with metadata stamped.
    No transformation — pure landing.
    """
    now = datetime.now(timezone.utc).isoformat()

    bronze_record = {
        "batch_id": message.get("batch_id"),
        "tank_id": message.get("tank_id"),
        "timestamp": message.get("timestamp"),
        "temperature_c": message.get("temperature_c"),
        "pressure_bar": message.get("pressure_bar"),
        "ph_level": message.get("ph_level"),
        "dissolved_o2": message.get("dissolved_o2"),
        "agitation_rpm": message.get("agitation_rpm"),
        "feed_rate_lph": message.get("feed_rate_lph"),
        "product_name": message.get("product_name"),
        "_source_system": message.get("_source_system", "dcs_historian"),
        "_ingestion_ts": now,
        "_pipeline_version": PIPELINE_VERSION,
        "_bronze_run_id": run_id,
    }

    row_id = insert_bronze(bronze_record)
    return bronze_record


# ─────────────────────────────────────────────
# SILVER PROCESSOR
# ─────────────────────────────────────────────


def process_silver(bronze_record: dict) -> dict:
    """
    Silver layer processing.
    Validates, cleans, and enriches the Bronze record.

    Checks:
    - Temperature valid range (20-45°C for fermentation)
    - pH valid range (5.5-8.5)
    - Dissolved O2 valid range (20-80%)
    - Outlier detection on temperature
    """
    now = datetime.now(timezone.utc).isoformat()

    temp = bronze_record.get("temperature_c") or 0
    ph = bronze_record.get("ph_level") or 0
    o2 = bronze_record.get("dissolved_o2") or 0
    feed = bronze_record.get("feed_rate_lph") or 0

    # Validation checks
    temp_valid = 1 if 20 <= temp <= 45 else 0
    ph_valid = 1 if 5.5 <= ph <= 8.5 else 0
    o2_valid = 1 if 20 <= o2 <= 80 else 0

    # Outlier detection — temperature spike above 43°C is anomalous
    is_outlier = 1 if temp > 43 or temp < 25 else 0

    # Derived feature — productivity score
    # Higher O2 + optimal temp + stable pH = better productivity
    temp_score = max(0, 1 - abs(temp - 35) / 10)
    ph_score = max(0, 1 - abs(ph - 7.0) / 1.5)
    o2_score = max(0, o2 / 80)
    productivity_score = round((temp_score + ph_score + o2_score) / 3, 3)

    silver_record = {
        "batch_id": bronze_record["batch_id"],
        "tank_id": bronze_record["tank_id"],
        "timestamp": bronze_record["timestamp"],
        "temperature_c": round(temp, 2),
        "pressure_bar": bronze_record.get("pressure_bar"),
        "ph_level": round(ph, 2),
        "dissolved_o2": round(o2, 2),
        "agitation_rpm": bronze_record.get("agitation_rpm"),
        "feed_rate_lph": round(feed, 2),
        "product_name": bronze_record.get("product_name"),
        "temp_valid": temp_valid,
        "ph_valid": ph_valid,
        "o2_valid": o2_valid,
        "is_outlier": is_outlier,
        "productivity_score": productivity_score,
        "_silver_ts": now,
    }

    insert_silver(silver_record)

    if is_outlier:
        log.warning(
            f"OUTLIER detected: {silver_record['tank_id']} "
            f"temp={temp}C — flagged in Silver"
        )

    return silver_record


# ─────────────────────────────────────────────
# GOLD PROCESSOR
# ─────────────────────────────────────────────


def process_gold(batch_id: str, tank_id: str):
    """
    Gold layer processing.
    Aggregates all Silver records for this batch+tank
    and upserts the Gold summary table.

    This gives a live updating analytics view per batch.
    """
    now = datetime.now(timezone.utc).isoformat()

    conn = get_connection()
    try:
        # Aggregate all Silver records for this batch+tank
        agg = conn.execute(
            """
            SELECT
                product_name,
                AVG(temperature_c)  as avg_temperature,
                AVG(pressure_bar)   as avg_pressure,
                AVG(ph_level)       as avg_ph,
                AVG(dissolved_o2)   as avg_dissolved_o2,
                AVG(agitation_rpm)  as avg_agitation,
                AVG(feed_rate_lph)  as avg_feed_rate,
                COUNT(*)            as total_readings,
                SUM(is_outlier)     as outlier_count,
                MIN(timestamp)      as first_reading_ts,
                MAX(timestamp)      as last_reading_ts
            FROM silver_fermentation
            WHERE batch_id = ? AND tank_id = ?
        """,
            (batch_id, tank_id),
        ).fetchone()

    finally:
        conn.close()

    if not agg:
        return

    total = agg["total_readings"] or 1
    outliers = agg["outlier_count"] or 0
    quality = round((1 - outliers / total) * 100, 1)

    gold_record = {
        "batch_id": batch_id,
        "tank_id": tank_id,
        "product_name": agg["product_name"],
        "avg_temperature": round(agg["avg_temperature"] or 0, 2),
        "avg_pressure": round(agg["avg_pressure"] or 0, 3),
        "avg_ph": round(agg["avg_ph"] or 0, 2),
        "avg_dissolved_o2": round(agg["avg_dissolved_o2"] or 0, 2),
        "avg_agitation": round(agg["avg_agitation"] or 0, 1),
        "avg_feed_rate": round(agg["avg_feed_rate"] or 0, 2),
        "total_readings": total,
        "outlier_count": outliers,
        "data_quality_score": quality,
        "first_reading_ts": agg["first_reading_ts"],
        "last_reading_ts": agg["last_reading_ts"],
        "_gold_ts": now,
    }

    upsert_gold(gold_record)


# ─────────────────────────────────────────────
# MAIN PIPELINE CONSUMER
# ─────────────────────────────────────────────


def run_realtime_pipeline(max_messages: int = None):
    """
    Main real-time pipeline.
    Consumes from Kafka and processes each message through
    Bronze -> Silver -> Gold in real time.
    """
    try:
        from confluent_kafka import Consumer, KafkaError, KafkaException
    except ImportError:
        log.error("confluent-kafka not installed")
        return

    # Initialize database
    initialize_database()

    cfg = dict(KAFKA_CONFIG)
    cfg.update(
        {
            "group.id": KAFKA_CONSUMER_GROUP + "-realtime",
            "auto.offset.reset": "latest",  # Only process NEW messages
            "enable.auto.commit": False,
        }
    )

    consumer = Consumer(cfg)
    consumer.subscribe([KAFKA_TOPIC])

    run_id = uuid.uuid4().hex[:12]
    processed = 0

    log.info(f"Real-time pipeline started. Run ID: {run_id}")
    log.info("Waiting for sensor readings...\n")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise KafkaException(msg.error())

            # Parse message
            try:
                data = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                log.error(f"Failed to parse message: {e}")
                continue

            batch_id = data.get("batch_id")
            tank_id = data.get("tank_id")

            # Process through all three layers
            log.info(f"Processing: {tank_id} | {batch_id}")

            # BRONZE
            bronze = process_bronze(data, run_id)

            # SILVER
            silver = process_silver(bronze)

            # GOLD
            process_gold(batch_id, tank_id)

            # Commit offset after successful processing
            consumer.commit()
            processed += 1

            # Show live layer counts every 10 messages
            if processed % 10 == 0:
                counts = get_layer_counts()
                log.info(
                    f"Layer counts -> "
                    f"Bronze: {counts['bronze']} | "
                    f"Silver: {counts['silver']} | "
                    f"Gold: {counts['gold']}"
                )

            # Log to audit table
            log_pipeline_run(run_id, "bronze->silver->gold", 1, "success")

            if max_messages and processed >= max_messages:
                log.info(f"Reached max messages: {max_messages}")
                break

    except KeyboardInterrupt:
        log.info(f"\nPipeline stopped. Total processed: {processed}")
    finally:
        consumer.close()
        counts = get_layer_counts()
        log.info(f"Final counts: {counts}")


if __name__ == "__main__":
    run_realtime_pipeline()
