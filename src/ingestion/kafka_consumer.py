"""
kafka_consumer.py
=================
Consumes messages from the Kafka bioprocess topic and lands them
in the Bronze layer as Parquet files.

Implements manual offset commits — messages are only marked as
consumed after they are successfully written to Bronze.
This guarantees fault-tolerant, exactly-once-style delivery.
"""

import json
import logging
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:
    from confluent_kafka import Consumer, KafkaError, KafkaException

    CONFLUENT = True
except ImportError:
    CONFLUENT = False

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import KAFKA_CONFIG, KAFKA_TOPIC, KAFKA_CONSUMER_GROUP, BRONZE_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CONSUMER] %(message)s")
log = logging.getLogger(__name__)

# How many messages to batch before writing to Bronze (saves small-file overhead)
BATCH_SIZE = 50


def get_consumer_config() -> dict:
    """Build consumer config from base Kafka config."""
    cfg = dict(KAFKA_CONFIG)
    cfg.update(
        {
            "group.id": KAFKA_CONSUMER_GROUP,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # MANUAL commits — key for fault tolerance
        }
    )
    return cfg


def write_batch_to_bronze(batch: list, run_id: str):
    """
    Write a batch of consumed messages to Bronze as Parquet.
    Each batch becomes one Parquet file partitioned by run_id.
    """
    if not batch:
        return

    records = []
    for msg in batch:
        envelope = json.loads(msg.value().decode("utf-8"))
        row = envelope.get("payload", {})

        # Attach Bronze metadata
        row["_message_id"] = envelope.get("message_id")
        row["_ingestion_ts"] = envelope.get("ingestion_ts")
        row["_source_system"] = envelope.get("source_system")
        row["_pipeline_version"] = envelope.get("pipeline_version")
        row["_bronze_run_id"] = run_id
        row["_bronze_write_ts"] = datetime.now(timezone.utc).isoformat()

        records.append(row)

    df = pd.DataFrame(records)

    # Partition Bronze files by date and run_id
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_dir = BRONZE_PATH / f"date={date_str}" / f"run={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)

    file_name = f"batch_{uuid.uuid4().hex[:8]}.parquet"
    out_path = out_dir / file_name
    df.to_parquet(out_path, index=False)

    log.info(f"Written {len(records)} records to Bronze: {out_path}")
    return out_path


def run_consumer(max_messages: int = None):
    """
    Main consumer loop.

    Args:
        max_messages: Stop after consuming this many messages (useful for testing).
                      None = run indefinitely until interrupted.
    """
    if not CONFLUENT:
        log.error("confluent-kafka not installed. Run: pip install confluent-kafka")
        return

    consumer = Consumer(get_consumer_config())
    consumer.subscribe([KAFKA_TOPIC])

    run_id = uuid.uuid4().hex[:12]
    batch = []
    consumed = 0
    files_written = 0

    log.info(f"Consumer started. Run ID: {run_id}")
    log.info(f"Subscribed to topic: {KAFKA_TOPIC}")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # No new messages — flush any partial batch
                if batch:
                    write_batch_to_bronze(batch, run_id)
                    consumer.commit()
                    files_written += 1
                    batch = []
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    log.info("Reached end of partition")
                else:
                    raise KafkaException(msg.error())
                continue

            batch.append(msg)
            consumed += 1

            # Write batch to Bronze and commit offsets
            if len(batch) >= BATCH_SIZE:
                write_batch_to_bronze(batch, run_id)
                consumer.commit()  # manual commit AFTER successful write
                files_written += 1
                batch = []
                log.info(f"Total consumed: {consumed} | Bronze files: {files_written}")

            if max_messages and consumed >= max_messages:
                log.info(f"Reached max_messages limit ({max_messages}). Stopping.")
                break

    except KeyboardInterrupt:
        log.info("Consumer interrupted by user")
    finally:
        # Flush remaining batch
        if batch:
            write_batch_to_bronze(batch, run_id)
            consumer.commit()

        consumer.close()
        log.info(
            f"Consumer closed. Total consumed: {consumed} records, {files_written} Bronze files written."
        )


if __name__ == "__main__":
    run_consumer()
