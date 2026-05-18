"""
kafka_producer.py
=================
Simulates a real-time DCS historian stream by reading the bioprocess
dataset row-by-row and publishing each record to a Kafka topic.

In production at Manus this would be replaced by a real connector
to the DCS historian (OSIsoft PI, Honeywell Experion, etc.).
"""

import json
import time
import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# confluent_kafka is preferred for production; falls back to kafka-python
try:
    from confluent_kafka import Producer
    CONFLUENT = True
except ImportError:
    CONFLUENT = False

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import KAFKA_CONFIG, KAFKA_TOPIC, KAFKA_STREAM_DELAY, RAW_DATA_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [PRODUCER] %(message)s")
log = logging.getLogger(__name__)


def load_source_data(filepath: Path) -> pd.DataFrame:
    """Load raw bioprocess dataset — supports xlsx and csv."""
    log.info(f"Loading source data from: {filepath}")
    if str(filepath).endswith((".xlsx", ".xls")):
        xl = pd.ExcelFile(filepath)
        sheet = "Data" if "Data" in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(filepath, sheet_name=sheet)
    else:
        df = pd.read_csv(filepath)
    log.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
    return df


def build_message(row: dict, row_idx: int) -> dict:
    """
    Wrap a dataset row into a Kafka message envelope.
    Adds pipeline metadata so Bronze layer knows the source and timing.
    """
    return {
        # Pipeline metadata
        "message_id":        str(uuid.uuid4()),
        "ingestion_ts":      datetime.now(timezone.utc).isoformat(),
        "source_system":     "dcs_historian_sim",   # In prod: real historian name
        "pipeline_version":  "1.0.0",
        "row_index":         row_idx,

        # Actual bioprocess payload
        "payload": row,
    }


def delivery_report(err, msg):
    """Callback fired when Kafka confirms message delivery."""
    if err:
        log.error(f"Delivery failed for message {msg.key()}: {err}")
    else:
        log.debug(f"Delivered to {msg.topic()} partition [{msg.partition()}] offset {msg.offset()}")


def run_producer(filepath: Path = RAW_DATA_PATH, delay: float = KAFKA_STREAM_DELAY):
    """
    Main producer loop.
    Reads each row and publishes it to Kafka as a JSON message.
    """
    if not CONFLUENT:
        log.error("confluent-kafka not installed. Run: pip install confluent-kafka")
        return

    df = load_source_data(filepath)

    producer = Producer(KAFKA_CONFIG)
    log.info(f"Connected to Kafka. Publishing to topic: {KAFKA_TOPIC}")
    log.info(f"Streaming {len(df)} records at {delay}s intervals...")

    published = 0
    for idx, row in df.iterrows():
        # Convert row to clean dict (handle NaN → None for JSON serialisation)
        row_dict = {k: (None if pd.isna(v) else v) for k, v in row.items()}
        message  = build_message(row_dict, idx)

        producer.produce(
            topic    = KAFKA_TOPIC,
            key      = str(idx),
            value    = json.dumps(message),
            callback = delivery_report,
        )
        producer.poll(0)   # trigger delivery callbacks without blocking

        published += 1
        if published % 50 == 0:
            log.info(f"Published {published}/{len(df)} records")

        time.sleep(delay)

    producer.flush()   # wait for all messages to be delivered
    log.info(f"Done. Total records published: {published}")


if __name__ == "__main__":
    run_producer()
