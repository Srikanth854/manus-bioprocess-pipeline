"""
main.py
=======
Manus Bioprocess Data Pipeline — Main Orchestrator

Runs the full pipeline end to end:
  1. Batch ingestion (Kaggle dataset → Bronze)
  2. Kafka streaming (simulated DCS stream → Bronze)  [optional]
  3. API enrichment (PubChem compound data → Bronze)  [optional]
  4. Silver pipeline (Bronze → cleaned Silver)
  5. Gold pipeline (Silver → analytics + insights)

Usage:
  python main.py                          # Full pipeline with Kafka + API
  python main.py --mode batch             # Batch only (no Kafka)
  python main.py --skip-kafka             # Skip Kafka streaming
  python main.py --skip-api               # Skip PubChem API calls
  python main.py --skip-kafka --skip-api  # Batch only (CI mode)
"""

import argparse
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from config import PIPELINE_NAME, PIPELINE_VERSION, RAW_DATA_PATH
from src.layers.bronze import ingest_batch
from src.layers.gold import run_gold_pipeline
from src.layers.silver import run_silver_pipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("main")


def print_banner():
    print("\n" + "=" * 65)
    print(f"  {PIPELINE_NAME.upper()}  v{PIPELINE_VERSION}")
    print("  Manus Biomanufacturing — Data Unification Platform")
    print("=" * 65 + "\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Manus Bioprocess Pipeline")
    parser.add_argument(
        "--mode", default="full", choices=["full", "batch"], help="Pipeline mode"
    )
    parser.add_argument(
        "--skip-kafka", action="store_true", help="Skip Kafka streaming"
    )
    parser.add_argument(
        "--skip-api", action="store_true", help="Skip PubChem API enrichment"
    )
    parser.add_argument(
        "--data-path", default=str(RAW_DATA_PATH), help="Path to raw dataset"
    )
    return parser.parse_args()


def run_kafka_stream(data_path: Path):
    """Run Kafka producer + consumer in parallel threads."""
    import threading

    try:
        from src.ingestion.kafka_consumer import run_consumer
        from src.ingestion.kafka_producer import run_producer

        log.info("Starting Kafka streaming pipeline...")

        # Start consumer in background thread
        consumer_thread = threading.Thread(
            target=run_consumer, kwargs={"max_messages": 500}, daemon=True
        )
        consumer_thread.start()

        # Give consumer time to initialise
        time.sleep(2)

        # Run producer (streams dataset row by row)
        run_producer(filepath=data_path, delay=0.05)

        # Wait for consumer to finish processing
        consumer_thread.join(timeout=60)
        log.info("Kafka streaming complete")

    except ImportError:
        log.warning("confluent-kafka not installed — skipping Kafka streaming")
        log.warning("Install with: pip install confluent-kafka")


def run_api_enrichment(df_bronze):
    """Call PubChem API to enrich product data."""
    try:
        from src.ingestion.api_client import enrich_products, save_to_bronze

        log.info("Starting PubChem API enrichment...")
        enriched = enrich_products(df_bronze)
        save_to_bronze(enriched)
        log.info("API enrichment complete")
        return enriched
    except Exception as e:
        log.warning(f"API enrichment failed (non-critical): {e}")
        return df_bronze


def main():
    print_banner()
    args = parse_args()

    start_time = time.time()
    run_id_global = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    log.info(f"Pipeline run started | Mode: {args.mode} | Run: {run_id_global}")
    log.info(f"Dataset path: {args.data_path}")

    # ─────────────────────────────────────
    # STEP 1: BATCH INGESTION → BRONZE
    # ─────────────────────────────────────
    log.info("=" * 50)
    log.info("STEP 1: Batch ingestion → Bronze")
    log.info("=" * 50)

    df_bronze, run_id = ingest_batch(filepath=Path(args.data_path))
    log.info(f"Bronze ready: {len(df_bronze)} rows")

    # ─────────────────────────────────────
    # STEP 2: KAFKA STREAMING → BRONZE (optional)
    # ─────────────────────────────────────
    if not args.skip_kafka and args.mode == "full":
        log.info("=" * 50)
        log.info("STEP 2: Kafka streaming → Bronze")
        log.info("=" * 50)
        run_kafka_stream(Path(args.data_path))
    else:
        log.info("STEP 2: Kafka streaming — SKIPPED")

    # ─────────────────────────────────────
    # STEP 3: API ENRICHMENT → BRONZE (optional)
    # ─────────────────────────────────────
    if not args.skip_api and args.mode == "full":
        log.info("=" * 50)
        log.info("STEP 3: PubChem API enrichment → Bronze")
        log.info("=" * 50)
        df_bronze = run_api_enrichment(df_bronze)
    else:
        log.info("STEP 3: API enrichment — SKIPPED")

    # ─────────────────────────────────────
    # STEP 4: SILVER PIPELINE
    # ─────────────────────────────────────
    log.info("=" * 50)
    log.info("STEP 4: Bronze → Silver (cleaning + transformation)")
    log.info("=" * 50)

    df_silver, silver_path = run_silver_pipeline(df_bronze=df_bronze)
    log.info(f"Silver ready: {len(df_silver)} rows, {len(df_silver.columns)} columns")

    # ─────────────────────────────────────
    # STEP 5: GOLD PIPELINE
    # ─────────────────────────────────────
    log.info("=" * 50)
    log.info("STEP 5: Silver → Gold (analytics + insights)")
    log.info("=" * 50)

    gold_results = run_gold_pipeline(df_silver=df_silver)

    # ─────────────────────────────────────
    # SUMMARY
    # ─────────────────────────────────────
    elapsed = time.time() - start_time

    print("\n" + "=" * 65)
    print("  PIPELINE COMPLETE")
    print("=" * 65)
    print(f"  Run ID:      {run_id_global}")
    print(f"  Duration:    {elapsed:.1f}s")
    print(f"  Bronze rows: {len(df_bronze)}")
    print(f"  Silver rows: {len(df_silver)}")
    print(f"\n  Gold tables:")
    for name, df in gold_results["tables"].items():
        if df is not None:
            print(f"    {name}: {len(df)} rows")
    print(f"\n  Insight charts saved to: outputs/")
    print("=" * 65 + "\n")


if __name__ == "__main__":
    main()
