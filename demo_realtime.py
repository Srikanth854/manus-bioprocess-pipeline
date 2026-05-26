"""
demo_realtime.py
================
Real-time demo script for Manus Bioprocess Pipeline presentation.

This script shows the complete real-time data flow:
  3 simulated fermentation tanks
  -> Kafka streaming
  -> Bronze (raw landing)
  -> Silver (cleaning + validation)
  -> Gold (live aggregations)
  -> SQLite database (queryable in DBeaver)

HOW TO RUN:
  Terminal 1: python demo_realtime.py --mode simulate   (generates sensor data)
  Terminal 2: python demo_realtime.py --mode pipeline   (processes data through layers)
  Terminal 3: python demo_realtime.py --mode monitor    (shows live layer counts)
  Terminal 4: python demo_realtime.py --mode query      (queries each layer)
  Terminal X: python demo_realtime.py --reset           (reset database for fresh demo)
"""

import argparse
import time
import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent))
from src.database.db_manager import (
    initialize_database,
    reset_database,
    get_layer_counts,
    get_latest_bronze,
    get_latest_silver,
    get_gold_summary,
    DB_PATH,
)


def print_banner():
    print("\n" + "=" * 65)
    print("  MANUS BIOPROCESS PIPELINE — REAL TIME DEMO")
    print("  3 Fermentation Tanks | Kafka Streaming | Bronze->Silver->Gold")
    print("=" * 65 + "\n")


def mode_simulate():
    """Run the DCS historian simulator — generates live sensor data."""
    from src.ingestion.realtime_simulator import run_simulator

    print_banner()
    print("SIMULATOR MODE — Generating live sensor readings")
    print("Simulating 3 fermentation tanks streaming to Kafka")
    print("Press Ctrl+C to stop\n")
    run_simulator(interval_seconds=2.0, anomaly_probability=0.05)


def mode_pipeline():
    """Run the real-time pipeline — processes Kafka messages through all layers."""
    from src.ingestion.realtime_pipeline import run_realtime_pipeline

    print_banner()
    print("PIPELINE MODE — Processing sensor data through Bronze->Silver->Gold")
    print(f"Database: {DB_PATH}")
    print("Press Ctrl+C to stop\n")
    run_realtime_pipeline()


def mode_monitor():
    """Show live row counts at each layer — refreshes every 2 seconds."""
    print_banner()
    print("MONITOR MODE — Live layer counts (refreshes every 2s)")
    print("Press Ctrl+C to stop\n")
    print(f"{'Time':<12} {'Bronze':>10} {'Silver':>10} {'Gold':>10} {'Status'}")
    print("-" * 55)

    try:
        while True:
            counts = get_layer_counts()
            now = datetime.now().strftime("%H:%M:%S")

            status = "flowing" if counts["bronze"] > 0 else "waiting..."

            print(
                f"{now:<12} "
                f"{counts['bronze']:>10,} "
                f"{counts['silver']:>10,} "
                f"{counts['gold']:>10,} "
                f"  {status}"
            )
            time.sleep(2)

    except KeyboardInterrupt:
        print("\nMonitor stopped.")


def mode_query():
    """Query and display data from each layer."""
    print_banner()
    print("QUERY MODE — Showing data at each layer\n")

    counts = get_layer_counts()

    # BRONZE
    print("=" * 65)
    print(f"BRONZE LAYER — Raw data ({counts['bronze']:,} total records)")
    print("No transformation — exactly as received from DCS historian")
    print("=" * 65)

    bronze_rows = get_latest_bronze(5)
    if bronze_rows:
        print(
            f"\n{'Batch':<20} {'Tank':<10} {'Timestamp':<25} {'Temp C':>8} {'pH':>6} {'O2%':>6}"
        )
        print("-" * 80)
        for row in bronze_rows:
            print(
                f"{row['batch_id']:<20} "
                f"{row['tank_id']:<10} "
                f"{row['timestamp'][:19]:<25} "
                f"{row['temperature_c']:>8.2f} "
                f"{row['ph_level']:>6.2f} "
                f"{row['dissolved_o2']:>6.1f}"
            )
    else:
        print("No Bronze data yet — run the simulator and pipeline first")

    # SILVER
    print(f"\n{'='*65}")
    print(f"SILVER LAYER — Cleaned and validated ({counts['silver']:,} total records)")
    print("Outliers flagged | Validation checks applied | Features derived")
    print("=" * 65)

    silver_rows = get_latest_silver(5)
    if silver_rows:
        print(
            f"\n{'Batch':<20} {'Tank':<10} {'Temp C':>8} {'pH':>6} {'TempOK':>8} {'Outlier':>8} {'Score':>8}"
        )
        print("-" * 80)
        for row in silver_rows:
            temp_ok = "YES" if row["temp_valid"] else "NO"
            outlier = "YES" if row["is_outlier"] else "no"
            print(
                f"{row['batch_id']:<20} "
                f"{row['tank_id']:<10} "
                f"{row['temperature_c']:>8.2f} "
                f"{row['ph_level']:>6.2f} "
                f"{temp_ok:>8} "
                f"{outlier:>8} "
                f"{row['productivity_score']:>8.3f}"
            )
    else:
        print("No Silver data yet")

    # GOLD
    print(f"\n{'='*65}")
    print(f"GOLD LAYER — Live aggregations ({counts['gold']:,} active batches)")
    print("Real-time averages per batch — updates with every new reading")
    print("=" * 65)

    gold_rows = get_gold_summary()
    if gold_rows:
        print(
            f"\n{'Batch':<20} {'Tank':<10} {'Product':<12} {'AvgTemp':>8} {'AvgPH':>7} {'Readings':>10} {'Quality%':>10}"
        )
        print("-" * 85)
        for row in gold_rows:
            print(
                f"{row['batch_id']:<20} "
                f"{row['tank_id']:<10} "
                f"{row['product_name']:<12} "
                f"{row['avg_temperature']:>8.2f} "
                f"{row['avg_ph']:>7.2f} "
                f"{row['total_readings']:>10,} "
                f"{row['data_quality_score']:>9.1f}%"
            )
    else:
        print("No Gold data yet")

    print(f"\nDatabase location: {DB_PATH}")
    print("Open in DBeaver to query interactively\n")


def mode_reset():
    """Reset the database for a fresh demo."""
    print_banner()
    print("RESET — Clearing all data for fresh demo run...")
    reset_database()
    initialize_database()
    print("Database reset complete. Ready for demo.")
    counts = get_layer_counts()
    print(f"Current counts: {counts}\n")


def parse_args():
    parser = argparse.ArgumentParser(description="Manus Real-Time Demo")
    parser.add_argument(
        "--mode",
        choices=["simulate", "pipeline", "monitor", "query"],
        help="Demo mode to run",
    )
    parser.add_argument(
        "--reset", action="store_true", help="Reset database for fresh demo"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.reset:
        mode_reset()
        sys.exit(0)

    if not args.mode:
        print_banner()
        print("Usage:")
        print("  python demo_realtime.py --reset                  Reset database")
        print(
            "  python demo_realtime.py --mode simulate          Start sensor simulator"
        )
        print(
            "  python demo_realtime.py --mode pipeline          Start pipeline consumer"
        )
        print("  python demo_realtime.py --mode monitor           Live layer counts")
        print("  python demo_realtime.py --mode query             Query all layers")
        print()
        print("Run simulate + pipeline in separate terminals simultaneously")
        sys.exit(0)

    if args.mode == "simulate":
        mode_simulate()
    elif args.mode == "pipeline":
        mode_pipeline()
    elif args.mode == "monitor":
        mode_monitor()
    elif args.mode == "query":
        mode_query()
