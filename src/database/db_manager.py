"""
db_manager.py
=============
SQLite database manager for Manus Bioprocess Pipeline.
Handles connections, table creation, and all read/write operations
for Bronze, Silver, and Gold layers.
"""

import sqlite3
import logging
from datetime import datetime, timezone
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.database.schema import ALL_TABLES

logging.basicConfig(level=logging.INFO, format="%(asctime)s [DB] %(message)s")
log = logging.getLogger(__name__)

DB_PATH = Path(__file__).parent.parent.parent / "manus_pipeline.db"


def get_connection():
    """Get SQLite connection with row factory for dict-like access."""
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrent access
    return conn


def initialize_database():
    """Create all tables if they don't exist."""
    log.info(f"Initializing SQLite database at: {DB_PATH}")
    conn = get_connection()
    try:
        for table_sql in ALL_TABLES:
            conn.execute(table_sql)
        conn.commit()
        log.info("All tables created successfully")
    finally:
        conn.close()


def insert_bronze(record: dict) -> int:
    """Insert a raw sensor reading into Bronze table."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            """
            INSERT INTO bronze_fermentation (
                batch_id, tank_id, timestamp,
                temperature_c, pressure_bar, ph_level,
                dissolved_o2, agitation_rpm, feed_rate_lph,
                product_name, _source_system, _ingestion_ts,
                _pipeline_version, _bronze_run_id, _layer
            ) VALUES (
                :batch_id, :tank_id, :timestamp,
                :temperature_c, :pressure_bar, :ph_level,
                :dissolved_o2, :agitation_rpm, :feed_rate_lph,
                :product_name, :_source_system, :_ingestion_ts,
                :_pipeline_version, :_bronze_run_id, 'bronze'
            )
        """,
            record,
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def insert_silver(record: dict) -> int:
    """Insert a cleaned record into Silver table."""
    conn = get_connection()
    try:
        cursor = conn.execute(
            """
            INSERT INTO silver_fermentation (
                batch_id, tank_id, timestamp,
                temperature_c, pressure_bar, ph_level,
                dissolved_o2, agitation_rpm, feed_rate_lph,
                product_name, temp_valid, ph_valid, o2_valid,
                is_outlier, productivity_score, _silver_ts, _layer
            ) VALUES (
                :batch_id, :tank_id, :timestamp,
                :temperature_c, :pressure_bar, :ph_level,
                :dissolved_o2, :agitation_rpm, :feed_rate_lph,
                :product_name, :temp_valid, :ph_valid, :o2_valid,
                :is_outlier, :productivity_score, :_silver_ts, 'silver'
            )
        """,
            record,
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()


def upsert_gold(record: dict):
    """
    Insert or update Gold aggregation for a batch.
    Gold always shows the latest aggregated state per batch+tank.
    """
    conn = get_connection()
    try:
        # Check if batch already exists in Gold
        existing = conn.execute(
            """
            SELECT id FROM gold_fermentation_summary
            WHERE batch_id = ? AND tank_id = ?
        """,
            (record["batch_id"], record["tank_id"]),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE gold_fermentation_summary SET
                    avg_temperature    = :avg_temperature,
                    avg_pressure       = :avg_pressure,
                    avg_ph             = :avg_ph,
                    avg_dissolved_o2   = :avg_dissolved_o2,
                    avg_agitation      = :avg_agitation,
                    avg_feed_rate      = :avg_feed_rate,
                    total_readings     = :total_readings,
                    outlier_count      = :outlier_count,
                    data_quality_score = :data_quality_score,
                    last_reading_ts    = :last_reading_ts,
                    _gold_ts           = :_gold_ts
                WHERE batch_id = :batch_id AND tank_id = :tank_id
            """,
                record,
            )
        else:
            conn.execute(
                """
                INSERT INTO gold_fermentation_summary (
                    batch_id, tank_id, product_name,
                    avg_temperature, avg_pressure, avg_ph,
                    avg_dissolved_o2, avg_agitation, avg_feed_rate,
                    total_readings, outlier_count, data_quality_score,
                    first_reading_ts, last_reading_ts, _gold_ts, _layer
                ) VALUES (
                    :batch_id, :tank_id, :product_name,
                    :avg_temperature, :avg_pressure, :avg_ph,
                    :avg_dissolved_o2, :avg_agitation, :avg_feed_rate,
                    :total_readings, :outlier_count, :data_quality_score,
                    :first_reading_ts, :last_reading_ts, :_gold_ts, 'gold'
                )
            """,
                record,
            )

        conn.commit()
    finally:
        conn.close()


def log_pipeline_run(
    run_id: str, layer: str, rows: int, status: str, message: str = ""
):
    """Log every pipeline run to the audit table."""
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT INTO pipeline_run_log
            (run_id, layer, rows_processed, status, message, ts)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            (
                run_id,
                layer,
                rows,
                status,
                message,
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def get_layer_counts():
    """Get current row counts for all layers — for monitoring."""
    conn = get_connection()
    try:
        bronze = conn.execute("SELECT COUNT(*) FROM bronze_fermentation").fetchone()[0]
        silver = conn.execute("SELECT COUNT(*) FROM silver_fermentation").fetchone()[0]
        gold = conn.execute(
            "SELECT COUNT(*) FROM gold_fermentation_summary"
        ).fetchone()[0]
        return {"bronze": bronze, "silver": silver, "gold": gold}
    finally:
        conn.close()


def get_latest_bronze(n: int = 5):
    """Get latest n Bronze records."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT batch_id, tank_id, timestamp, temperature_c,
                   pressure_bar, ph_level, dissolved_o2, _ingestion_ts
            FROM bronze_fermentation
            ORDER BY id DESC LIMIT ?
        """,
            (n,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_latest_silver(n: int = 5):
    """Get latest n Silver records."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT batch_id, tank_id, timestamp, temperature_c,
                   ph_level, temp_valid, ph_valid, is_outlier,
                   productivity_score, _silver_ts
            FROM silver_fermentation
            ORDER BY id DESC LIMIT ?
        """,
            (n,),
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_gold_summary():
    """Get all Gold aggregations."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT batch_id, tank_id, product_name,
                   avg_temperature, avg_ph, avg_dissolved_o2,
                   total_readings, outlier_count,
                   data_quality_score, last_reading_ts
            FROM gold_fermentation_summary
            ORDER BY last_reading_ts DESC
        """
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def reset_database():
    """Drop all tables and recreate — for fresh demo runs."""
    conn = get_connection()
    try:
        conn.execute("DROP TABLE IF EXISTS bronze_fermentation")
        conn.execute("DROP TABLE IF EXISTS silver_fermentation")
        conn.execute("DROP TABLE IF EXISTS gold_fermentation_summary")
        conn.execute("DROP TABLE IF EXISTS pipeline_run_log")
        conn.commit()
        log.info("Database reset complete")
    finally:
        conn.close()
    initialize_database()


if __name__ == "__main__":
    initialize_database()
    counts = get_layer_counts()
    print(f"Database initialized: {counts}")
