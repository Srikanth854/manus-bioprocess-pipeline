"""
bronze.py
=========
Bronze Layer — Raw data landing zone.

Handles batch ingestion (CSV/Excel) directly to Bronze.
The Kafka consumer also writes to Bronze (see kafka_consumer.py).
This module handles the BATCH path and data quality gate.

Bronze principle: land data RAW, never transform.
Add only metadata (source, timestamps, run_id).
"""

import uuid
import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

import sys

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import BRONZE_PATH, RAW_DATA_PATH, QUALITY_CHECKS, PIPELINE_VERSION

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BRONZE] %(message)s")
log = logging.getLogger(__name__)


def load_raw_data(filepath: Path = RAW_DATA_PATH) -> pd.DataFrame:
    """Load raw bioprocess dataset from file."""
    log.info(f"Loading raw data from: {filepath}")

    if not filepath.exists():
        raise FileNotFoundError(
            f"Dataset not found at {filepath}.\n"
            f"Download from: https://www.kaggle.com/datasets/cbeurrier/bioprocess-performance\n"
            f"and place the file at: {filepath}"
        )

    if str(filepath).endswith((".xlsx", ".xls")):
        xl = pd.ExcelFile(filepath)
        sheet = "Data" if "Data" in xl.sheet_names else xl.sheet_names[0]
        df = pd.read_excel(filepath, sheet_name=sheet)
    else:
        df = pd.read_csv(filepath)

    log.info(f"Loaded {len(df)} rows x {len(df.columns)} columns")
    return df


def add_bronze_metadata(df: pd.DataFrame, run_id: str, source: str) -> pd.DataFrame:
    """
    Stamp each row with Bronze metadata.
    This is the ONLY transformation Bronze does — everything else is raw.
    """
    df = df.copy()
    now = datetime.now(timezone.utc).isoformat()

    df["_bronze_run_id"] = run_id
    df["_source_system"] = source
    df["_ingestion_ts"] = now
    df["_pipeline_version"] = PIPELINE_VERSION
    df["_layer"] = "bronze"

    return df


def run_quality_gate(df: pd.DataFrame) -> tuple[bool, list]:
    """
    Data quality gate — runs before writing to Bronze.
    Returns (passed: bool, issues: list of strings).
    """
    issues = []

    # Check 1: Minimum row count
    if len(df) < QUALITY_CHECKS["min_row_count"]:
        issues.append(
            f"Row count {len(df)} is below minimum {QUALITY_CHECKS['min_row_count']}"
        )

    # Check 2: Null percentage per column
    for col in df.columns:
        if col.startswith("_"):
            continue  # skip metadata columns
        null_pct = df[col].isnull().mean()
        if null_pct > QUALITY_CHECKS["max_null_pct"]:
            issues.append(
                f"Column '{col}' has {null_pct:.1%} nulls (threshold: {QUALITY_CHECKS['max_null_pct']:.0%})"
            )

    # Check 3: Temperature range (if column exists)
    if "temp" in df.columns:
        temp_min, temp_max = QUALITY_CHECKS["temp_valid_range"]
        out_of_range = df["temp"].dropna()
        out_of_range = out_of_range[
            (out_of_range < temp_min) | (out_of_range > temp_max)
        ]
        if len(out_of_range) > 0:
            issues.append(
                f"Temperature: {len(out_of_range)} values outside valid range {QUALITY_CHECKS['temp_valid_range']}"
            )

    # Check 4: Yield range (0–1)
    if "yield" in df.columns:
        y_min, y_max = QUALITY_CHECKS["yield_valid_range"]
        bad_yield = df["yield"].dropna()
        bad_yield = bad_yield[(bad_yield < y_min) | (bad_yield > y_max)]
        if len(bad_yield) > 0:
            issues.append(
                f"Yield: {len(bad_yield)} values outside valid range {QUALITY_CHECKS['yield_valid_range']}"
            )

    passed = len(issues) == 0

    if passed:
        log.info("Quality gate PASSED")
    else:
        log.warning(f"Quality gate found {len(issues)} issue(s):")
        for issue in issues:
            log.warning(f"  - {issue}")

    return passed, issues


def sanitize_mixed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert any column with mixed types (e.g. numbers + strings like 'no.1')
    to string so PyArrow can safely write to Parquet.
    This is a Bronze-layer concern — we never lose raw data, just ensure it lands.
    """
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
        else:
            # Try numeric — if it fails, fall back to string
            try:
                df[col] = pd.to_numeric(df[col], errors="raise")
            except (ValueError, TypeError):
                df[col] = df[col].astype(str)
    return df


def write_to_bronze(df: pd.DataFrame, run_id: str) -> Path:
    """Write DataFrame to Bronze as Parquet, partitioned by date."""
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_dir = BRONZE_PATH / f"date={date_str}" / f"run={run_id}" / "source=batch"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Sanitize mixed-type columns before Parquet write
    df = sanitize_mixed_columns(df)

    out_path = out_dir / "bioprocess_raw.parquet"
    df.to_parquet(out_path, index=False)

    log.info(f"Bronze write complete: {out_path} ({len(df)} rows)")
    return out_path


def ingest_batch(filepath: Path = RAW_DATA_PATH) -> tuple[pd.DataFrame, str]:
    """
    Full batch ingestion pipeline:
    Load → Quality gate → Add metadata → Write to Bronze.
    Returns (df, run_id).
    """
    run_id = uuid.uuid4().hex[:12]
    log.info(f"Starting batch ingestion. Run ID: {run_id}")

    # Load
    df = load_raw_data(filepath)

    # Quality gate
    passed, issues = run_quality_gate(df)
    if not passed:
        log.warning(
            "Quality gate failed — data will still land in Bronze with issue flags."
        )

    # Add metadata
    df = add_bronze_metadata(df, run_id, source="kaggle_batch")

    # Write
    write_to_bronze(df, run_id)

    log.info(f"Batch ingestion complete. {len(df)} rows in Bronze.")
    return df, run_id


if __name__ == "__main__":
    df, run_id = ingest_batch()
    print(f"\nBronze layer ready. Run ID: {run_id}")
    print(f"Shape: {df.shape}")
    print(df.head(2))
