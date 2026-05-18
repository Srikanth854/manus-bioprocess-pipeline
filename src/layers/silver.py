"""
silver.py
=========
Silver Layer — Cleaned, normalized, schema-conformant data.

Reads from Bronze, applies all cleaning and transformation logic,
writes clean Parquet to Silver.

What Silver does:
- Handles missing values (imputation or flagging)
- Detects and handles outliers
- Normalizes data types
- Aligns timestamps
- Applies business rules and mapping tables
- Runs uniqueness and completeness checks
- Cleans messy gene/genotype string columns

What Silver does NOT do:
- Aggregations (that's Gold)
- Modeling (that's Gold)
- Any business-level joins (that's Gold)
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import BRONZE_PATH, PIPELINE_VERSION, QUALITY_CHECKS, SILVER_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s [SILVER] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 1. LOAD FROM BRONZE
# ─────────────────────────────────────────────


def load_from_bronze(date_str: str = None) -> pd.DataFrame:
    """
    Load all Bronze Parquet files for a given date (defaults to today).
    Merges batch and Kafka-streamed data into one DataFrame.
    """
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")

    bronze_date_dir = BRONZE_PATH / f"date={date_str}"
    if not bronze_date_dir.exists():
        raise FileNotFoundError(f"No Bronze data found for date: {date_str}")

    parquet_files = list(bronze_date_dir.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files in Bronze for date: {date_str}")

    log.info(f"Loading {len(parquet_files)} Bronze file(s) from {bronze_date_dir}")
    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)

    log.info(f"Bronze loaded: {len(df)} rows x {len(df.columns)} columns")
    return df


# ─────────────────────────────────────────────
# 2. CLEANING FUNCTIONS
# ─────────────────────────────────────────────


def drop_metadata_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Remove Bronze metadata columns before transformation."""
    meta_cols = [c for c in df.columns if c.startswith("_")]
    return df.drop(columns=meta_cols, errors="ignore")


def handle_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Business-rule-based null handling:
    - Numeric columns: median imputation (flag with _imputed suffix)
    - Critical ID columns: drop row if null
    - Non-critical string columns: fill with 'unknown'
    """
    log.info("Handling missing values...")

    # Critical columns — drop rows if null
    critical_cols = ["paper_number", "product_name"]
    before = len(df)
    df = df.dropna(subset=[c for c in critical_cols if c in df.columns])
    dropped = before - len(df)
    if dropped:
        log.info(f"Dropped {dropped} rows with null critical columns")

    # Numeric columns — median imputation
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    for col in numeric_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            median_val = df[col].median()
            df[f"{col}_was_null"] = df[col].isnull().astype(int)  # flag before imputing
            df[col] = df[col].fillna(median_val)
            log.info(
                f"  {col}: imputed {null_count} nulls with median {median_val:.4f}"
            )

    # String columns — fill with 'unknown'
    str_cols = df.select_dtypes(include=["object"]).columns.tolist()
    for col in str_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            df[col] = df[col].fillna("unknown")

    return df


def detect_and_handle_outliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    IQR-based outlier detection on key numeric columns.
    Outliers are flagged (not dropped) — preserves data for audit.
    """
    log.info("Detecting outliers...")

    target_cols = ["titer", "yield", "rate", "bio_titre", "bio_growth_rate"]
    available = [c for c in target_cols if c in df.columns]

    for col in available:
        # Skip if column is not numeric
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue

        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr

        outlier_mask = (df[col] < lower) | (df[col] > upper)
        df[f"{col}_is_outlier"] = outlier_mask.astype(int)

        n_outliers = outlier_mask.sum()
        if n_outliers:
            log.info(
                f"  {col}: {n_outliers} outliers flagged (range: {lower:.4f}–{upper:.4f})"
            )

    return df


def normalize_temperature(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure temperature is in Celsius and within valid range."""
    if "temp" not in df.columns:
        return df

    t_min, t_max = QUALITY_CHECKS["temp_valid_range"]
    invalid = (df["temp"] < t_min) | (df["temp"] > t_max)
    if invalid.sum():
        log.warning(
            f"Temperature: clamping {invalid.sum()} out-of-range values to [{t_min}, {t_max}]"
        )
        df["temp"] = df["temp"].clip(lower=t_min, upper=t_max)

    return df


def clean_gene_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    The gene/genotype columns in this dataset are messy comma-separated strings
    like 'ompT, gal, dcm, lon'. This cleans and normalizes them.
    """
    gene_cols = [
        "strain_background_genotype",
        "genes_modified",
        "gene_deletion",
        "gene_overexpression",
        "heterologous_gene",
    ]

    for col in [c for c in gene_cols if c in df.columns]:
        # Strip whitespace from each gene in the list
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].apply(
            lambda x: (
                ",".join([g.strip() for g in x.split(",")])
                if x not in ("nan", "NA", "unknown")
                else x
            )
        )
        # Count number of genes as a numeric feature
        df[f"{col}_count"] = df[col].apply(
            lambda x: len(x.split(",")) if x not in ("nan", "NA", "unknown", "0") else 0
        )

    return df


def add_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Engineer new features that are useful for Gold layer analytics.
    Based on domain knowledge of bioprocess engineering.
    """
    # Productivity = titer / fermentation_time
    if "titer" in df.columns and "fermentation_time" in df.columns:
        df["productivity"] = df["titer"] / df["fermentation_time"].replace(0, np.nan)

    # Yield efficiency (yield vs theoretical max)
    if "yield" in df.columns and "yield_o" in df.columns:
        df["yield_efficiency"] = df["yield"] / df["yield_o"].replace(0, np.nan)

    # Molecular complexity score
    if "no_C" in df.columns and "no_H" in df.columns and "no_O" in df.columns:
        df["molecular_complexity"] = df["no_C"] + df["no_H"] + df["no_O"]

    return df


def add_silver_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """Stamp Silver metadata onto each row."""
    df["_silver_ts"] = datetime.now(timezone.utc).isoformat()
    df["_silver_pipeline_version"] = PIPELINE_VERSION
    df["_layer"] = "silver"
    return df


# ─────────────────────────────────────────────
# 3. SILVER QUALITY CHECKS
# ─────────────────────────────────────────────


def run_silver_quality_checks(df: pd.DataFrame) -> tuple[bool, list]:
    """Quality checks after cleaning — these should be stricter than Bronze."""
    issues = []

    # Uniqueness check on paper_number + row index
    if "paper_number" in df.columns:
        dupes = df.duplicated(subset=["paper_number"]).sum()
        # Note: duplicates are expected (multiple runs per paper) — just log
        log.info(
            f"Duplicate paper_number entries: {dupes} (expected — multiple runs per paper)"
        )

    # Completeness check on key output columns
    key_outputs = ["titer", "yield", "rate"]
    for col in [c for c in key_outputs if c in df.columns]:
        null_pct = df[col].isnull().mean()
        if null_pct > QUALITY_CHECKS["max_null_pct"]:
            issues.append(
                f"Silver completeness: '{col}' still has {null_pct:.1%} nulls after cleaning"
            )

    # Titer must be non-negative
    if "titer" in df.columns:
        negative = (df["titer"] < 0).sum()
        if negative:
            issues.append(f"Silver: {negative} negative titer values found")

    passed = len(issues) == 0
    if passed:
        log.info("Silver quality checks PASSED")
    else:
        for issue in issues:
            log.warning(f"Silver QC: {issue}")

    return passed, issues


# ─────────────────────────────────────────────
# 4. WRITE TO SILVER
# ─────────────────────────────────────────────


def write_to_silver(df: pd.DataFrame) -> Path:
    """Write cleaned DataFrame to Silver as Parquet."""
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_dir = SILVER_PATH / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "bioprocess_silver.parquet"
    df.to_parquet(out_path, index=False)

    log.info(f"Silver write complete: {out_path} ({len(df)} rows)")
    return out_path


# ─────────────────────────────────────────────
# 5. MAIN SILVER PIPELINE
# ─────────────────────────────────────────────


def run_silver_pipeline(df_bronze: pd.DataFrame = None) -> tuple[pd.DataFrame, Path]:
    """
    Full Silver pipeline:
    Bronze → clean → quality check → write Silver.

    Args:
        df_bronze: Pass DataFrame directly (from Bronze layer) or None to load from disk.
    """
    log.info("Starting Silver pipeline...")

    if df_bronze is None:
        df = load_from_bronze()
    else:
        df = df_bronze.copy()

    # Run all cleaning steps
    df = drop_metadata_columns(df)
    df = handle_missing_values(df)
    df = detect_and_handle_outliers(df)
    df = normalize_temperature(df)
    df = clean_gene_columns(df)
    df = add_derived_features(df)
    df = add_silver_metadata(df)

    # Quality checks
    passed, issues = run_silver_quality_checks(df)

    # Write to Silver
    out_path = write_to_silver(df)

    log.info(f"Silver pipeline complete. {len(df)} rows, {len(df.columns)} columns.")
    return df, out_path


if __name__ == "__main__":
    df, path = run_silver_pipeline()
    print(f"\nSilver layer ready: {path}")
    print(f"Shape: {df.shape}")
    print("\nSample columns:", df.columns.tolist()[:10])
