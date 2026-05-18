"""
test_data_quality.py
====================
Data quality tests — run by GitHub Actions CI on every push.
Tests that each layer meets quality standards before promotion.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))
from config import BRONZE_PATH, GOLD_PATH, QUALITY_CHECKS, SILVER_PATH

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────


def load_latest_parquet(layer_path: Path) -> pd.DataFrame:
    """Load the most recent Parquet file from a layer directory."""
    files = list(layer_path.rglob("*.parquet"))
    if not files:
        pytest.skip(f"No Parquet files found in {layer_path} — skipping layer test")
    latest = max(files, key=lambda f: f.stat().st_mtime)
    return pd.read_parquet(latest)


# ─────────────────────────────────────────────
# BRONZE TESTS
# ─────────────────────────────────────────────


class TestBronzeLayer:

    def test_bronze_has_data(self):
        """Bronze must have at least one Parquet file with data."""
        df = load_latest_parquet(BRONZE_PATH)
        assert (
            len(df) >= QUALITY_CHECKS["min_row_count"]
        ), f"Bronze has only {len(df)} rows — minimum is {QUALITY_CHECKS['min_row_count']}"

    def test_bronze_has_metadata_columns(self):
        """Bronze rows must have pipeline metadata stamped."""
        df = load_latest_parquet(BRONZE_PATH)
        required_meta = ["_source_system", "_ingestion_ts", "_pipeline_version"]
        for col in required_meta:
            assert col in df.columns, f"Bronze missing metadata column: {col}"

    def test_bronze_ingestion_ts_not_null(self):
        """Ingestion timestamp must never be null."""
        df = load_latest_parquet(BRONZE_PATH)
        if "_ingestion_ts" in df.columns:
            assert (
                df["_ingestion_ts"].isnull().sum() == 0
            ), "Null ingestion timestamps in Bronze"

    def test_bronze_null_threshold(self):
        """No data column should exceed max null threshold."""
        df = load_latest_parquet(BRONZE_PATH)
        data_cols = [c for c in df.columns if not c.startswith("_")]
        for col in data_cols:
            null_pct = df[col].isnull().mean()
            assert (
                null_pct <= QUALITY_CHECKS["max_null_pct"]
            ), f"Bronze column '{col}' has {null_pct:.1%} nulls — exceeds threshold"


# ─────────────────────────────────────────────
# SILVER TESTS
# ─────────────────────────────────────────────


class TestSilverLayer:

    def test_silver_has_data(self):
        """Silver must have data."""
        df = load_latest_parquet(SILVER_PATH)
        assert len(df) > 0, "Silver layer is empty"

    def test_silver_titer_non_negative(self):
        """Titer (product yield) must be non-negative after cleaning."""
        df = load_latest_parquet(SILVER_PATH)
        if "titer" in df.columns:
            negative = (df["titer"] < 0).sum()
            assert negative == 0, f"Silver has {negative} negative titer values"

    def test_silver_temperature_in_range(self):
        """Temperature must be within valid fermentation range."""
        df = load_latest_parquet(SILVER_PATH)
        if "temp" in df.columns:
            t_min, t_max = QUALITY_CHECKS["temp_valid_range"]
            out_of_range = ((df["temp"] < t_min) | (df["temp"] > t_max)).sum()
            assert (
                out_of_range == 0
            ), f"Silver has {out_of_range} temperature values outside [{t_min}, {t_max}]°C"

    def test_silver_yield_in_range(self):
        """Yield must be between 0 and 1."""
        df = load_latest_parquet(SILVER_PATH)
        if "yield" in df.columns:
            y_min, y_max = QUALITY_CHECKS["yield_valid_range"]
            clean_yield = df["yield"].dropna()
            bad = ((clean_yield < y_min) | (clean_yield > y_max)).sum()
            assert bad == 0, f"Silver has {bad} yield values outside [{y_min}, {y_max}]"

    def test_silver_has_outlier_flags(self):
        """Outlier detection columns should exist after Silver processing."""
        df = load_latest_parquet(SILVER_PATH)
        outlier_cols = [c for c in df.columns if c.endswith("_is_outlier")]
        assert len(outlier_cols) > 0, "Silver missing outlier flag columns"

    def test_silver_has_derived_features(self):
        """Derived features should be present."""
        df = load_latest_parquet(SILVER_PATH)
        # At least one derived feature should exist
        derived = [
            c
            for c in ["productivity", "yield_efficiency", "molecular_complexity"]
            if c in df.columns
        ]
        assert len(derived) > 0, "Silver missing derived feature columns"

    def test_silver_no_nulls_in_product_name(self):
        """product_name must not have nulls after Silver cleaning."""
        df = load_latest_parquet(SILVER_PATH)
        if "product_name" in df.columns:
            nulls = df["product_name"].isnull().sum()
            assert nulls == 0, f"Silver has {nulls} null product_name values"


# ─────────────────────────────────────────────
# GOLD TESTS
# ─────────────────────────────────────────────


class TestGoldLayer:

    def test_gold_has_fact_table(self):
        """Gold fact_fermentation table must exist and have data."""
        fact_files = list(GOLD_PATH.rglob("fact_fermentation.parquet"))
        assert len(fact_files) > 0, "Gold fact_fermentation table not found"
        df = pd.read_parquet(fact_files[-1])
        assert len(df) > 0, "Gold fact_fermentation table is empty"

    def test_gold_has_dimension_tables(self):
        """All three dimension tables must exist."""
        for table in ["dim_strain", "dim_process", "dim_product"]:
            files = list(GOLD_PATH.rglob(f"{table}.parquet"))
            assert len(files) > 0, f"Gold dimension table missing: {table}"

    def test_gold_insights_exist(self):
        """Insight tables must be generated."""
        for insight in ["insight_top_strains", "insight_reactor_yields"]:
            files = list(GOLD_PATH.rglob(f"{insight}.parquet"))
            assert len(files) > 0, f"Gold insight table missing: {insight}"

    def test_gold_fact_has_run_key(self):
        """Fact table must have a run_key primary key column."""
        fact_files = list(GOLD_PATH.rglob("fact_fermentation.parquet"))
        if not fact_files:
            pytest.skip("No fact table found")
        df = pd.read_parquet(fact_files[-1])
        assert "run_key" in df.columns, "Gold fact table missing run_key"
        assert df["run_key"].nunique() == len(df), "Gold fact run_key is not unique"

    def test_gold_output_charts_exist(self):
        """Key insight charts must be generated."""
        expected_charts = [
            "insight_top_strain_titer.png",
            "insight_correlation_heatmap.png",
        ]
        for chart in expected_charts:
            chart_path = Path("outputs") / chart
            assert chart_path.exists(), f"Missing output chart: {chart}"


# ─────────────────────────────────────────────
# PIPELINE INTEGRATION TEST
# ─────────────────────────────────────────────


class TestPipelineIntegration:

    def test_silver_row_count_leq_bronze(self):
        """Silver should not have MORE rows than Bronze (we may drop rows during cleaning)."""
        bronze_files = list(BRONZE_PATH.rglob("*.parquet"))
        silver_files = list(SILVER_PATH.rglob("*.parquet"))

        if not bronze_files or not silver_files:
            pytest.skip("Bronze or Silver data not available")

        bronze_count = sum(len(pd.read_parquet(f)) for f in bronze_files)
        silver_count = sum(len(pd.read_parquet(f)) for f in silver_files)

        assert (
            silver_count <= bronze_count
        ), f"Silver ({silver_count}) has more rows than Bronze ({bronze_count}) — something went wrong"

    def test_gold_titer_values_reasonable(self):
        """Gold titer values should be within a reasonable bioprocess range."""
        fact_files = list(GOLD_PATH.rglob("fact_fermentation.parquet"))
        if not fact_files:
            pytest.skip("No Gold fact table")

        df = pd.read_parquet(fact_files[-1])
        if "titer" in df.columns:
            max_titer = df["titer"].max()
            # Titer above 1000 g/L would be physically unrealistic for most bioprocesses
            assert max_titer < 1000, f"Gold titer max {max_titer:.2f} seems unrealistic"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
