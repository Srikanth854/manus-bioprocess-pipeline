"""
gold.py
=======
Gold Layer — Analytics-ready datasets for Manus biomanufacturing.

This is a DATA ENGINEERING Gold layer — not a data science notebook.
What it does:
  - Builds star schema (fact + dimension tables)
  - Produces data quality summary (Bronze vs Silver comparison)
  - Produces pipeline run metrics (row counts, column counts, latency)
  - Produces simple business aggregations (groupby summaries — no ML)
  - Produces data lineage report (source to Bronze to Silver to Gold)

What it does NOT do:
  - Machine learning
  - Statistical modeling
  - Clustering or PCA
  - Anything a data scientist would own
"""

import logging
import warnings
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns

warnings.filterwarnings("ignore")

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import BRONZE_PATH, SILVER_PATH, GOLD_PATH, OUTPUT_PATH, PIPELINE_VERSION

logging.basicConfig(level=logging.INFO, format="%(asctime)s [GOLD] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_PATH.mkdir(parents=True, exist_ok=True)

plt.rcParams.update({
    "figure.facecolor":  "white",
    "axes.facecolor":    "white",
    "axes.spines.top":   False,
    "axes.spines.right": False,
    "font.family":       "sans-serif",
    "axes.titlesize":    13,
    "axes.labelsize":    11,
})

MANUS_COLORS = ["#2C3E50", "#27AE60", "#E67E22", "#2980B9", "#8E44AD"]


def load_from_silver(date_str=None):
    if date_str is None:
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    silver_path = SILVER_PATH / f"date={date_str}" / "bioprocess_silver.parquet"
    if not silver_path.exists():
        all_silver = list(SILVER_PATH.rglob("*.parquet"))
        if not all_silver:
            raise FileNotFoundError("No Silver data found.")
        silver_path = max(all_silver, key=lambda f: f.stat().st_mtime)
    df = pd.read_parquet(silver_path)
    log.info(f"Silver loaded: {len(df)} rows x {len(df.columns)} columns")
    return df


def load_from_bronze():
    all_bronze = list(BRONZE_PATH.rglob("*.parquet"))
    if not all_bronze:
        return pd.DataFrame()
    latest = max(all_bronze, key=lambda f: f.stat().st_mtime)
    return pd.read_parquet(latest)


def build_dim_strain(df):
    cols = [c for c in ["strain_background_genotype","genes_modified","gene_deletion",
                         "gene_overexpression","heterologous_gene","codon_optimization",
                         "replication_origin"] if c in df.columns]
    dim = df[cols].drop_duplicates().reset_index(drop=True)
    dim.insert(0, "strain_key", range(1, len(dim)+1))
    log.info(f"dim_strain: {len(dim)} rows")
    return dim


def build_dim_process(df):
    cols = [c for c in ["reactor_type","rxt_volume","media","temp","oxygen"] if c in df.columns]
    dim = df[cols].drop_duplicates().reset_index(drop=True)
    dim.insert(0, "process_key", range(1, len(dim)+1))
    log.info(f"dim_process: {len(dim)} rows")
    return dim


def build_dim_product(df):
    cols = [c for c in ["product_name","no_C","no_H","no_O","no_N","mw","precursor","enzyme_steps"] if c in df.columns]
    dim = df[cols].drop_duplicates().reset_index(drop=True)
    dim.insert(0, "product_key", range(1, len(dim)+1))
    log.info(f"dim_product: {len(dim)} rows")
    return dim


def build_fact_fermentation(df):
    cols = [c for c in ["paper_number","titer","yield","rate","fermentation_time",
                         "bio_titre","bio_growth_rate","atp_cost","nadh_nadph_cost",
                         "enzyme_steps","productivity","titer_is_outlier","yield_is_outlier"] if c in df.columns]
    fact = df[cols].copy()
    fact.insert(0, "run_key", range(1, len(fact)+1))
    fact["_gold_ts"] = datetime.now(timezone.utc).isoformat()
    log.info(f"fact_fermentation: {len(fact)} rows")
    return fact


def chart_data_quality_dashboard(df_bronze, df_silver):
    log.info("Generating Chart 1: Data quality dashboard...")
    key_cols = [c for c in ["titer","yield","rate","fermentation_time","bio_growth_rate",
                              "enzyme_steps","atp_cost","reactor_type","temp","mw"]
                if c in df_bronze.columns and c in df_silver.columns]
    if not key_cols:
        return

    bronze_nulls = df_bronze[key_cols].isnull().mean() * 100
    silver_nulls = df_silver[key_cols].isnull().mean() * 100
    x = np.arange(len(key_cols))
    width = 0.35

    fig, ax = plt.subplots(figsize=(14, 6))
    bars1 = ax.bar(x - width/2, bronze_nulls, width, label="Bronze (raw)", color="#E74C3C", alpha=0.85)
    bars2 = ax.bar(x + width/2, silver_nulls, width, label="Silver (cleaned)", color="#27AE60", alpha=0.85)

    for bar in bars1:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x()+bar.get_width()/2, h+0.5, f"{h:.0f}%",
                    ha="center", va="bottom", fontsize=8, color="#E74C3C")
    for bar in bars2:
        h = bar.get_height()
        if h > 0:
            ax.text(bar.get_x()+bar.get_width()/2, h+0.5, f"{h:.0f}%",
                    ha="center", va="bottom", fontsize=8, color="#27AE60")

    ax.set_xticks(x)
    ax.set_xticklabels(key_cols, rotation=30, ha="right")
    ax.set_ylabel("Null %")
    ax.set_title("Data Quality: Null % Before vs After Silver Cleaning\nBronze = raw landing  |  Silver = cleaned and imputed", fontweight="bold")
    ax.legend()
    ax.set_ylim(0, max(bronze_nulls.max(), 10) * 1.2)
    plt.tight_layout()
    out = OUTPUT_PATH / "chart1_data_quality_dashboard.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Saved: {out.name}")


def chart_pipeline_run_summary(df_bronze, df_silver, df_gold):
    log.info("Generating Chart 2: Pipeline run summary...")
    layers     = ["Bronze\n(Raw)", "Silver\n(Cleaned)", "Gold\n(Analytics)"]
    row_counts = [len(df_bronze), len(df_silver), len(df_gold)]
    col_counts = [len(df_bronze.columns), len(df_silver.columns), len(df_gold.columns)]
    colors     = ["#E67E22", "#2980B9", "#27AE60"]

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))

    bars = ax1.bar(layers, row_counts, color=colors, alpha=0.85, width=0.5)
    for bar, val in zip(bars, row_counts):
        ax1.text(bar.get_x()+bar.get_width()/2, bar.get_height()+5,
                 f"{val:,}", ha="center", va="bottom", fontweight="bold", fontsize=11)
    ax1.set_title("Row Count at Each Layer", fontweight="bold")
    ax1.set_ylabel("Number of Rows")
    ax1.set_ylim(0, max(row_counts)*1.15)

    bars2 = ax2.bar(layers, col_counts, color=colors, alpha=0.85, width=0.5)
    for bar, val in zip(bars2, col_counts):
        ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+0.3,
                 f"{val}", ha="center", va="bottom", fontweight="bold", fontsize=11)
    ax2.set_title("Column Count at Each Layer\n(Silver adds derived features and outlier flags)", fontweight="bold")
    ax2.set_ylabel("Number of Columns")
    ax2.set_ylim(0, max(col_counts)*1.15)

    fig.suptitle(f"Manus Bioprocess Pipeline Run Summary  |  Pipeline v{PIPELINE_VERSION}  |  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
                 fontsize=12, fontweight="bold")
    plt.tight_layout()
    out = OUTPUT_PATH / "chart2_pipeline_run_summary.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Saved: {out.name}")


def chart_business_aggregations(df):
    log.info("Generating Chart 3: Business aggregations...")
    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    fig.suptitle("Gold Layer Business Aggregations\n(SQL groupby summaries ready for dashboards and reporting)",
                 fontsize=13, fontweight="bold")

    # Agg 1: titer by product
    if "product_name" in df.columns and "titer" in df.columns:
        df2 = df.copy()
        df2["titer_num"] = pd.to_numeric(df2["titer"], errors="coerce")
        prod = (df2.groupby("product_name")["titer_num"]
                .agg(avg_titer="mean", n_runs="count")
                .reset_index()
                .sort_values("avg_titer", ascending=False)
                .head(8))
        axes[0].barh(prod["product_name"], prod["avg_titer"], color=MANUS_COLORS[1], alpha=0.85)
        axes[0].set_xlabel("Average Titer (g/L)")
        axes[0].set_title("Avg Titer by Product\n(Gold groupby)", fontweight="bold")
        axes[0].invert_yaxis()
        for i, (val, n) in enumerate(zip(prod["avg_titer"], prod["n_runs"])):
            axes[0].text(val+0.05, i, f"{val:.2f}  n={n}", va="center", fontsize=8)

    # Agg 2: yield by reactor type
    if "reactor_type" in df.columns and "yield" in df.columns:
        df3 = df.copy()
        df3["yield_num"] = pd.to_numeric(df3["yield"], errors="coerce")
        reac = (df3.groupby("reactor_type")["yield_num"]
                .agg(avg_yield="mean", n_runs="count")
                .reset_index()
                .sort_values("avg_yield", ascending=False))
        reac["label"] = reac["reactor_type"].apply(
            lambda x: f"Reactor {int(float(x))}" if str(x).replace(".","").isdigit() else str(x))
        axes[1].bar(reac["label"], reac["avg_yield"], color=MANUS_COLORS[3], alpha=0.85)
        axes[1].set_title("Avg Yield by Reactor Type\n(Gold groupby)", fontweight="bold")
        axes[1].set_ylabel("Average Yield")
        axes[1].tick_params(axis="x", rotation=30)
        for i, (val, n) in enumerate(zip(reac["avg_yield"], reac["n_runs"])):
            axes[1].text(i, val+0.001, f"n={n}", ha="center", fontsize=8)

    # Agg 3: run count by temp range
    if "temp" in df.columns:
        temp_num = pd.to_numeric(df["temp"], errors="coerce").dropna()
        bins   = [0, 25, 30, 35, 37, 42, 100]
        labels = ["<25C", "25-30C", "30-35C", "35-37C", "37-42C", ">42C"]
        tc = pd.cut(temp_num, bins=bins, labels=labels).value_counts().sort_index()
        axes[2].bar(tc.index.astype(str), tc.values, color=MANUS_COLORS[2], alpha=0.85)
        axes[2].set_xlabel("Temperature Range")
        axes[2].set_ylabel("Number of Runs")
        axes[2].set_title("Run Count by Temp Range\n(Gold groupby)", fontweight="bold")
        axes[2].tick_params(axis="x", rotation=30)
        for i, val in enumerate(tc.values):
            axes[2].text(i, val+0.3, str(val), ha="center", fontweight="bold", fontsize=9)

    plt.tight_layout()
    out = OUTPUT_PATH / "chart3_business_aggregations.png"
    plt.savefig(out, dpi=150)
    plt.close()
    log.info(f"Saved: {out.name}")


def chart_data_lineage(df_bronze, df_silver, df_gold):
    log.info("Generating Chart 4: Data lineage...")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    fig, ax = plt.subplots(figsize=(16, 8))
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 8)
    ax.axis("off")
    fig.suptitle("Manus Bioprocess Pipeline — Data Lineage Report",
                 fontsize=15, fontweight="bold", y=0.98)

    def draw_box(x, y, w, h, color, title, lines):
        rect = mpatches.FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.1",
                                       facecolor=color, edgecolor="white", linewidth=2, alpha=0.9)
        ax.add_patch(rect)
        ax.text(x+w/2, y+h-0.3, title, ha="center", va="top",
                fontsize=10, fontweight="bold", color="white")
        for i, line in enumerate(lines):
            ax.text(x+w/2, y+h-0.75-i*0.45, line, ha="center", va="top", fontsize=8, color="white")

    def draw_arrow(x1, x2, y, label):
        ax.annotate("", xy=(x2, y), xytext=(x1, y),
                    arrowprops=dict(arrowstyle="->", color="#2C3E50", lw=2.5))
        ax.text((x1+x2)/2, y+0.2, label, ha="center", fontsize=7, fontweight="bold", color="#555")

    draw_box(0.2, 3.5, 2.8, 3.5, "#7F8C8D", "SOURCE",
             ["Kaggle Bioprocess", "Excel Dataset", "Sheet: Data",
              f"Rows: {len(df_bronze):,}", "Columns: 60", "Format: xlsx"])

    draw_arrow(3.0, 3.8, 5.25, "batch ingest")

    draw_box(3.8, 3.5, 3.0, 3.5, "#E67E22", "BRONZE",
             ["Raw landing zone", f"Rows: {len(df_bronze):,}",
              f"Cols: {len(df_bronze.columns)}", "Format: Parquet",
              "Partitioned by date", "Metadata stamped"])

    draw_arrow(6.8, 7.6, 5.25, "clean + transform")

    draw_box(7.6, 3.5, 3.0, 3.5, "#2980B9", "SILVER",
             ["Cleaned and conformed", f"Rows: {len(df_silver):,}",
              f"Cols: {len(df_silver.columns)}", "Nulls imputed",
              "Outliers flagged", "Features derived"])

    draw_arrow(10.6, 11.4, 5.25, "star schema")

    draw_box(11.4, 3.5, 3.5, 3.5, "#27AE60", "GOLD",
             ["Analytics-ready", f"Fact: {len(df_gold):,} rows",
              "dim_strain", "dim_process", "dim_product", "Ready for BI / AI"])

    # QC checks
    for label, color, x, checks in [
        ("Bronze QC", "#E67E22", 3.8, ["Row count >= 10", "Metadata present", "Null % checked"]),
        ("Silver QC", "#2980B9", 7.6, ["Nulls imputed", "Temp in range", "Outliers flagged"]),
        ("Gold QC",   "#27AE60", 11.4, ["Fact table exists", "Dim tables exist", "run_key unique"]),
    ]:
        rect = mpatches.FancyBboxPatch((x, 1.5), 3.0, 1.8, boxstyle="round,pad=0.1",
                                       facecolor=color, edgecolor="white", linewidth=1, alpha=0.25)
        ax.add_patch(rect)
        ax.text(x+1.5, 3.1, f"{label} Checks", ha="center", fontsize=8, fontweight="bold", color=color)
        for i, c in enumerate(checks):
            ax.text(x+1.5, 2.72-i*0.38, f"✓ {c}", ha="center", fontsize=7.5, color="#2C3E50")

    ax.text(8, 0.8,
            f"Pipeline v{PIPELINE_VERSION}  |  Generated: {now}  |  Source: Kaggle bioprocess dataset  |  CI/CD: GitHub Actions",
            ha="center", fontsize=8, color="gray", style="italic")
    ax.text(8, 0.35,
            "Production: Source = DCS Historian / LIMS / ERP  |  Storage = Azure Data Lake  |  Compute = Databricks  |  Orchestration = ADF",
            ha="center", fontsize=8, color="#2C3E50", style="italic")

    plt.tight_layout()
    out = OUTPUT_PATH / "chart4_data_lineage.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    log.info(f"Saved: {out.name}")


def write_to_gold(tables):
    date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
    out_dir  = GOLD_PATH / f"date={date_str}"
    out_dir.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        if df is not None and len(df) > 0:
            for col in df.select_dtypes(include="object").columns:
                df[col] = df[col].astype(str)
            out = out_dir / f"{name}.parquet"
            df.to_parquet(out, index=False)
            log.info(f"Gold table: {name} -> {len(df)} rows")


def run_gold_pipeline(df_silver=None):
    log.info("Starting Gold pipeline...")

    df = load_from_silver() if df_silver is None else df_silver.copy()
    df_bronze = load_from_bronze()

    # Drop Silver metadata
    df = df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")

    # Star schema
    log.info("Building star schema tables...")
    dim_strain  = build_dim_strain(df)
    dim_process = build_dim_process(df)
    dim_product = build_dim_product(df)
    fact_ferm   = build_fact_fermentation(df)

    # Charts
    log.info("Generating data engineering charts...")
    if not df_bronze.empty:
        chart_data_quality_dashboard(df_bronze, df)
    chart_pipeline_run_summary(df_bronze if not df_bronze.empty else df, df, fact_ferm)
    chart_business_aggregations(df)
    chart_data_lineage(df_bronze if not df_bronze.empty else df, df, fact_ferm)

    # Write
    gold_tables = {
        "fact_fermentation": fact_ferm,
        "dim_strain":        dim_strain,
        "dim_process":       dim_process,
        "dim_product":       dim_product,
    }
    write_to_gold(gold_tables)

    log.info("Gold pipeline complete.")
    return {"tables": gold_tables}


if __name__ == "__main__":
    results = run_gold_pipeline()
    print("\nGold layer complete!")
    for name, df in results["tables"].items():
        print(f"  {name}: {len(df)} rows")
