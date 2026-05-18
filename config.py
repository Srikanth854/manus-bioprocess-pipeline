"""
config.py
=========
Central configuration for the Manus Bioprocess Pipeline.
All Kafka, path, and API settings live here.
"""

import os
from pathlib import Path

# ─────────────────────────────────────────────
# PROJECT PATHS
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

BRONZE_PATH = DATA_DIR / "bronze"
SILVER_PATH = DATA_DIR / "silver"
GOLD_PATH   = DATA_DIR / "gold"
OUTPUT_PATH = BASE_DIR / "outputs"

# Raw dataset (Kaggle download)
RAW_DATA_PATH = BASE_DIR / "Oyetunde et al. data.xlsx"

# ─────────────────────────────────────────────
# KAFKA SETTINGS (Confluent Cloud)
# ─────────────────────────────────────────────
# Set these as environment variables or fill in directly for local dev
KAFKA_CONFIG = {
    "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    "security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT"),
    # Confluent Cloud — uncomment and set env vars when using cloud
    # "sasl.mechanisms":       "PLAIN",
    # "sasl.username":         os.getenv("KAFKA_API_KEY", ""),
    # "sasl.password":         os.getenv("KAFKA_API_SECRET", ""),
}

KAFKA_TOPIC          = "bioprocess-sensor-stream"
KAFKA_CONSUMER_GROUP = "manus-bronze-consumer"
KAFKA_STREAM_DELAY   = 0.1   # seconds between messages (simulates live sensor rate)

# ─────────────────────────────────────────────
# API SETTINGS
# ─────────────────────────────────────────────
PUBCHEM_BASE_URL = "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name"
API_TIMEOUT      = 10   # seconds
API_RETRY_LIMIT  = 3

# ─────────────────────────────────────────────
# DATA QUALITY THRESHOLDS
# ─────────────────────────────────────────────
QUALITY_CHECKS = {
    "max_null_pct":       0.95,   # fail if >40% nulls in any column
    "min_row_count":      10,     # fail if fewer than 10 rows land in Bronze
    "temp_valid_range":   (0, 60),    # °C — valid fermentation temperature
    "ph_valid_range":     (0, 14),
    "yield_valid_range":  (0, 10),
    "titer_min":          0,
}

# ─────────────────────────────────────────────
# PIPELINE METADATA
# ─────────────────────────────────────────────
PIPELINE_VERSION = "1.0.0"
PIPELINE_NAME    = "manus-bioprocess-pipeline"
