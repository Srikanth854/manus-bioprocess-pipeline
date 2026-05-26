"""
schema.py
=========
SQLite table definitions for Bronze, Silver, and Gold layers.
Each layer has its own table — shows data transformation visually.
"""

# ─────────────────────────────────────────────
# BRONZE — raw landing, no transformation
# ─────────────────────────────────────────────
BRONZE_TABLE = """
CREATE TABLE IF NOT EXISTS bronze_fermentation (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id            TEXT,
    tank_id             TEXT,
    timestamp           TEXT,
    temperature_c       REAL,
    pressure_bar        REAL,
    ph_level            REAL,
    dissolved_o2        REAL,
    agitation_rpm       REAL,
    feed_rate_lph       REAL,
    product_name        TEXT,
    _source_system      TEXT,
    _ingestion_ts       TEXT,
    _pipeline_version   TEXT,
    _bronze_run_id      TEXT,
    _layer              TEXT DEFAULT 'bronze'
);
"""

# ─────────────────────────────────────────────
# SILVER — cleaned and validated
# ─────────────────────────────────────────────
SILVER_TABLE = """
CREATE TABLE IF NOT EXISTS silver_fermentation (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id                TEXT,
    tank_id                 TEXT,
    timestamp               TEXT,
    temperature_c           REAL,
    pressure_bar            REAL,
    ph_level                REAL,
    dissolved_o2            REAL,
    agitation_rpm           REAL,
    feed_rate_lph           REAL,
    product_name            TEXT,
    temp_valid              INTEGER,
    ph_valid                INTEGER,
    o2_valid                INTEGER,
    is_outlier              INTEGER,
    productivity_score      REAL,
    _silver_ts              TEXT,
    _layer                  TEXT DEFAULT 'silver'
);
"""

# ─────────────────────────────────────────────
# GOLD — analytics ready aggregations
# ─────────────────────────────────────────────
GOLD_TABLE = """
CREATE TABLE IF NOT EXISTS gold_fermentation_summary (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id                TEXT,
    tank_id                 TEXT,
    product_name            TEXT,
    avg_temperature         REAL,
    avg_pressure            REAL,
    avg_ph                  REAL,
    avg_dissolved_o2        REAL,
    avg_agitation           REAL,
    avg_feed_rate           REAL,
    total_readings          INTEGER,
    outlier_count           INTEGER,
    data_quality_score      REAL,
    first_reading_ts        TEXT,
    last_reading_ts         TEXT,
    _gold_ts                TEXT,
    _layer                  TEXT DEFAULT 'gold'
);
"""

# Pipeline run audit log
PIPELINE_LOG_TABLE = """
CREATE TABLE IF NOT EXISTS pipeline_run_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id          TEXT,
    layer           TEXT,
    rows_processed  INTEGER,
    status          TEXT,
    message         TEXT,
    ts              TEXT
);
"""

ALL_TABLES = [
    BRONZE_TABLE,
    SILVER_TABLE,
    GOLD_TABLE,
    PIPELINE_LOG_TABLE,
]
