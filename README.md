# Manus Bioprocess Data Pipeline

A production-style data engineering pipeline built to demonstrate the data unification architecture proposed for Manus — bridging real-time DCS sensor streams, batch laboratory data, and external compound databases into a single analytics-ready platform.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                            │
│                                                             │
│  Kaggle Bioprocess CSV ──batch──────────────────────┐       │
│                                                     ▼       │
│  Simulated DCS Stream ──► Kafka Producer ──► Kafka Consumer │
│                                                     │       │
│  PubChem REST API ──────────────────────────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────┐
              │   BRONZE — Raw Landing    │
              │   Azure Data Lake / local │
              │   Parquet · metadata only │
              └───────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────┐
              │   SILVER — Cleaned Data   │
              │   Nulls · outliers · types│
              │   Derived features · QC   │
              └───────────────────────────┘
                              │
                              ▼
              ┌───────────────────────────┐
              │   GOLD — Analytics Ready  │
              │   Star schema · insights  │
              │   Predictive model · viz  │
              └───────────────────────────┘
```

---

## Key Features

| Feature | Implementation |
|---|---|
| Real-time ingestion | Apache Kafka (Confluent Cloud) |
| Batch ingestion | Python + Parquet |
| API enrichment | PubChem REST API |
| Data cleaning | Silver layer — nulls, outliers, timestamps |
| Analytics | Gold layer — star schema + ML model |
| Data quality | Automated checks at every layer |
| CI/CD | GitHub Actions — lint + test on every push |
| Version control | Git |

---

## Project Structure

```
manus-bioprocess-pipeline/
├── .github/workflows/ci.yml     # CI/CD — runs on every push
├── src/
│   ├── ingestion/
│   │   ├── kafka_producer.py    # Simulates DCS historian stream
│   │   ├── kafka_consumer.py    # Lands Kafka messages in Bronze
│   │   └── api_client.py        # PubChem compound enrichment
│   └── layers/
│       ├── bronze.py            # Raw landing + quality gate
│       ├── silver.py            # Cleaning + transformation
│       └── gold.py              # Star schema + insights
├── tests/
│   └── test_data_quality.py     # Pytest data quality tests
├── data/
│   ├── bronze/                  # Raw Parquet (partitioned by date)
│   ├── silver/                  # Cleaned Parquet
│   └── gold/                    # Analytics tables
├── outputs/                     # Insight charts (PNG)
├── config.py                    # All settings in one place
├── main.py                      # Pipeline orchestrator
└── requirements.txt
```

---

## Setup

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Download dataset
Download from Kaggle: [Bioprocess Performance Dataset](https://www.kaggle.com/datasets/cbeurrier/bioprocess-performance)

Place the file at the project root as `Oyetunde et al. data.xlsx`

### 3. Configure Kafka (optional — for streaming mode)
Set environment variables for Confluent Cloud:
```bash
export KAFKA_BOOTSTRAP_SERVERS="your-cluster.confluent.cloud:9092"
export KAFKA_SECURITY_PROTOCOL="SASL_SSL"
export KAFKA_API_KEY="your-api-key"
export KAFKA_API_SECRET="your-api-secret"
```

---

## Running the Pipeline

```bash
# Full pipeline (Kafka + API + batch)
python main.py

# Batch only (no Kafka required — good for local dev)
python main.py --mode batch --skip-kafka --skip-api

# Skip API enrichment only
python main.py --skip-api
```

---

## CI/CD

GitHub Actions runs automatically on every push to `main` or `develop`:

1. **Lint** — black, isort, flake8
2. **Pipeline test** — runs in batch mode, validates all layers
3. **Data quality tests** — pytest checks Bronze, Silver, Gold
4. **Schema validation** — verifies required columns at each layer

---

## Gold Layer Insights

The pipeline produces four key insights for Manus:

| Insight | File | Business Question |
|---|---|---|
| Top strain/gene combos | `insight_top_strain_titer.png` | Which genetic modification gives highest titer? |
| Reactor conditions heatmap | `insight_reactor_yield_heatmap.png` | Which temp + reactor type maximizes yield? |
| Fermentation time vs titer | `insight_fermentation_time_titer.png` | What is the optimal fermentation run length? |
| Feature importance | `insight_feature_importance.png` | What drives titer most — for the ML team? |

---

## Dataset

**Source:** Oyetunde et al. Bioprocess Performance Dataset  
**Via:** [Kaggle — cbeurrier/bioprocess-performance](https://www.kaggle.com/datasets/cbeurrier/bioprocess-performance)

Contains fermentation run data including strain genotypes, gene modifications, reactor conditions, and performance outcomes (titer, yield, growth rate) — directly analogous to the data Manus generates across its manufacturing operations.

---

## Production Extension

In a production Manus environment this pipeline would be extended with:

- **Azure Data Lake Storage** replacing local `data/` directory
- **Azure Databricks** running Silver and Gold PySpark jobs
- **Azure Data Factory** orchestrating the pipeline schedule
- **Real Kafka connectors** to DCS historians (OSIsoft PI, Honeywell)
- **Real LIMS integration** replacing the Kaggle batch source
- **Azure DevOps** replacing GitHub Actions for enterprise CI/CD
