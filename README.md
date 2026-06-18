# 🔵 Azure Real-Time Data Quality & Anomaly Detection Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue?logo=python&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3.4-orange?logo=apachespark&logoColor=white)
![Azure](https://img.shields.io/badge/Azure-Synapse%20%7C%20ADF%20%7C%20ADLS-0078D4?logo=microsoftazure&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Medallion-green)
![GitHub Actions](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-2088FF?logo=githubactions&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-yellow)

A **production-grade, end-to-end data quality and anomaly detection pipeline** built on the Azure ecosystem using PySpark, Delta Lake (Medallion Architecture), Azure Data Factory, Azure Synapse Analytics, and Power BI.

> 🚀 Built to demonstrate real-world Azure Data Engineering skills — from streaming ingestion to automated anomaly detection and Power BI dashboarding.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         AZURE DATA PLATFORM                             │
│                                                                         │
│              [Event Hub / Kafka]                                        │
│                      │                                                  │
│                      ▼                                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐                  │
│  │   BRONZE    │─▶│    SILVER    │─▶│     GOLD     │                  │
│  │  Raw Ingest │  │ Validated +  │  │  Aggregated  │                  │
│  │ (ADLS Gen2) │  │  Cleansed    │  │   Anomaly    │                  │
│  └─────────────┘  │  (Delta)     │  │  Scorecard   │                  │
│                   └──────────────┘  └──────┬───────┘                  │
│                                            │                           │
│                                            ▼                           │
│                               [Power BI Dashboard]                     │
│                               [Azure Synapse SQL]                      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Project Structure

```
azure-realtime-data-quality-pipeline/
│
├── src/
│   ├── bronze_ingestion.py          # Raw data ingestion to Bronze layer
│   ├── silver_data_quality.py       # Data validation & cleansing (Silver)
│   ├── gold_anomaly_detection.py    # Anomaly detection & scorecards (Gold)
│   └── utils/
│       └── logger.py                # Centralized logging utility
│
├── sql/
│   └── quality_checks.sql          # SQL-based data quality checks
│
├── azure/
│   └── adf_pipeline.json           # Azure Data Factory pipeline config
│
├── data/
│   └── sample_stock_data.csv       # Sample input data for testing
│
├── tests/
│   └── test_pipeline.py            # Unit tests for pipeline components
│
├── config/
│   └── pipeline_config.yaml        # Pipeline configuration parameters
│
├── .github/
│   └── workflows/
│       └── ci.yml                  # GitHub Actions CI/CD workflow
│
├── CHANGELOG.md
├── requirements.txt
├── .gitignore
├── LICENSE
└── README.md
```

---

## 🚀 Tech Stack

| Layer | Technology |
|---|---|
| Ingestion | Azure Event Hub / Apache Kafka |
| Storage | Azure Data Lake Storage Gen2 |
| Processing | PySpark 3.4, Azure Databricks / Synapse Spark |
| Table Format | Delta Lake (Bronze / Silver / Gold) |
| Orchestration | Azure Data Factory (ADF) |
| SQL Analytics | Azure Synapse Analytics |
| Visualization | Power BI |
| CI/CD | GitHub Actions |
| Language | Python 3.10, Spark SQL |

---

## ⚙️ Pipeline Stages

### 🥉 Bronze Layer — Raw Ingestion
- Reads streaming data from Event Hub / Kafka topic
- Lands raw records as-is in Delta format in ADLS Gen2
- Adds ingestion metadata: `ingestion_timestamp`, `source_system`, `batch_id`
- No transformations — full fidelity of source data

### 🥈 Silver Layer — Data Quality & Validation
- Applies **schema enforcement** and **null checks**
- Validates business rules (price > 0, volume > 0, valid ticker symbols)
- Quarantines failed records into a `_quarantine` Delta table
- Computes **Data Quality Score** per batch (0–100)
- Deduplicates records using window functions

### 🥇 Gold Layer — Anomaly Detection & Scorecards
- Uses **Z-score** and **IQR-based** anomaly detection on price/volume
- Computes rolling averages and standard deviations with Spark Window functions
- Generates **Data Quality Scorecards** aggregated by ticker, date, and source
- Writes final aggregated tables for Power BI consumption

---

## 📊 Data Quality Metrics

| Metric | Description |
|---|---|
| Completeness Score | % of non-null fields per record |
| Validity Score | % of records passing business rules |
| Anomaly Rate | % of records flagged as anomalies |
| Duplicate Rate | % of duplicate records detected |
| Overall DQ Score | Weighted average of all metrics (0–100) |

---

## 🛠️ Setup & Installation

### Prerequisites
- Python 3.10+
- Apache Spark 3.4+ / Azure Databricks
- Azure Subscription (ADLS Gen2, ADF, Synapse)
- Delta Lake library

### Local Setup

```bash
# Clone the repository
git clone https://github.com/Ashok98765vvs/azure-realtime-data-quality-pipeline.git
cd azure-realtime-data-quality-pipeline

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Run tests
python -m pytest tests/ -v

# Run bronze ingestion locally
python src/bronze_ingestion.py --config config/pipeline_config.yaml
```

### Azure Deployment

```bash
# Deploy ADF pipeline
az datafactory pipeline create \
  --resource-group <your-resource-group> \
  --factory-name <your-adf-name> \
  --name DataQualityPipeline \
  --pipeline @azure/adf_pipeline.json
```

---

## 🧪 Running Tests

```bash
python -m pytest tests/test_pipeline.py -v --tb=short
```

---

## 📈 Power BI Dashboard

The Gold layer tables power a real-time Power BI dashboard showing:
- **Data Quality Score Trend** (by day, ticker, source)
- **Anomaly Heatmap** (price vs volume outliers)
- **Quarantine Rate by Source**
- **Pipeline SLA Compliance**

See `powerbi/dashboard_spec.md` for detailed layout and DAX measures.

---

## 🏆 Key Features

- **Medallion Architecture** (Bronze → Silver → Gold) for clean data lineage
- **Automated anomaly detection** using Z-score and IQR methods
- **Data quarantine system** for failed records with full audit trail
- **Configurable quality thresholds** via YAML config
- **Idempotent pipeline runs** using Delta Lake MERGE operations
- **Structured logging** with correlation IDs for debugging
- **Unit tested** pipeline components with PySpark test utilities
- **CI/CD pipeline** via GitHub Actions for automated testing on every push

---

## 📄 Changelog

See [CHANGELOG.md](CHANGELOG.md) for version history and recent updates.

---

## 👤 Author

**Ashok Chowdary** — Data Engineer | B.Tech @ Auburn University Montgomery

Stack: Python | PySpark | Azure Synapse | Delta Lake | ADF | Power BI

[![GitHub](https://img.shields.io/badge/GitHub-Ashok98765vvs-181717?logo=github)](https://github.com/Ashok98765vvs)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?logo=linkedin)](https://www.linkedin.com/in/ashok-chowdary-vvs)

---

## 📄 License

MIT License — see [LICENSE](LICENSE) for details.
