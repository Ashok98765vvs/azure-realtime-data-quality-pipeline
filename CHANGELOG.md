# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.1.0] - 2026-06-17

### Added
- GitHub Actions CI/CD workflow (`.github/workflows/ci.yml`) with:
  - Automated unit testing via `pytest` on every push and pull request
  - Python syntax and style linting via `flake8`
  - Code formatting checks via `black` and `isort`
  - Pip dependency caching for faster CI runs
- `CHANGELOG.md` to track version history

### Changed
- Enhanced `README.md` with:
  - Improved shields.io badges with logos (Python, PySpark, Azure, GitHub Actions)
  - Added LinkedIn and GitHub profile badge links in Author section
  - Fixed author section: corrected degree to B.Tech @ Auburn University Montgomery
  - Added CI/CD badge and reference to GitHub Actions workflow
  - Added callout quote block highlighting project purpose
  - Added CHANGELOG reference section
  - Included `.github/workflows/ci.yml` in project structure tree

---

## [1.0.0] - 2026-05-25

### Added
- Initial production-grade project setup
- **Bronze Layer** (`src/bronze_ingestion.py`): Streaming ingestion from Azure Event Hub / Kafka into ADLS Gen2 Delta tables with metadata tagging
- **Silver Layer** (`src/silver_data_quality.py`): Schema enforcement, null checks, business rule validation, deduplication, and data quality scoring
- **Gold Layer** (`src/gold_anomaly_detection.py`): Z-score and IQR-based anomaly detection, rolling statistics with Spark Window functions, and quality scorecards
- Centralized logging utility (`src/utils/logger.py`) with correlation IDs
- Azure Data Factory pipeline configuration (`azure/adf_pipeline.json`)
- SQL-based data quality checks (`sql/quality_checks.sql`)
- Pipeline configuration via YAML (`config/pipeline_config.yaml`)
- Sample stock data for local testing (`data/sample_stock_data.csv`)
- Comprehensive unit test suite (`tests/test_pipeline.py`)
- Power BI dashboard specification (`powerbi/dashboard_spec.md`)
- `requirements.txt` with all dependencies
- MIT License
- Comprehensive README with architecture diagram, tech stack, setup instructions, and pipeline stage documentation
