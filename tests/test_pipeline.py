"""
Unit Tests for Azure Data Quality Pipeline
Tests Bronze ingestion, Silver validation, Gold anomaly detection.

Run with: python -m pytest tests/test_pipeline.py -v
"""

import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, LongType
)
from pyspark.sql.functions import col
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


# ─── Fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Create a local SparkSession for testing."""
    return (
        SparkSession.builder
        .appName("TestPipeline")
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(spark):
    """Create a sample DataFrame with known good and bad records."""
    schema = StructType([
        StructField("ticker",        StringType(), True),
        StructField("open_price",    DoubleType(), True),
        StructField("close_price",   DoubleType(), True),
        StructField("high_price",    DoubleType(), True),
        StructField("low_price",     DoubleType(), True),
        StructField("volume",        LongType(),   True),
        StructField("trade_date",    StringType(), True),
        StructField("source_system", StringType(), True),
    ])
    data = [
        # Valid records
        ("AAPL",  182.5, 185.2, 186.0, 181.9, 52341200, "2026-05-01", "nasdaq_feed"),
        ("MSFT",  415.3, 418.8, 420.1, 414.8, 24681500, "2026-05-01", "nasdaq_feed"),
        ("NVDA",  895.6, 902.1, 904.5, 893.2, 43219800, "2026-05-01", "nasdaq_feed"),
        # Invalid: negative price
        ("TSLA",  -50.0, 263.8, 265.0, 261.9, 92345600, "2026-05-07", "nasdaq_feed"),
        # Invalid: null ticker
        (None,    180.0, 182.0, 183.0, 179.0, 10000000, "2026-05-01", "nasdaq_feed"),
        # Invalid: high < low
        ("AMD",   162.3, 165.7, 160.0, 166.0, 38762100, "2026-05-01", "nasdaq_feed"),
        # Invalid: zero volume
        ("META",  512.4, 516.8, 517.5, 511.2, 0,        "2026-05-01", "nasdaq_feed"),
        # Anomaly: price spike
        ("TSLA",  252.7, 1250.0, 1251.0, 251.8, 98765400, "2026-05-05", "nasdaq_feed"),
    ]
    return spark.createDataFrame(data, schema)


# ─── Silver Layer Tests ───────────────────────────────────────────────────

class TestSilverDataQuality:
    """Tests for Silver layer data quality validation."""

    def test_apply_business_rules_marks_invalid_price(self, sample_df):
        from silver_data_quality import apply_business_rules
        result = apply_business_rules(sample_df)
        tsla_neg = result.filter(
            (col("ticker") == "TSLA") & (col("open_price") == -50.0)
        ).first()
        assert tsla_neg["rule_price_positive"] == False

    def test_apply_business_rules_marks_null_ticker(self, sample_df):
        from silver_data_quality import apply_business_rules
        result = apply_business_rules(sample_df)
        null_ticker = result.filter(col("ticker").isNull()).first()
        assert null_ticker["rule_no_null_ticker"] == False

    def test_apply_business_rules_marks_high_lt_low(self, sample_df):
        from silver_data_quality import apply_business_rules
        result = apply_business_rules(sample_df)
        amd_bad = result.filter(
            (col("ticker") == "AMD") & (col("low_price") > col("high_price"))
        ).first()
        assert amd_bad["rule_high_gte_low"] == False

    def test_apply_business_rules_marks_zero_volume(self, sample_df):
        from silver_data_quality import apply_business_rules
        result = apply_business_rules(sample_df)
        meta_zero = result.filter(
            (col("ticker") == "META") & (col("volume") == 0)
        ).first()
        assert meta_zero["rule_volume_positive"] == False

    def test_valid_records_pass_all_rules(self, sample_df):
        from silver_data_quality import apply_business_rules
        result = apply_business_rules(sample_df)
        aapl = result.filter(
            (col("ticker") == "AAPL") & (col("trade_date") == "2026-05-01")
        ).first()
        assert aapl["is_valid_record"] == True

    def test_dq_score_is_between_0_and_100(self, sample_df):
        from silver_data_quality import apply_business_rules, compute_dq_score
        validated = apply_business_rules(sample_df)
        score = compute_dq_score(validated)
        assert 0 <= score <= 100

    def test_deduplication_removes_exact_duplicates(self, spark):
        from silver_data_quality import deduplicate
        from pyspark.sql.functions import current_timestamp
        schema = StructType([
            StructField("ticker",               StringType(), True),
            StructField("trade_date",            StringType(), True),
            StructField("close_price",           DoubleType(), True),
            StructField("ingestion_timestamp",   StringType(), True),
        ])
        data = [
            ("AAPL", "2026-05-01", 185.2, "2026-05-01 10:00:00"),
            ("AAPL", "2026-05-01", 185.5, "2026-05-01 10:05:00"),  # duplicate date, newer
        ]
        df = spark.createDataFrame(data, schema)
        result = deduplicate(df)
        assert result.count() == 1
        assert result.first()["close_price"] == 185.5


# ─── Gold Layer Tests ─────────────────────────────────────────────────────

class TestGoldAnomalyDetection:
    """Tests for Gold layer anomaly detection."""

    def test_zscore_detects_price_spike(self, sample_df):
        from gold_anomaly_detection import detect_zscore_anomalies
        from silver_data_quality import apply_business_rules
        valid_df = apply_business_rules(sample_df).filter(col("is_valid_record") == True)
        result = detect_zscore_anomalies(valid_df)
        spike = result.filter(
            (col("ticker") == "TSLA") & (col("close_price") == 1250.0)
        ).first()
        assert spike is not None
        assert spike["is_price_anomaly"] == True

    def test_normal_records_are_not_anomalies(self, sample_df):
        from gold_anomaly_detection import detect_zscore_anomalies
        from silver_data_quality import apply_business_rules
        valid_df = apply_business_rules(sample_df).filter(col("is_valid_record") == True)
        result = detect_zscore_anomalies(valid_df)
        aapl = result.filter(
            (col("ticker") == "AAPL") & (col("close_price") == 185.2)
        ).first()
        assert aapl["is_anomaly"] == False

    def test_scorecard_generates_correct_columns(self, sample_df):
        from gold_anomaly_detection import detect_zscore_anomalies, generate_dq_scorecard
        from silver_data_quality import apply_business_rules
        valid_df = apply_business_rules(sample_df).filter(col("is_valid_record") == True)
        anomaly_df = detect_zscore_anomalies(valid_df)
        scorecard = generate_dq_scorecard(anomaly_df)
        expected_cols = [
            "report_date", "ticker", "source_system",
            "total_records", "anomaly_count", "clean_count", "dq_score"
        ]
        for c in expected_cols:
            assert c in scorecard.columns, f"Missing column: {c}"

    def test_dq_score_in_scorecard_is_valid(self, sample_df):
        from gold_anomaly_detection import detect_zscore_anomalies, generate_dq_scorecard
        from silver_data_quality import apply_business_rules
        valid_df = apply_business_rules(sample_df).filter(col("is_valid_record") == True)
        anomaly_df = detect_zscore_anomalies(valid_df)
        scorecard = generate_dq_scorecard(anomaly_df)
        scores = [row["dq_score"] for row in scorecard.collect()]
        for score in scores:
            assert 0 <= score <= 100


if __name__ == "__main__":
    pytest.main(["-v", "--tb=short", __file__])
