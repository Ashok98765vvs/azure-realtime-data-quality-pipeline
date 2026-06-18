"""
Silver Layer - Data Quality & Validation
Azure Real-Time Data Quality & Anomaly Detection Pipeline

Applies schema enforcement, null checks, business rule validation,
deduplication, and data quality scoring. Failed records are quarantined.
Also computes basic profiling metrics for observability.
"""

import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    when,
    isnan,
    isnull,
    lit,
    current_timestamp,
    row_number,
    round as spark_round,
    sum as spark_sum,
    count as spark_count,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.logger import get_logger
import yaml

logger = get_logger(__name__)


# ─── Business Rule Validators ────────────────────────────────────────────────


def is_valid_ticker(df: DataFrame) -> DataFrame:
    """
    Flag records with invalid ticker symbols.
    In real projects this would come from a reference table.
    """
    valid_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AMD"]

    return df.withColumn(
        "is_valid_ticker",
        col("ticker").isin(valid_tickers),
    )


def apply_business_rules(df: DataFrame) -> DataFrame:
    """
    Apply all business rule validations and return enriched dataframe.
    """
    return (
        df.withColumn(
            "rule_price_positive",
            (col("close_price") > 0) & (col("open_price") > 0),
        )
        .withColumn("rule_volume_positive", col("volume") > 0)
        .withColumn("rule_high_gte_low", col("high_price") >= col("low_price"))
        .withColumn("rule_no_null_ticker", col("ticker").isNotNull())
        .withColumn("rule_no_null_date", col("trade_date").isNotNull())
        .withColumn(
            "is_valid_record",
            col("rule_price_positive")
            & col("rule_volume_positive")
            & col("rule_high_gte_low")
            & col("rule_no_null_ticker")
            & col("rule_no_null_date")
            & col("is_valid_ticker"),
        )
    )


def compute_dq_score(df: DataFrame) -> float:
    """
    Compute overall data quality score (0-100) for the batch.
    """
    total = df.count()
    if total == 0:
        logger.warning("DQ Score: no records in batch.")
        return 0.0

    valid = df.filter(col("is_valid_record") == True).count()
    score = round((valid / total) * 100, 2)
    logger.info("DQ Score: %.2f%% (%d / %d valid records)", score, valid, total)
    return score


def compute_null_profile(df: DataFrame) -> DataFrame:
    """
    Basic null / NaN profiling per column to help debug data issues.
    Returns a small aggregated DataFrame that can be written to a metrics table.
    """
    total = df.count()
    if total == 0:
        return df.sparkSession.createDataFrame(
            [],
            schema="column_name string, null_count long, null_pct double",
        )

    cols = ["ticker", "trade_date", "open_price", "close_price", "high_price", "low_price", "volume"]

    agg_exprs = []
    for c in cols:
        agg_exprs.append(
            spark_sum(
                when(isnull(col(c)) | isnan(col(c)), 1).otherwise(0)
            ).alias(f"{c}_nulls")
        )

    agg_df = df.agg(*agg_exprs)

    rows = []
    for c in cols:
        null_col = f"{c}_nulls"
        rows.append(
            (c, agg_df.first()[null_col], round(agg_df.first()[null_col] / total * 100, 2))
        )

    return df.sparkSession.createDataFrame(
        rows,
        schema="column_name string, null_count long, null_pct double",
    )


def deduplicate(df: DataFrame) -> DataFrame:
    """
    Remove duplicate records using ticker + trade_date window.
    Keeps the latest record by ingestion_timestamp.
    """
    window_spec = Window.partitionBy("ticker", "trade_date").orderBy(
        col("ingestion_timestamp").desc()
    )

    return (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )


# ─── Silver Write Logic ───────────────────────────────────────────────────────


def write_quarantine(df: DataFrame, quarantine_path: str, batch_id: str) -> int:
    """
    Write invalid records to the quarantine Delta table.
    """
    failed_df = (
        df.filter(col("is_valid_record") == False)
        .withColumn("quarantine_reason", lit("Business rule violation"))
        .withColumn("quarantine_timestamp", current_timestamp())
        .withColumn("batch_id", lit(batch_id))
    )

    count = failed_df.count()
    if count > 0:
        failed_df.write.format("delta").mode("append").save(quarantine_path)
        logger.warning(
            "Quarantined %d failed records to: %s", count, quarantine_path
        )
    else:
        logger.info("No records quarantined for batch %s", batch_id)

    return count


def write_silver(df: DataFrame, silver_path: str) 
