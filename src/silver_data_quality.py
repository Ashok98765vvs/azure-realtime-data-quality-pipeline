"""
Silver Layer - Data Quality & Validation
Azure Real-Time Data Quality & Anomaly Detection Pipeline

Applies schema enforcement, null checks, business rule validation,
deduplication, and data quality scoring. Failed records are quarantined.
"""

import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, count, lit, current_timestamp,
    row_number, round as spark_round, sum as spark_sum
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.logger import get_logger
import yaml

logger = get_logger(__name__)


# ─── Business Rule Validators ────────────────────────────────────────────────

def is_valid_ticker(df: DataFrame) -> DataFrame:
    """Flag records with invalid ticker symbols."""
    valid_tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "AMD"]
    return df.withColumn(
        "is_valid_ticker",
        col("ticker").isin(valid_tickers)
    )


def apply_business_rules(df: DataFrame) -> DataFrame:
    """Apply all business rule validations and return enriched dataframe."""
    return (
        df
        .withColumn("rule_price_positive",   (col("close_price") > 0) & (col("open_price") > 0))
        .withColumn("rule_volume_positive",   col("volume") > 0)
        .withColumn("rule_high_gte_low",      col("high_price") >= col("low_price"))
        .withColumn("rule_no_null_ticker",    col("ticker").isNotNull())
        .withColumn("rule_no_null_date",      col("trade_date").isNotNull())
        .withColumn(
            "is_valid_record",
            col("rule_price_positive") &
            col("rule_volume_positive") &
            col("rule_high_gte_low") &
            col("rule_no_null_ticker") &
            col("rule_no_null_date")
        )
    )


def compute_dq_score(df: DataFrame) -> float:
    """Compute overall data quality score (0-100) for the batch."""
    total = df.count()
    if total == 0:
        return 0.0
    valid = df.filter(col("is_valid_record") == True).count()
    score = round((valid / total) * 100, 2)
    logger.info("DQ Score: %.2f%% (%d / %d valid records)", score, valid, total)
    return score


def deduplicate(df: DataFrame) -> DataFrame:
    """Remove duplicate records using ticker + trade_date window."""
    window_spec = Window.partitionBy("ticker", "trade_date").orderBy(
        col("ingestion_timestamp").desc()
    )
    return (
        df.withColumn("row_num", row_number().over(window_spec))
          .filter(col("row_num") == 1)
          .drop("row_num")
    )


# ─── Silver Write Logic ───────────────────────────────────────────────────────

def write_quarantine(df: DataFrame, quarantine_path: str, batch_id: str):
    """Write invalid records to the quarantine Delta table."""
    failed_df = df.filter(col("is_valid_record") == False) \
                  .withColumn("quarantine_reason", lit("Business rule violation")) \
                  .withColumn("quarantine_timestamp", current_timestamp()) \
                  .withColumn("batch_id", lit(batch_id))

    count = failed_df.count()
    if count > 0:
        failed_df.write.format("delta").mode("append").save(quarantine_path)
        logger.warning("Quarantined %d failed records to: %s", count, quarantine_path)
    return count


def write_silver(df: DataFrame, silver_path: str):
    """Merge clean records into Silver Delta table (UPSERT)."""
    clean_df = df.filter(col("is_valid_record") == True)

    if DeltaTable.isDeltaTable(df.sparkSession, silver_path):
        silver_table = DeltaTable.forPath(df.sparkSession, silver_path)
        silver_table.alias("target").merge(
            clean_df.alias("source"),
            "target.ticker = source.ticker AND target.trade_date = source.trade_date"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        logger.info("Silver MERGE complete.")
    else:
        clean_df.write.format("delta").mode("overwrite") \
                .partitionBy("trade_date").save(silver_path)
        logger.info("Silver initial write complete.")

    return clean_df.count()


# ─── Main Pipeline ────────────────────────────────────────────────────────────

def run_silver_pipeline(config_path: str):
    """Main entry point for Silver data quality pipeline."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    batch_id = str(uuid.uuid4())
    logger.info("Starting Silver pipeline. Batch ID: %s", batch_id)

    spark = SparkSession.builder.appName("SilverDataQuality") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    bronze_path     = config["storage"]["bronze_path"]
    silver_path     = config["storage"]["silver_path"]
    quarantine_path = config["storage"]["quarantine_path"]

    # Read Bronze
    bronze_df = spark.read.format("delta").load(bronze_path)
    logger.info("Bronze records read: %d", bronze_df.count())

    # Validate & Score
    validated_df = apply_business_rules(bronze_df)
    validated_df = is_valid_ticker(validated_df)
    dq_score     = compute_dq_score(validated_df)

    # Deduplicate
    deduped_df = deduplicate(validated_df)

    # Write quarantine
    quarantined = write_quarantine(deduped_df, quarantine_path, batch_id)

    # Write silver
    silver_count = write_silver(deduped_df, silver_path)

    logger.info(
        "Silver complete. DQ Score: %.2f | Silver: %d | Quarantine: %d",
        dq_score, silver_count, quarantined
    )
    spark.stop()
    return {"dq_score": dq_score, "silver_records": silver_count, "quarantined": quarantined}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Silver Layer Data Quality")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    print(run_silver_pipeline(args.config))
