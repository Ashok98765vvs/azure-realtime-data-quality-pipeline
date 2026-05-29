"""
Gold Layer - Anomaly Detection & Data Quality Scorecards
Azure Real-Time Data Quality & Anomaly Detection Pipeline

Detects price/volume anomalies using Z-score and IQR methods.
Generates DQ scorecards and writes Gold aggregations for Power BI.
"""

import uuid
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, avg, stddev, abs as spark_abs, lit, current_timestamp,
    percentile_approx, when, count, round as spark_round,
    to_date, date_format
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from utils.logger import get_logger
import yaml

logger = get_logger(__name__)

Z_SCORE_THRESHOLD  = 3.0
IQR_MULTIPLIER     = 1.5


# ─── Anomaly Detection ────────────────────────────────────────────────────

def detect_zscore_anomalies(df: DataFrame) -> DataFrame:
    """
    Detect anomalies using Z-score method.
    Records with |z_score| > Z_SCORE_THRESHOLD are flagged.
    """
    window = Window.partitionBy("ticker")

    df = df.withColumn("mean_close",   avg(col("close_price")).over(window)) \
           .withColumn("stddev_close", stddev(col("close_price")).over(window)) \
           .withColumn("mean_volume",  avg(col("volume").cast("double")).over(window)) \
           .withColumn("stddev_volume",stddev(col("volume").cast("double")).over(window))

    df = df.withColumn(
        "zscore_price",
        when(col("stddev_close") > 0,
             spark_abs((col("close_price") - col("mean_close")) / col("stddev_close"))
        ).otherwise(lit(0.0))
    ).withColumn(
        "zscore_volume",
        when(col("stddev_volume") > 0,
             spark_abs((col("volume").cast("double") - col("mean_volume")) / col("stddev_volume"))
        ).otherwise(lit(0.0))
    )

    return df.withColumn(
        "is_price_anomaly",  col("zscore_price")  > Z_SCORE_THRESHOLD
    ).withColumn(
        "is_volume_anomaly", col("zscore_volume") > Z_SCORE_THRESHOLD
    ).withColumn(
        "is_anomaly",
        col("is_price_anomaly") | col("is_volume_anomaly")
    ).withColumn(
        "anomaly_method", lit("Z-Score")
    )


def detect_iqr_anomalies(df: DataFrame) -> DataFrame:
    """
    Detect anomalies using IQR (Interquartile Range) method per ticker.
    Records outside [Q1 - 1.5*IQR, Q3 + 1.5*IQR] are flagged.
    """
    window = Window.partitionBy("ticker")

    df = df.withColumn("q1_price", percentile_approx(col("close_price"), 0.25).over(window)) \
           .withColumn("q3_price", percentile_approx(col("close_price"), 0.75).over(window))

    df = df.withColumn("iqr_price", col("q3_price") - col("q1_price")) \
           .withColumn("lower_bound", col("q1_price") - IQR_MULTIPLIER * col("iqr_price")) \
           .withColumn("upper_bound", col("q3_price") + IQR_MULTIPLIER * col("iqr_price"))

    return df.withColumn(
        "is_iqr_anomaly",
        (col("close_price") < col("lower_bound")) | (col("close_price") > col("upper_bound"))
    ).withColumn(
        "anomaly_method",
        when(col("is_anomaly") | col("is_iqr_anomaly"), lit("Z-Score + IQR")).otherwise(lit("None"))
    ).withColumn(
        "is_anomaly",
        col("is_anomaly") | col("is_iqr_anomaly")
    )


# ─── Scorecard Generation ───────────────────────────────────────────────────

def generate_dq_scorecard(df: DataFrame) -> DataFrame:
    """
    Generate daily Data Quality Scorecard per ticker and source.
    Aggregates anomaly rates, validity scores, and volumes.
    """
    return (
        df.groupBy(
            to_date(col("trade_date")).alias("report_date"),
            col("ticker"),
            col("source_system")
        ).agg(
            count("*").alias("total_records"),
            count(when(col("is_anomaly") == True,  1)).alias("anomaly_count"),
            count(when(col("is_anomaly") == False, 1)).alias("clean_count"),
            spark_round(avg(col("close_price")), 2).alias("avg_close_price"),
            spark_round(avg(col("volume").cast("double")), 0).alias("avg_volume"),
            spark_round(
                (count(when(col("is_anomaly") == False, 1)) / count("*")) * 100, 2
            ).alias("dq_score")
        ).withColumn("scorecard_generated_at", current_timestamp())
    )


# ─── Gold Write Logic ──────────────────────────────────────────────────────

def write_gold(df: DataFrame, path: str, partition_cols: list = None):
    """Write dataframe to Gold Delta layer."""
    writer = df.write.format("delta").mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(path)
    logger.info("Gold write complete: %s | Records: %d", path, df.count())


# ─── Main Pipeline ────────────────────────────────────────────────────────────

def run_gold_pipeline(config_path: str):
    """Main entry point for Gold anomaly detection pipeline."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    logger.info("Starting Gold pipeline")

    spark = SparkSession.builder.appName("GoldAnomalyDetection") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()

    silver_path    = config["storage"]["silver_path"]
    gold_path      = config["storage"]["gold_path"]
    scorecard_path = config["storage"]["scorecard_path"]

    # Read Silver
    silver_df = spark.read.format("delta").load(silver_path)
    logger.info("Silver records loaded: %d", silver_df.count())

    # Anomaly Detection
    anomaly_df = detect_zscore_anomalies(silver_df)
    anomaly_df = detect_iqr_anomalies(anomaly_df)

    anomaly_count = anomaly_df.filter(col("is_anomaly") == True).count()
    logger.info("Anomalies detected: %d", anomaly_count)

    # Generate Scorecards
    scorecard_df = generate_dq_scorecard(anomaly_df)

    # Write Gold
    write_gold(anomaly_df,   gold_path,      ["trade_date", "ticker"])
    write_gold(scorecard_df, scorecard_path, ["report_date"])

    logger.info("Gold pipeline complete.")
    spark.stop()
    return {"anomalies_detected": anomaly_count}


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Gold Layer Anomaly Detection")
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    print(run_gold_pipeline(args.config))
