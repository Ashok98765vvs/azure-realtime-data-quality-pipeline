from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def get_spark(app_name: str = "gold_anomaly_detection") -> SparkSession:
    """
    Create or get a SparkSession with Delta enabled.
    """
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def read_silver_table(spark: SparkSession, silver_path: str) -> DataFrame:
    """
    Read the curated Silver-level data (clean records only) from Delta.
    Expected columns (example): symbol, event_time, price, volume, ...
    """
    return spark.read.format("delta").load(silver_path)


def detect_price_anomalies(df: DataFrame, z_threshold: float = 3.0) -> DataFrame:
    """
    Perform simple z-score based anomaly detection on price per symbol.
    For each symbol, compute mean/StdDev(price), then flag rows whose
    price is more than z_threshold standard deviations from the mean.

    z_score = (price - mean_price_symbol) / stddev_price_symbol
    """
    # Window per symbol
    w_symbol = Window.partitionBy("symbol")

    stats_df = (
        df.withColumn("mean_price", F.mean("price").over(w_symbol))
        .withColumn("std_price", F.stddev_pop("price").over(w_symbol))
    )

    # Avoid division by zero: if std is null/0, z_score will be null
    stats_df = stats_df.withColumn(
        "z_score",
        F.when(F.col("std_price").isNull() | (F.col("std_price") == 0), None).otherwise(
            (F.col("price") - F.col("mean_price")) / F.col("std_price")
        ),
    )

    result_df = (
        stats_df.withColumn(
            "is_anomaly",
            F.when(F.abs(F.col("z_score")) >= z_threshold, F.lit(True)).otherwise(
                F.lit(False)
            ),
        )
        .withColumn("anomaly_reason", F.when(F.col("is_anomaly"), F.lit("price_zscore")))
        .withColumn("anomaly_detected_ts", F.current_timestamp())
    )

    return result_df


def write_gold_anomalies(
    df: DataFrame,
    gold_path: str,
    mode: str = "overwrite",
):
    """
    Write anomalies (and contextual stats) to a Gold Delta table.
    In real-world usage, you may switch to 'append' and partition by date.
    """
    df.write.format("delta").mode(mode).save(gold_path)


def run_gold_anomaly_detection():
    """
    Entry point:
    - Read Silver table
    - Run anomaly detection
    - Persist to Gold table
    """
    import os

    spark = get_spark()

    silver_path = os.getenv(
        "SILVER_DELTA_PATH",
        "abfss://silver@<your-account>.dfs.core.windows.net/stock_events_silver",
    )
    gold_path = os.getenv(
        "GOLD_DELTA_PATH",
        "abfss://gold@<your-account>.dfs.core.windows.net/stock_price_anomalies",
    )
    z_threshold = float(os.getenv("PRICE_Z_THRESHOLD", "3.0"))

    silver_df = read_silver_table(spark, silver_path)
    anomalies_df = detect_price_anomalies(silver_df, z_threshold=z_threshold)

    # Persist full dataset with anomaly flag (you can also filter only anomalies)
    write_gold_anomalies(anomalies_df, gold_path=gold_path, mode="overwrite")

    spark.stop()


if __name__ == "__main__":
    run_gold_anomaly_detection()
