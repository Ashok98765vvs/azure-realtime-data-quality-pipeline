"""
Bronze Layer - Raw Data Ingestion
Azure Real-Time Data Quality & Anomaly Detection Pipeline

Ingests raw streaming data from Azure Event Hub / Kafka
and writes to ADLS Gen2 in Delta format (Bronze layer).
"""

import argparse
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    LongType, TimestampType
)
from utils.logger import get_logger
import yaml

logger = get_logger(__name__)


RAW_SCHEMA = StructType([
    StructField("ticker",        StringType(),    True),
    StructField("open_price",    DoubleType(),    True),
    StructField("close_price",   DoubleType(),    True),
    StructField("high_price",    DoubleType(),    True),
    StructField("low_price",     DoubleType(),    True),
    StructField("volume",        LongType(),      True),
    StructField("trade_date",    StringType(),    True),
    StructField("source_system", StringType(),    True),
])


def get_spark_session(app_name: str = "BronzeIngestion") -> SparkSession:
    """Initialize a Spark session with Delta Lake support."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .getOrCreate()
    )


def read_from_event_hub(spark: SparkSession, config: dict):
    """Read streaming data from Azure Event Hub."""
    eh_config = config["event_hub"]
    connection_string = eh_config["connection_string"]
    eh_conf = {
        "eventhubs.connectionString": spark._jvm.org.apache.spark.eventhubs.EventHubsUtils
            .encrypt(connection_string),
        "eventhubs.consumerGroup": eh_config.get("consumer_group", "$Default"),
        "eventhubs.startingPosition": '{"offset":"-1","seqNo":-1,"enqueuedTime":null,"isInclusive":true}',
    }
    logger.info("Connecting to Event Hub: %s", eh_config["name"])
    return (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
    )


def read_from_csv(spark: SparkSession, path: str):
    """Read batch data from CSV (for local testing)."""
    logger.info("Reading CSV from: %s", path)
    return spark.read.schema(RAW_SCHEMA).option("header", True).csv(path)


def add_bronze_metadata(df, batch_id: str):
    """Add ingestion metadata columns to the dataframe."""
    return df.withColumn("ingestion_timestamp", current_timestamp()) \
             .withColumn("batch_id", lit(batch_id)) \
             .withColumn("pipeline_version", lit("1.0.0")) \
             .withColumn("trade_date_ts", to_timestamp(col("trade_date"), "yyyy-MM-dd"))


def write_to_bronze(df, config: dict, batch_id: str):
    """Write enriched dataframe to Bronze Delta table in ADLS Gen2."""
    bronze_path = config["storage"]["bronze_path"]
    logger.info("Writing to Bronze layer: %s", bronze_path)

    enriched_df = add_bronze_metadata(df, batch_id)

    (
        enriched_df.write
        .format("delta")
        .mode("append")
        .partitionBy("trade_date", "source_system")
        .save(bronze_path)
    )
    logger.info("Bronze write complete. Batch ID: %s", batch_id)
    return enriched_df.count()


def run_bronze_pipeline(config_path: str):
    """Main entry point for Bronze ingestion pipeline."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    batch_id = str(uuid.uuid4())
    logger.info("Starting Bronze ingestion. Batch ID: %s", batch_id)

    spark = get_spark_session()

    # Use CSV for local testing; Event Hub for production
    source_mode = config.get("source_mode", "csv")
    if source_mode == "eventhub":
        raw_df = read_from_event_hub(spark, config)
    else:
        raw_df = read_from_csv(spark, config["storage"]["sample_data_path"])

    record_count = write_to_bronze(raw_df, config, batch_id)
    logger.info("Bronze ingestion complete. Records written: %d", record_count)

    spark.stop()
    return {"batch_id": batch_id, "records_written": record_count}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion")
    parser.add_argument("--config", required=True, help="Path to pipeline_config.yaml")
    args = parser.parse_args()
    result = run_bronze_pipeline(args.config)
    print(f"Pipeline result: {result}")
