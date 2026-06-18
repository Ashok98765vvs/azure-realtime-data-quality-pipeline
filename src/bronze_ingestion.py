from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


def get_spark(app_name: str = "bronze_ingestion") -> SparkSession:
    """
    Create or get an existing SparkSession with basic configs for Delta.
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


def get_bronze_schema() -> StructType:
    """
    Define the schema for raw stock market events landing in the Bronze layer.
    Adjust fields if your Event Hub payload changes.
    """
    return StructType(
        [
            StructField("symbol", StringType(), nullable=False),
            StructField("event_time", TimestampType(), nullable=False),
            StructField("price", DoubleType(), nullable=True),
            StructField("volume", DoubleType(), nullable=True),
            StructField("source_system", StringType(), nullable=True),
        ]
    )


def read_from_eventhub(spark: SparkSession, eh_connection_string: str) -> DataFrame:
    """
    Read streaming data from Azure Event Hubs into a DataFrame
    with the expected Bronze schema.
    """
    raw_df = (
        spark.readStream.format("eventhubs")
        .option("eventhubs.connectionString", eh_connection_string)
        .load()
    )

    # Event Hubs body is in 'body' column as binary; cast/parse as needed.
    # Here we assume JSON strings in 'body'.
    from pyspark.sql.functions import from_json

    schema = get_bronze_schema()

    parsed_df = (
        raw_df.selectExpr("cast(body as string) as json_payload")
        .select(from_json(col("json_payload"), schema).alias("data"))
        .select("data.*")
    )

    return parsed_df


def enrich_bronze(df: DataFrame, run_id: str) -> DataFrame:
    """
    Add standard Bronze metadata columns for lineage and observability.
    """
    return (
        df.withColumn("ingestion_ts", current_timestamp())
        .withColumn("run_id", lit(run_id))
        .withColumn("record_quality", lit("raw"))
    )


def write_to_delta_bronze(
    df: DataFrame,
    output_path: str,
    checkpoint_path: str,
    trigger_interval: str = "30 seconds",
):
    """
    Write the streaming Bronze DataFrame to Delta with exactly-once semantics.
    """
    (
        df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime=trigger_interval)
        .start(output_path)
    )


def run_bronze_ingestion():
    """
    Entry point for Bronze ingestion.
    Reads from Event Hub, enriches with metadata, and writes to Delta.
    """
    import os
    import uuid
    from utils.logger import get_logger  # keep logging consistent with Silver/Gold

    logger = get_logger(__name__)

    spark = get_spark()

    eh_connection_string = os.getenv("EVENT_HUB_CONN_STRING")
    bronze_path = os.getenv(
        "BRONZE_DELTA_PATH",
        "abfss://bronze@<your-account>.dfs.core.windows.net/stock_events",
    )
    checkpoint_path = os.getenv(
        "BRONZE_CHECKPOINT_PATH",
        "abfss://bronze@<your-account>.dfs.core.windows.net/checkpoints/bronze_ingestion",
    )

    if not eh_connection_string:
        raise ValueError("EVENT_HUB_CONN_STRING environment variable is not set")

    run_id = str(uuid.uuid4())
    logger.info("Starting Bronze ingestion. run_id=%s", run_id)
    logger.info("Bronze path: %s | Checkpoint: %s", bronze_path, checkpoint_path)

    raw_df = read_from_eventhub(spark, eh_connection_string)
    bronze_df = enrich_bronze(raw_df, run_id)

    write_to_delta_bronze(
        bronze_df,
        output_path=bronze_path,
        checkpoint_path=checkpoint_path,
    )

    logger.info("Bronze ingestion streaming query started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_bronze_ingestion()
