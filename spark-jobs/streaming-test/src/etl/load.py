"""Load transformed data into the Iceberg Bronze table."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

# DDL for the streaming-test Bronze table.
# Partitioned by ingestion_date to keep file sizes manageable.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
    kafka_topic         STRING          COMMENT 'Source Kafka topic',
    kafka_partition     INT             COMMENT 'Kafka partition',
    kafka_offset        LONG            COMMENT 'Kafka offset',
    kafka_timestamp     TIMESTAMP       COMMENT 'Kafka message timestamp (event time)',
    kafka_key           STRING          COMMENT 'Kafka message key (may be null)',
    raw_value           STRING          COMMENT 'Raw Kafka message value as UTF-8 string',
    raw_value_bytes_len INT             COMMENT 'Byte length of the raw Kafka value',
    bronze_ingestion_ts TIMESTAMP       COMMENT 'Wall-clock time when record landed in Bronze',
    ingestion_date      DATE            COMMENT 'Date partition key (from bronze_ingestion_ts)'
)
USING iceberg
PARTITIONED BY (ingestion_date)
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max' = '10'
)
"""


def ensure_table_exists(
    spark: SparkSession, catalog: str, schema: str, table: str
) -> None:
    """
    Create the namespace and table if they do not already exist.

    Args:
        spark: Active SparkSession.
        catalog: Iceberg catalog name (e.g. 'lakekeeper').
        schema: Iceberg namespace/schema (e.g. 'bronze').
        table: Iceberg table name (e.g. 'streaming_test_raw').
    """
    logger.info("[ICEBERG] Ensuring namespace %s.%s exists...", catalog, schema)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{schema}")
    logger.info("[ICEBERG] Namespace %s.%s OK.", catalog, schema)

    full_path = f"{catalog}.{schema}.{table}"
    logger.info("[ICEBERG] Ensuring table %s exists...", full_path)
    spark.sql(
        _CREATE_TABLE_SQL.format(catalog=catalog, schema=schema, table=table)
    )
    logger.info("[ICEBERG] Table %s is ready.", full_path)


def load_to_bronze(
    df: DataFrame, catalog: str, schema: str, table: str, epoch_id: int
) -> None:
    """
    Append a micro-batch DataFrame to the Bronze Iceberg table.

    Called by Spark's foreachBatch for every streaming micro-batch.

    Args:
        df: Transformed batch DataFrame.
        catalog: Target Iceberg catalog name.
        schema: Target Iceberg schema/namespace.
        table: Target Iceberg table name.
        epoch_id: Micro-batch epoch identifier (used for logging).
    """
    if df.isEmpty():
        logger.warning("[BATCH-%d] Empty batch — skipping write.", epoch_id)
        return

    table_path = f"{catalog}.{schema}.{table}"
    record_count = df.count()
    logger.info("[BATCH-%d] Writing %d records to %s ...", epoch_id, record_count, table_path)

    try:
        df.writeTo(table_path).append()
        logger.info("[BATCH-%d] ✓ Written %d records to %s.", epoch_id, record_count, table_path)
    except Exception as primary_err:
        logger.warning(
            "[BATCH-%d] writeTo failed (%s) — retrying with INSERT INTO...",
            epoch_id,
            primary_err,
        )
        tmp_view = f"streaming_test_batch_{epoch_id}"
        df.createOrReplaceTempView(tmp_view)
        spark = SparkSession.getActiveSession()
        if spark is None:
            raise RuntimeError(
                "No active SparkSession — cannot fall back to INSERT INTO."
            )
        spark.sql(f"INSERT INTO {table_path} SELECT * FROM {tmp_view}")
        logger.info("[BATCH-%d] ✓ Written %d records via INSERT INTO fallback.", epoch_id, record_count)
