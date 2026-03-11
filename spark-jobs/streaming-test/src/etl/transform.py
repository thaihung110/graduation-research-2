"""Transform raw Kafka bytes into a flat DataFrame ready for the Bronze table."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, length, to_date

logger = logging.getLogger(__name__)


def transform_raw_message(stream_df: DataFrame) -> DataFrame:
    """
    Minimal transformation for the streaming-test job.

    Strategy
    --------
    This is a *test* job — the goal is to validate the streaming pipeline
    end-to-end, not to parse a business schema.  Therefore we keep the raw
    Kafka message as a plain string column alongside the standard Kafka
    metadata columns.  Any downstream job can parse the JSON/Avro as needed.

    Output columns
    --------------
    kafka_topic          : str  — Kafka topic name
    kafka_partition      : int  — Kafka partition number
    kafka_offset         : long — Kafka offset
    kafka_timestamp      : timestamp — Kafka message timestamp (event time)
    kafka_key            : str  — Kafka message key (may be null)
    raw_value            : str  — Kafka message value as UTF-8 string
    raw_value_bytes_len  : int  — byte length of the raw value
    ingestion_date       : date — date portion of bronze_ingestion_timestamp (partition key)
    bronze_ingestion_ts  : timestamp — wall-clock time when this record landed in Bronze
    """
    logger.debug("Applying minimal transform to raw Kafka DataFrame")

    transformed = stream_df.select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_value"),
        length(col("value")).alias("raw_value_bytes_len"),
        current_timestamp().alias("bronze_ingestion_ts"),
        to_date(current_timestamp()).alias("ingestion_date"),
    )

    return transformed
