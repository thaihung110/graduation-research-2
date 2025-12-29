"""Transform decoded Kafka messages to Bronze table schema."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    from_unixtime,
    to_date,
    to_timestamp,
)

logger = logging.getLogger(__name__)


def transform_trades(parsed_df: DataFrame) -> DataFrame:
    """
    Transform parsed messages to trade records for Bronze table.

    Args:
        parsed_df: DataFrame with parsed Kafka messages (from decoder)

    Returns:
        Transformed DataFrame ready for Bronze table
    """
    logger.debug("Transforming trades from parsed messages")

    # Explode trades array
    exploded_df = parsed_df.select(
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        explode(col("message.trades")).alias("trade"),
        col("message.message_type").alias("message_type"),
        col("message.producer_metadata").alias("producer_metadata"),
    )

    # Transform trades to Bronze schema
    trades_df = exploded_df.select(
        col("trade.symbol"),
        col("trade.exchange"),
        col("trade.base_currency"),
        col("trade.quote_currency"),
        col("trade.price"),
        col("trade.volume"),
        to_timestamp(from_unixtime(col("trade.timestamp_ms") / 1000)).alias(
            "trade_datetime"
        ),
        to_date(from_unixtime(col("trade.timestamp_ms") / 1000)).alias(
            "trade_date"
        ),
        col("trade.timestamp_ms"),
        col("trade.conditions"),
        col("trade.ingestion_timestamp_ms"),
        col("message_type"),
        col("producer_metadata.producer_id").alias("producer_id"),
        col("producer_metadata.kafka_topic").alias("kafka_topic"),
        col("producer_metadata.schema_version").alias("schema_version"),
        col("kafka_partition"),
        col("kafka_offset"),
        current_timestamp().alias("bronze_ingestion_timestamp"),
    )

    # Final DataFrame with columns in order
    final_df = trades_df.select(
        col("symbol"),
        col("exchange"),
        col("base_currency"),
        col("quote_currency"),
        col("price"),
        col("volume"),
        col("trade_datetime"),
        col("trade_date"),
        col("timestamp_ms"),
        col("conditions"),
        col("ingestion_timestamp_ms"),
        col("message_type"),
        col("producer_id"),
        col("kafka_topic"),
        col("schema_version"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("bronze_ingestion_timestamp"),
    )

    return final_df
