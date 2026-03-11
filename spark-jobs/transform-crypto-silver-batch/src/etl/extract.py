"""Extract data from Bronze layer Iceberg table."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def extract_from_bronze(
    spark: SparkSession,
    bronze_table: str,
) -> DataFrame:
    """
    Extract all crypto trades from Bronze table.

    Args:
        spark: SparkSession instance
        bronze_table: Full table path (e.g., bronze.bronze.crypto_trades_raw)

    Returns:
        DataFrame with all Bronze trade data
    """
    logger.info(f"Extracting from Bronze table: {bronze_table}")
    logger.info("No date filter applied — processing all available data.")

    # Read from Bronze Iceberg table (full scan, no date filter)
    bronze_df = spark.table(bronze_table)

    # Select relevant columns for aggregation
    selected_df = bronze_df.select(
        "symbol",
        "exchange",
        "base_currency",
        "quote_currency",
        "price",
        "volume",
        "trade_datetime",
        "trade_date",
    )

    record_count = selected_df.count()
    logger.info(f"✅ Extracted {record_count} records from Bronze")

    return selected_df
