"""Extract data from Bronze layer Iceberg table."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def extract_from_bronze(
    spark: SparkSession,
    bronze_table: str,
    start_date: str,
    end_date: str,
) -> DataFrame:
    """
    Extract crypto trades from Bronze table for specified date range.

    Args:
        spark: SparkSession instance
        bronze_table: Full table path (e.g., bronze.bronze.crypto_trades_raw)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame with Bronze trade data
    """
    logger.info(f"Extracting from Bronze table: {bronze_table}")
    logger.info(f"Date range: {start_date} to {end_date}")

    # Read from Bronze Iceberg table
    bronze_df = spark.table(bronze_table)

    # Filter by date range
    filtered_df = bronze_df.filter(
        (bronze_df.trade_date >= start_date)
        & (bronze_df.trade_date <= end_date)
    )

    # Select relevant columns for aggregation
    selected_df = filtered_df.select(
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
