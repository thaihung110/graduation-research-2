"""Extract data from Bronze Iceberg table."""

import logging
import os
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_unixtime, lit, unix_timestamp

logger = logging.getLogger(__name__)


def extract_from_bronze(
    spark: SparkSession,
    bronze_table: str,
    filter_timestamp: Optional[str] = None,
) -> Optional[DataFrame]:
    """
    Extract data from Bronze table with optional timestamp filter.

    Args:
        spark: SparkSession instance
        bronze_table: Full table name (e.g., "bronze.bronze.crypto_trades_raw")
        filter_timestamp: Optional timestamp to filter records after this time

    Returns:
        DataFrame from Bronze table, or None if error
    """

    try:
        bronze_df = spark.table(bronze_table)

        if filter_timestamp:
            buffer_seconds = int(os.getenv("BRONZE_BUFFER_SECONDS", "10"))
            buffer_timestamp_expr = from_unixtime(
                unix_timestamp(lit(filter_timestamp)) - buffer_seconds
            )
            bronze_df = bronze_df.filter(
                col("bronze_ingestion_timestamp") > buffer_timestamp_expr
            )
            logger.info(
                f"Filtering records after: {filter_timestamp} "
                f"(with {buffer_seconds}s buffer)"
            )

        return bronze_df
    except Exception as e:
        logger.error(f"Failed to extract from {bronze_table}: {e}")
        return None


def get_last_processed_timestamp(
    spark: SparkSession, silver_table: str
) -> Optional[str]:
    """
    Get max bronze_ingestion_timestamp from Silver table.

    Args:
        spark: SparkSession instance
        silver_table: Full table name (e.g., "silver.silver.crypto_trades")

    Returns:
        Max timestamp string or None
    """

    try:
        result = spark.sql(
            f"SELECT MAX(bronze_ingestion_timestamp) as max_ts FROM {silver_table}"
        ).collect()
        if result and result[0]["max_ts"]:
            return result[0]["max_ts"]
        return None
    except Exception as e:
        logger.warning(f"Could not get last processed timestamp: {e}")
        return None
