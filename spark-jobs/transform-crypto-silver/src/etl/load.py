"""Load transformed data into Iceberg Silver table."""

import logging

from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def ensure_silver_table_exists(spark: SparkSession, silver_table: str) -> None:
    """
    Ensure Silver table exists, create if it doesn't.

    Args:
        spark: SparkSession instance
        silver_table: Full table name (e.g., "silver.silver.crypto_trades")
    """
    from utils.schemas import get_create_table_sql

    def _create_namespace():
        silver_parts = silver_table.split(".")
        if len(silver_parts) >= 2:
            catalog = silver_parts[0]
            namespace = silver_parts[1]
            logger.info(f"Ensuring namespace exists: {catalog}.{namespace}")
            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")

    def _check_table():
        spark.sql(f"DESCRIBE TABLE {silver_table}").show()
        logger.info(f"✅ Table {silver_table} already exists")

    def _create_table():
        logger.info(f"Creating table {silver_table}...")
        spark.sql(get_create_table_sql(silver_table))
        logger.info(f"✅ Table {silver_table} created")

    # Create namespace
    _create_namespace()

    # Check table exists, create if not
    try:
        _check_table()
    except Exception:
        _create_table()


def load_to_silver(df: DataFrame, silver_table: str, epoch_id: int) -> None:
    """
    Load transformed DataFrame to Silver Iceberg table.

    Args:
        df: Transformed DataFrame ready for Silver
        silver_table: Full table name
        epoch_id: Batch epoch ID for logging
    """

    def _write_with_writeto():
        logger.info(f"Writing batch {epoch_id} to {silver_table}...")
        df.writeTo(silver_table).append()
        logger.info(f"✅ Batch {epoch_id} written successfully")

    def _write_with_insert():
        # Fallback to INSERT INTO if writeTo fails
        df.createOrReplaceTempView(f"batch_{epoch_id}_temp")
        spark = SparkSession.getActiveSession()
        if spark:
            spark.sql(
                f"INSERT INTO {silver_table} SELECT * FROM batch_{epoch_id}_temp"
            )
            logger.info(f"✅ Batch {epoch_id} written using INSERT INTO")
        else:
            raise RuntimeError("No active SparkSession found")

    # Try writeTo
    try:
        _write_with_writeto()
    except Exception as e:
        logger.warning(f"writeTo failed, trying INSERT INTO: {e}")
        # Fallback to INSERT INTO
        _write_with_insert()
